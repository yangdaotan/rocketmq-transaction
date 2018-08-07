/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.store.transaction.file;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.store.ConsumeQueue;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.MappedFile;
import org.apache.rocketmq.store.MappedFileQueue;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.config.StorePathConfigHelper;
import org.apache.rocketmq.store.transaction.TransactionCheckExecutor;
import org.apache.rocketmq.store.transaction.TransactionStateService;
import org.slf4j.LoggerFactory;

/**
 * 通过文件存储事务 构造一个redolog来进行恢复事务状态 构造一个transaction state table来存储事务明细，存储格式为 |---- commitlog offset--|---
 * size----|---timestamp---|---group hashcode---|---state（p/c/r）--| | ------   8 Byte ----- |--- 4 Byte--|--- 4 Byte
 * ----|--- 4 Byte----------|----- 4 Byte ------|
 *
 */
public class FileTransactionStateService implements TransactionStateService {
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(LoggerName.TRANSACTION_LOGGER_NAME);

    public static final int TSS_STORE_UNIT_SIZE = 24; // 8 + 4 + 4 + 4+ 4
    public static final String TRANSACTION_REDOLOG_TOPIC = "TRANSACTION_REDOLOG_TOPIC_XXXX";
    public static final int TRANSACTION_REDOLOG_TOPIC_QUEUEID = 0;

    private final DefaultMessageStore defaultMessageStore;
    private MappedFileQueue tranStateTable;
    private final ByteBuffer byteBufferAppend = ByteBuffer.allocate(TSS_STORE_UNIT_SIZE);
    // redolog的实现利用消费队列，方便恢复
    private final ConsumeQueue tranRedoLog;
    public final static long PREPARED_MESSAGE_TAGS_CODE = -1;
    // state在每天记录的第20个字节
    private final static int TS_STATE_POS = 20;
    // 每个transaction state table文件的开头offset从0开始
    private final AtomicLong tranStateTableOffset = new AtomicLong(0);
    // 回查线程
    private final ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(4,
        new BasicThreadFactory.Builder().namingPattern("checkFileTransactionMessage-schedule-pool-%d").daemon(true).build());
    private final ConcurrentHashMap<String, Future> futures = new ConcurrentHashMap<>(4);

    public FileTransactionStateService(DefaultMessageStore defaultMessageStore) {
        this.defaultMessageStore = defaultMessageStore;

        this.tranStateTable = new MappedFileQueue(StorePathConfigHelper.getTranStateTableStorePath(defaultMessageStore
            .getMessageStoreConfig().getStorePathRootDir()), defaultMessageStore
            .getMessageStoreConfig().getTranStateTableMappedFileSize(), null);

        this.tranRedoLog = new ConsumeQueue(
            TRANSACTION_REDOLOG_TOPIC,
            TRANSACTION_REDOLOG_TOPIC_QUEUEID,
            StorePathConfigHelper.getTranRedoLogStorePath(defaultMessageStore.getMessageStoreConfig()
                .getStorePathRootDir()),
            defaultMessageStore.getMessageStoreConfig().getTranRedoLogMappedFileSize(),
            defaultMessageStore
        );
    }

    @Override public boolean load() {
        boolean result = this.tranRedoLog.load();
        result = result && this.tranStateTable.load();
        return result;
    }

    @Override public void start() {
        final List<MappedFile> mappedFiles = this.tranStateTable.getMappedFiles();
        for (final MappedFile mappedFile : mappedFiles) {
            final Future future = executorService.scheduleAtFixedRate(new Runnable() {
                @Override public void run() {
                    doCheck(mappedFile);
                }
            }, 1000 * 5, this.defaultMessageStore.getMessageStoreConfig().getCheckTransactionMessageTimerInterval(), TimeUnit.MILLISECONDS);

            futures.put(mappedFile.getFileName(), future);
        }
    }

    @Override public void shutdown() {
        executorService.shutdown();
    }

    @Override public int deleteExpiredStateFile(long offset) {
        int count = this.tranStateTable.deleteExpiredFileByOffset(offset, TSS_STORE_UNIT_SIZE);
        return count;
    }

    // 恢复，正常退出则从transcation state table恢复
    //      异常退出则删除tst，从redolog恢复
    @Override public void recoverStateTable(boolean lastExitOK) {
        if (lastExitOK) {
            this.recoverStateTableNormal();
        } else {
            this.tranStateTable.destroy();
            this.recreateStateTable();
        }
    }

    private void recoverStateTableNormal() {
        final List<MappedFile> mappedFiles = this.tranStateTable.getMappedFiles();
        if (!mappedFiles.isEmpty()) {
            int index = mappedFiles.size() - 3;
            if (index < 0) {
                index = 0;
            }

            int mappedFileSizeLogics = this.tranStateTable.getMappedFileSize();
            MappedFile mappedFile = mappedFiles.get(index);
            ByteBuffer byteBuffer = mappedFile.sliceByteBuffer();
            long processOffset = mappedFile.getFileFromOffset();
            long mappedFileOffset = 0;
            while (true) {
                for (int i = 0; i < mappedFileSizeLogics; i += TSS_STORE_UNIT_SIZE) {
                    long offset = byteBuffer.getLong();
                    int size = byteBuffer.getInt();
                    int timestamp = byteBuffer.getInt();
                    int groupHashCode = byteBuffer.getInt();
                    int tranState = byteBuffer.getInt();

                    boolean stateOk = false;
                    switch (tranState) {
                        case MessageSysFlag.TRANSACTION_PREPARED_TYPE:
                        case MessageSysFlag.TRANSACTION_COMMIT_TYPE:
                        case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE:
                            stateOk = true;
                            break;
                        default:
                            break;
                    }
                    // 说明当前存储单元有效，继续向下遍历
                    if (offset > 0 && size > 0 && stateOk) {
                        mappedFileOffset = i + TSS_STORE_UNIT_SIZE;
                    } else {
                        log.info("recover current transaction state table file over, " + mappedFile.getFileName() +
                            ", " + offset + ", " + size + ", " + timestamp + ", " + groupHashCode + ", " + tranState);
                        break;
                    }
                }

                if (mappedFileOffset == mappedFileSizeLogics) {
                    index++;
                    if (index >= mappedFiles.size()) {
                        log.info("recover last transcation state table file over, last mapped file, " + mappedFile.getFileName());
                        break;
                    } else {
                        mappedFile = mappedFiles.get(index);
                        byteBuffer = mappedFile.sliceByteBuffer();
                        processOffset = mappedFile.getFileFromOffset();
                        mappedFileOffset = 0;
                        log.info("recover next transaction state table file, " + mappedFile.getFileName());
                    }
                } else {
                    log.info("recover current transaction state table over " + mappedFile.getFileName() + " "
                        + (processOffset + mappedFileOffset));
                    break;
                }
            }

            processOffset += mappedFileOffset;
            this.tranStateTableOffset.set(tranStateTable.getMaxOffset() / TSS_STORE_UNIT_SIZE);
            log.info("recover normal over, transaction state table max offset: {}", this.tranStateTableOffset.get());
            this.tranStateTable.truncateDirtyFiles(processOffset);
        }
    }

    private void recreateStateTable() {
        this.tranStateTable = new MappedFileQueue(StorePathConfigHelper.getTranStateTableStorePath(
            defaultMessageStore.getMessageStoreConfig().getStorePathRootDir()),
            defaultMessageStore.getMessageStoreConfig().getTranStateTableMappedFileSize(), null);

        final TreeSet<Long> preparedItemSet = new TreeSet<>();

        final long minOffset = this.tranRedoLog.getMinLogicOffset();
        long processOffset = minOffset;
        while (true) {
            // consumeQueue: commitLogOffset, msgSize, tagsCode
            SelectMappedBufferResult bufferConsumQueue = tranRedoLog.getIndexBuffer(processOffset);
            if (bufferConsumQueue != null) {
                try {
                    long i = 0;
                    for (; i < bufferConsumQueue.getSize(); i += ConsumeQueue.CQ_STORE_UNIT_SIZE) {
                        long offset = bufferConsumQueue.getByteBuffer().getLong();
                        int size = bufferConsumQueue.getByteBuffer().getInt();
                        long groupTagsCode = bufferConsumQueue.getByteBuffer().getLong();
                        // prepare
                        if (PREPARED_MESSAGE_TAGS_CODE == groupTagsCode) {
                            preparedItemSet.add(offset);
                        } else { // commit/rollback
                            preparedItemSet.remove(groupTagsCode);
                        }
                    }

                    processOffset += i;
                } finally {
                    bufferConsumQueue.release();
                }
            } else {
                break;
            }
        }
        log.info("scan transcation redolog over, end offset:{}, prepared transaction count: {}", processOffset, preparedItemSet.size());

        // step2: 重建transaction state table
        Iterator<Long> it = preparedItemSet.iterator();
        while (it.hasNext()) {
            Long offset = it.next();
            MessageExt msgExt = this.defaultMessageStore.lookMessageByOffset(offset);
            if (msgExt != null) {
                appendPreparedTransaction(msgExt.getCommitLogOffset(), msgExt.getStoreSize(),
                    (int) (msgExt.getStoreTimestamp() / 1000),
                    msgExt.getProperty(MessageConst.PROPERTY_PRODUCER_GROUP).hashCode());
                tranStateTableOffset.incrementAndGet();
            }
        }
    }

    @Override public boolean appendPreparedTransaction(DispatchRequest request) {
        final long offset = request.getCommitLogOffset();
        final int size = request.getMsgSize();
        final int timestamp = (int) (request.getStoreTimestamp() / 1000);
        final int groupHashCode = request.getPropertiesMap().get(MessageConst.PROPERTY_PRODUCER_GROUP).hashCode();

        return appendPreparedTransaction(offset, size, timestamp, groupHashCode);
    }

    private boolean appendPreparedTransaction(final long offset, final int size, final int timestamp,
        final int groupHashCode) {
        final MappedFile mappedFile = tranStateTable.getLastMappedFile(0);
        if (null == mappedFile) {
            log.error("appendPeparedTransaction: create mappedfile error.");
            return false;
        }

        // 刚创建的mappedFile需要加入定时任务
        if (0 == mappedFile.getWrotePosition()) {
            final Future future = executorService.scheduleAtFixedRate(new Runnable() {
                @Override public void run() {
                    doCheck(mappedFile);
                }
            }, 1000 * 5, this.defaultMessageStore.getMessageStoreConfig().getCheckTransactionMessageTimerInterval(), TimeUnit.MILLISECONDS);
            futures.put(mappedFile.getFileName(), future);
        }

        // 重置下buffer，准备写入
        this.byteBufferAppend.position(0);
        this.byteBufferAppend.limit(TSS_STORE_UNIT_SIZE);

        this.byteBufferAppend.putLong(offset);
        this.byteBufferAppend.putInt(size);
        this.byteBufferAppend.putInt(timestamp);
        this.byteBufferAppend.putInt(groupHashCode);
        this.byteBufferAppend.putInt(MessageSysFlag.TRANSACTION_PREPARED_TYPE);

        return mappedFile.appendMessage(this.byteBufferAppend.array());
    }

    /*
        update state from prepare to commit/rollback
     */
    @Override public boolean updateTransactionState(DispatchRequest request) {
        final long tranStateTableOffset = request.getConsumeQueueOffset();
        // 该commitLogOffset为prepared commitLog的commitLogOffset
        final long commitLogOffset = request.getPreparedTransactionOffset();
        final int groupHashCode = request.getPropertiesMap().get(MessageConst.PROPERTY_PRODUCER_GROUP).hashCode();
        final int state = MessageSysFlag.getTransactionValue(request.getSysFlag());
        SelectMappedBufferResult selectMappedBufferResult = this.findTransactionBuffer(tranStateTableOffset);

        if (selectMappedBufferResult != null) {
            try {
                final long expectOffset = selectMappedBufferResult.getByteBuffer().getLong();
                selectMappedBufferResult.getByteBuffer().getInt();
                selectMappedBufferResult.getByteBuffer().getInt();
                final int expectGroupHashCode = selectMappedBufferResult.getByteBuffer().getInt();
                final int expectState = selectMappedBufferResult.getByteBuffer().getInt();

                if (expectOffset != commitLogOffset) {
                    log.error("updateTranscationState error: reqOffset={}, oriOffset={}", commitLogOffset, expectOffset);
                    return false;
                }

                if (groupHashCode != expectGroupHashCode) {
                    log.error("updateTranscationState error: reqGroupHashCode={}, oriGroupHashCode={}", groupHashCode, expectGroupHashCode);
                    return false;
                }

                if (MessageSysFlag.TRANSACTION_PREPARED_TYPE != expectState) {
                    log.warn("updateTransactionState error, the transaction is updated before.");
                    return true;
                }

                selectMappedBufferResult.getByteBuffer().putInt(TS_STATE_POS, state);
            } catch (Exception e) {
                log.error("updateTransactionState exception", e);
            } finally {
                selectMappedBufferResult.release();
            }
        }

        return false;
    }

    @Override public ConsumeQueue getTranRedoLog() {
        return tranRedoLog;
    }

    @Override public AtomicLong getTranStateTableOffset() {
        return tranStateTableOffset;
    }

    @Override public long getMaxTransOffset() {
        return 0;
    }

    @Override public long getMinTransOffset() {
        return 0;
    }

    private void doCheck(MappedFile mappedFile) {
        final TransactionCheckExecutor transactionCheckExecutor = defaultMessageStore.getTransactionCheckExecutor();
        final long checkTransactionMessageAtleastInterval = defaultMessageStore.getMessageStoreConfig().getCheckTransactionMessageAtleastInterval();
        final boolean slave = defaultMessageStore.getMessageStoreConfig().getBrokerRole() == BrokerRole.SLAVE;

        if (slave) {
            return;
        }

        if (!defaultMessageStore.getMessageStoreConfig().isCheckTransactionMessageEnable()) {
            return;
        }

        try {
            SelectMappedBufferResult selectMappedBufferResult = mappedFile.selectMappedBuffer(0);
            if (selectMappedBufferResult != null) {
                // prepared消息数
                long preparedMessageCountInThisMappedFile = 0;
                int i = 0;
                try {
                    for (; i < selectMappedBufferResult.getSize(); i += TSS_STORE_UNIT_SIZE) {
                        selectMappedBufferResult.getByteBuffer().position(i);

                        long offset = selectMappedBufferResult.getByteBuffer().getLong();
                        int msgSize = selectMappedBufferResult.getByteBuffer().getInt();
                        int timestamp = selectMappedBufferResult.getByteBuffer().getInt();
                        int groupHashCode = selectMappedBufferResult.getByteBuffer().getInt();
                        int state = selectMappedBufferResult.getByteBuffer().getInt();

                        if (state != MessageSysFlag.TRANSACTION_PREPARED_TYPE) {
                            continue;
                        }
                        // [0, checkTransactionMessageAtleastInterval] 为检测最小时间间隔，一个区间内只check一次
                        long timeDiff = System.currentTimeMillis() - 1000 * timestamp;
                        if (timeDiff < checkTransactionMessageAtleastInterval) {
                            break;
                        }
                        preparedMessageCountInThisMappedFile++;

                        try {
                            transactionCheckExecutor.gotoCheck(groupHashCode, getTranStateOffset(mappedFile, i), offset, msgSize);
                        } catch (Exception e) {
                            log.warn("gotoCheck exception, ", e);
                        }

                        if (0 == preparedMessageCountInThisMappedFile && i == mappedFile.getFileSize()) {
                            log.info("remove the transaction check task for no prepare massage int file {}", mappedFile.getFileName());
                            Future future = futures.remove(mappedFile.getFileName());
                            if (future != null) {
                                future.cancel(true);
                            }
                        }
                    }
                } finally {
                    selectMappedBufferResult.release();
                }
            } else if (mappedFile.isFull()) {
                log.info("the mappedfile[{}] maybe deleted, cancel check task", mappedFile.getFileName());
                Future future = futures.remove(mappedFile.getFileName());
                if (future != null) {
                    future.cancel(true);
                }
            }
        } catch (Exception e) {
            log.error("check transaction task exception ", e);
        }
    }

    private SelectMappedBufferResult findTransactionBuffer(final long tranStateTableOffset) {
        final int mappedFileSize = this.defaultMessageStore.getMessageStoreConfig().getTranStateTableMappedFileSize();
        final long offset = tranStateTableOffset * TSS_STORE_UNIT_SIZE;
        MappedFile mappedFile = this.tranStateTable.findMappedFileByOffset(offset);
        if (mappedFile != null) {
            return mappedFile.selectMappedBuffer((int) (offset % mappedFileSize));
        }
        return null;
    }

    private long getTranStateOffset(final MappedFile mappedFile, final long index) {
        return (mappedFile.getFileFromOffset() + index) / TSS_STORE_UNIT_SIZE;
    }
}
