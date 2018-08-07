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
package org.apache.rocketmq.store.transaction.jdbc;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.util.JdbcUtils;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.store.ConsumeQueue;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.DispatchRequest;
import org.apache.rocketmq.store.config.BrokerRole;
import org.apache.rocketmq.store.transaction.TransactionCheckExecutor;
import org.apache.rocketmq.store.transaction.TransactionStateService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class JDBCTransactionStateService implements TransactionStateService {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.TRANSACTION_LOGGER_NAME);

    private final ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(,
        new BasicThreadFactory.Builder().namingPattern("checkDBTransactionMessage-schedule-pool-%d").daemon(true).build());

    private final DefaultMessageStore defaultMessageStore;


    private DruidDataSource druidDataSource;

    private final AtomicLong totalRecordsValue = new AtomicLong(0);

    public JDBCTransactionStateService(DefaultMessageStore defaultMessageStore) {
        this.defaultMessageStore = defaultMessageStore;
    }

    @Override public boolean load() {
        try {
            druidDataSource = new DruidDataSource();
            druidDataSource.setDriverClassName(defaultMessageStore.getMessageStoreConfig().getJdbcDriverClass());
            druidDataSource.setUrl(defaultMessageStore.getMessageStoreConfig().getJdbcURL());
            druidDataSource.setUsername(defaultMessageStore.getMessageStoreConfig().getJdbcUser());
            druidDataSource.setPassword(defaultMessageStore.getMessageStoreConfig().getJdbcPassword());
            druidDataSource.setInitialSize(5);
            druidDataSource.setMaxActive(10);
            druidDataSource.setMinIdle(5);
            druidDataSource.setMaxWait(2000);
            druidDataSource.setMinEvictableIdleTimeMillis(300000);
            druidDataSource.setUseUnfairLock(true);
            druidDataSource.setConnectionErrorRetryAttempts(3);
            druidDataSource.setValidationQuery("SELECT 'x'");
            druidDataSource.setTestOnReturn(false);
            druidDataSource.setTestOnBorrow(false);

            log.info("data config: {}", druidDataSource);
            druidDataSource.init();
            return true;
        } catch (Exception e) {
            log.error("druidDataSource load Exeption", e);
            return false;
        }
    }

    @Override public void start() {
        executorService.scheduleAtFixedRate(new Runnable() {
            @Override public void run() {
                doCheck();
            }
        }, 1000 * 5, this.defaultMessageStore.getMessageStoreConfig().getCheckTransactionMessageTimerInterval(), TimeUnit.MILLISECONDS);
    }

    @Override public void shutdown() {
        druidDataSource.close();
    }

    @Override public int deleteExpiredStateFile(long offset) {
        // 不操作磁盘，无需删除transaction state table & redolog
        return 0;
    }

    @Override public void recoverStateTable(boolean lastExitOK) {
        // 持久化在db，不需要redolog
    }

    @Override public boolean appendPreparedTransaction(DispatchRequest request) {
        final int tranState = MessageSysFlag.getTransactionValue(request.getSysFlag());
        if (MessageSysFlag.TRANSACTION_PREPARED_TYPE != tranState) {
            return true;
        }

        TransactionRecord transactionRecord = new TransactionRecord(request.getCommitLogOffset(), request.getStoreTimestamp(),
                request.getMsgSize(), request.getPropertiesMap().get(MessageConst.PROPERTY_PRODUCER_GROUP));

        List<TransactionRecord> trs = new ArrayList<>();
        trs.add(transactionRecord);

        if (trs == null || trs.size() == 0) {
            return true;
        }

        Connection connection = null;
        PreparedStatement statement = null;
        try {
            connection = druidDataSource.getConnection();
            connection.setAutoCommit(false);
            statement = connection.prepareStatement(String.format("insert into rocketmq_transaction(offset,producerGroup,timestamp,size) values (?, ?, ?, ?)"));
            for (TransactionRecord tr : trs) {
                statement.setLong(1, tr.getOffset());
                statement.setString(2, tr.getProducerGroup());
                statement.setLong(3, tr.getTimestamp());
                statement.setInt(4, tr.getSize());
                statement.addBatch();
            }
            int[] executeBatch = statement.executeBatch();
            connection.commit();
            this.totalRecordsValue.addAndGet(updatedRows(executeBatch));
            return true;
        } catch (Exception e) {
            log.warn("createDB Exception", e);
            return false;
        } finally {
            JdbcUtils.close(statement);
            JdbcUtils.close(connection);
        }
    }

    private long updatedRows(int[] rows) {
        long res = 0;
        for (int i : rows) {
            res += i;
        }
        return res;
    }

    @Override public boolean updateTransactionState(DispatchRequest request) {
        final int tranState = MessageSysFlag.getTransactionValue(request.getSysFlag());
        try {
            switch (tranState) {
                case MessageSysFlag.TRANSACTION_COMMIT_TYPE:
                case MessageSysFlag.TRANSACTION_ROLLBACK_TYPE:
                    // 二阶段消息的preparedTransactionOffset指向一阶段消息的commitLogOffset
                    long offset = request.getPreparedTransactionOffset();
                    List<Long> rts = new ArrayList<>();
                    rts.add(offset);
                    return update(rts);
                default:
                    return true;
            }
        } catch (Exception e) {
            log.error("updateTransaction to db error: ", e);
        }

        return true;
    }

    private boolean update(List<Long> pks) {
        if (pks == null || pks.size() == 0) {
            return true;
        }
        PreparedStatement statement = null;
        Connection connection = null;
        try {
            connection = druidDataSource.getConnection();
            connection.setAutoCommit(false);
            statement = connection.prepareStatement(String.format("DELETE FROM rocketmq_transaction WHERE offset = ?"));
            for (long pk : pks) {
                statement.setLong(1, pk);
                statement.addBatch();
            }
            int[] executeBatch = statement.executeBatch();
            connection.commit();
            this.totalRecordsValue.addAndGet(-updatedRows(executeBatch));
            return true;
        } catch (Exception e) {
            log.warn("createDB Exception", e);
        } finally {
            JdbcUtils.close(statement);
            JdbcUtils.close(connection);
        }
        return false;
    }

    @Override public ConsumeQueue getTranRedoLog() {
        return null;
    }

    @Override public AtomicLong getTranStateTableOffset() {
        return null;
    }

    @Override public long getMaxTransOffset() {
        Connection connection = null;
        PreparedStatement ps = null;
        try {
            connection = druidDataSource.getConnection();
            ps = connection.prepareStatement(String.format("select max(offset) as maxOffset from rocketmq_transaction"));
            ResultSet resultSet = ps.executeQuery();
            if (resultSet.next()) {
                Long maxOffset = resultSet.getLong("maxOffset");
                return maxOffset != null ? maxOffset : -1L;
            }
        } catch (SQLException e) {
            log.warn("maxPK Exception", e);
        } finally {
            JdbcUtils.close(ps);
            JdbcUtils.close(connection);
        }
        return -1L;
    }

    @Override public long getMinTransOffset() {
        Connection connection = null;
        PreparedStatement ps = null;
        try {
            connection = druidDataSource.getConnection();
            ps = connection.prepareStatement(String.format("select min(offset) as minOffset from rocketmq_transaction"));
            ResultSet resultSet = ps.executeQuery();
            if (resultSet.next()) {
                Long minOffset = resultSet.getLong("minOffset");
                return minOffset != null ? minOffset : -1L;
            }
        } catch (SQLException e) {
            log.warn("maxPK Exception", e);
        } finally {
            JdbcUtils.close(ps);
            JdbcUtils.close(connection);
        }
        return -1L;
    }

    private void doCheck() {
        final TransactionCheckExecutor transactionCheckExecutor = this.defaultMessageStore.getTransactionCheckExecutor();
        boolean bSlave = this.defaultMessageStore.getMessageStoreConfig().getBrokerRole() == BrokerRole.SLAVE;

        if (bSlave) {
            return;
        }

        if (!this.defaultMessageStore.getMessageStoreConfig().isCheckTransactionMessageEnable()) {
            return;
        }

        long totalRecords = this.totalRecordsValue.get();
        long pk = -1;
        List<TransactionRecord> records;
        while (totalRecords > 0 && (records = traverse(pk, 100)).size() > 0) {
            for (TransactionRecord record : records) {
                log.info("record: offset={}, timestamp={}, producerGroup={}, size={}", record.getOffset(), record.getTimestamp(), record.getProducerGroup(), record.getSize());
                try {
                    long timestamp = record.getTimestamp();
                    if (System.currentTimeMillis() <= timestamp) {
                        log.info("record: offset={}, timestamp={}, producerGroup={}, size={}", record.getOffset(), record.getTimestamp(), record.getProducerGroup(), record.getSize());
                        continue;
                    }
                    transactionCheckExecutor.gotoCheck(record.getProducerGroup().hashCode(), 0L, record.getOffset(), record.getSize());
                    pk = record.getOffset();
                } catch (Exception e) {
                    log.warn("goto check exception", e);
                }
            }
        }

    }

    private List<TransactionRecord> traverse(long pk, int nums) {
        List<TransactionRecord> list = new ArrayList<>(nums);
        Connection connection = null;
        PreparedStatement ps = null;
        try {
            connection = druidDataSource.getConnection();
            ps = connection.prepareStatement(String.format("select offset,producerGroup,timestamp,size from rocketmq_transaction where offset>? order by offset limit ?"));
            ps.setLong(1, pk);
            ps.setInt(2, nums);
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) {
                TransactionRecord tr = new TransactionRecord(
                    resultSet.getLong("offset"),
                    resultSet.getLong("timestamp"),
                    resultSet.getInt("size"),
                    resultSet.getString("producerGroup")
                );
                list.add(tr);
            }
            return list;
        } catch (SQLException e) {
            log.warn("traverse Exception", e);
        } finally {
            JdbcUtils.close(ps);
            JdbcUtils.close(connection);
        }
        return list;
    }

}
