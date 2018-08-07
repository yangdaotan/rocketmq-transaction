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

package org.apache.rocketmq.broker.transaction;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.common.protocol.header.CheckTransactionStateRequestHeader;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.transaction.TransactionCheckExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**

 */
public class DefaultTransactionCheckExecuter implements TransactionCheckExecutor {
    private static final Logger log = LoggerFactory.getLogger(DefaultTransactionCheckExecuter.class);
    private final BrokerController brokerController;

    public DefaultTransactionCheckExecuter(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    @Override
    public void gotoCheck(int producerGroupHashCode, long tranStateTableOffset, long commitLogOffset,
        int msgSize) {
        // 第一步、查询Producer
        final ClientChannelInfo clientChannelInfo = this.brokerController.getProducerManager().pickProducerChannelRandomly(producerGroupHashCode);
        if (null == clientChannelInfo) {
            log.warn("check a producer transaction state, but not find any channel of this group[{}]", producerGroupHashCode);
            return;
        }

        // 第二步、查询消息
        SelectMappedBufferResult selectMapedBufferResult = this.brokerController.getMessageStore().selectOneMessageByOffset(commitLogOffset, msgSize);
        if (null == selectMapedBufferResult) {
            log.warn("check a producer transaction state, but not find message by commitLogOffset: {}, msgSize: ", commitLogOffset, msgSize);
            return;
        }

        // 第三步、向Producer发起请求
        final CheckTransactionStateRequestHeader requestHeader = new CheckTransactionStateRequestHeader();
        requestHeader.setCommitLogOffset(commitLogOffset);
        requestHeader.setTranStateTableOffset(tranStateTableOffset);
        this.brokerController.getBroker2Client().checkProducerTransactionState(clientChannelInfo.getChannel(), requestHeader, selectMapedBufferResult);
    }
}
