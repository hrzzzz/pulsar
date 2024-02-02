/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.client.impl;

import java.util.BitSet;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageIdAdv;

public class MessageIdAdvUtils {

    static int hashCode(MessageIdAdv msgId) {
        return (int) (31 * (msgId.getLedgerId() + 31 * msgId.getEntryId())
                + (31 * (long) msgId.getPartitionIndex()) + msgId.getBatchIndex());
    }

    static boolean equals(MessageIdAdv lhs, Object o) {
        if (!(o instanceof MessageIdAdv)) {
            return false;
        }
        final MessageIdAdv rhs = (MessageIdAdv) o;
        return lhs.getLedgerId() == rhs.getLedgerId()
                && lhs.getEntryId() == rhs.getEntryId()
                && lhs.getPartitionIndex() == rhs.getPartitionIndex()
                && lhs.getBatchIndex() == rhs.getBatchIndex();
    }

    static boolean acknowledge(MessageIdAdv msgId, boolean individual) {
        // 不是batch消息直接返回
        if (!isBatch(msgId)) {
            return true;
        }
        // 获取ackSet
        final BitSet ackSet = msgId.getAckSet();
        if (ackSet == null) {
            // The internal MessageId implementation should never reach here. If users have implemented their own
            // MessageId and getAckSet() is not override, return false to avoid acknowledge current entry.
            return false;
        }
        // 获取该消息是batch消息中的第几条消息
        int batchIndex = msgId.getBatchIndex();
        if (individual) {
            // 单独确认时，ackSet中对应位置设置为0：11011
            ackSet.clear(batchIndex);
        } else {
            // 累计确认时，把ackSet中对应位置及之前的都设置为0：000111
            ackSet.clear(0, batchIndex + 1);
        }
        return ackSet.isEmpty();
    }

    static boolean isBatch(MessageIdAdv msgId) {
        return msgId.getBatchIndex() >= 0 && msgId.getBatchSize() > 0;
    }

    static MessageIdAdv discardBatch(MessageId messageId) {
        if (messageId instanceof ChunkMessageIdImpl) {
            return (MessageIdAdv) messageId;
        }
        MessageIdAdv msgId = (MessageIdAdv) messageId;
        return new MessageIdImpl(msgId.getLedgerId(), msgId.getEntryId(), msgId.getPartitionIndex());
    }

    static MessageIdAdv prevMessageId(MessageIdAdv msgId) {
        return new MessageIdImpl(msgId.getLedgerId(), msgId.getEntryId() - 1, msgId.getPartitionIndex());
    }
}
