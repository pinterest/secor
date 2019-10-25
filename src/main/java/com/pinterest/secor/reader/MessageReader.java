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
package com.pinterest.secor.reader;

import com.pinterest.secor.common.OffsetTracker;
import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.common.TopicPartition;
import com.pinterest.secor.message.Message;
import com.pinterest.secor.util.IdUtil;
import com.pinterest.secor.util.RateLimitUtil;
import com.pinterest.secor.util.StatsUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Message reader consumer raw Kafka messages.
 *
 * @author Pawel Garbacki (pawel@pinterest.com)
 */
public class MessageReader {
    private static final Logger LOG = LoggerFactory.getLogger(MessageReader.class);

    protected SecorConfig mConfig;
    protected KafkaMessageIterator mKafkaMessageIterator;
    protected OffsetTracker mOffsetTracker;
    protected HashMap<TopicPartition, Long> mLastAccessTime;
    protected final int mTopicPartitionForgetSeconds;
    protected final int mCheckMessagesPerSecond;
    protected int mNMessages;

    public MessageReader(SecorConfig config, OffsetTracker offsetTracker, KafkaMessageIterator kafkaMessageIterator) throws
            UnknownHostException {
        mConfig = config;
        mOffsetTracker = offsetTracker;
        mLastAccessTime = new HashMap<TopicPartition, Long>();
        StatsUtil.setLabel("secor.kafka.consumer.id", IdUtil.getConsumerId());
        mTopicPartitionForgetSeconds = mConfig.getTopicPartitionForgetSeconds();
        mCheckMessagesPerSecond = mConfig.getMessagesPerSecond() / mConfig.getConsumerThreads();
        mKafkaMessageIterator = kafkaMessageIterator;
    }

    private void updateAccessTime(TopicPartition topicPartition) {
        long now = System.currentTimeMillis() / 1000L;
        mLastAccessTime.put(topicPartition, now);
        Iterator iterator = mLastAccessTime.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry pair = (Map.Entry) iterator.next();
            long lastAccessTime = (Long) pair.getValue();
            if (now - lastAccessTime > mTopicPartitionForgetSeconds) {
                iterator.remove();
            }
        }
    }

    private void exportStats() {
        StringBuffer topicPartitions = new StringBuffer();
        for (TopicPartition topicPartition : mLastAccessTime.keySet()) {
            if (topicPartitions.length() > 0) {
                topicPartitions.append(' ');
            }
            topicPartitions.append(topicPartition.getTopic() + '/' +
                                   topicPartition.getPartition());
        }
        StatsUtil.setLabel("secor.topic_partitions", topicPartitions.toString());
    }

    public boolean hasNext() {
        return mKafkaMessageIterator.hasNext();
    }

    public Message read() {
        assert hasNext();
        mNMessages = (mNMessages + 1) % mCheckMessagesPerSecond;
        if (mNMessages % mCheckMessagesPerSecond == 0) {
            RateLimitUtil.acquire(mCheckMessagesPerSecond);
        }
        Message message = mKafkaMessageIterator.next();
        if (message == null) {
            return null;
        }
        TopicPartition topicPartition = new TopicPartition(message.getTopic(),
                                                           message.getKafkaPartition());
        updateAccessTime(topicPartition);
        // Skip already committed messages.
        long committedOffsetCount = mOffsetTracker.getTrueCommittedOffsetCount(topicPartition);
        LOG.debug("read message {}", message);
        if (mNMessages % mCheckMessagesPerSecond == 0) {
            exportStats();
        }
        if (message.getOffset() < committedOffsetCount) {
            LOG.debug("skipping message {} because its offset precedes committed offset count {}",
                    message, committedOffsetCount);
            return null;
        }
        return message;
    }

    public void commit(TopicPartition topicPartition, long offset) {
        mKafkaMessageIterator.commit(topicPartition, offset);
    }
}
