/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.pinterest.secor.reader;

import com.pinterest.secor.common.OffsetTracker;
import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.common.TopicPartition;
import com.pinterest.secor.message.Message;
import com.pinterest.secor.timestamp.KafkaMessageTimestampFactory;
import com.pinterest.secor.util.IdUtil;
import com.pinterest.secor.util.RateLimitUtil;
import com.pinterest.secor.util.StatsUtil;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.consumer.TopicFilter;
import kafka.consumer.Whitelist;
import kafka.consumer.Blacklist;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Message reader consumer raw Kafka messages.
 *
 * @author Pawel Garbacki (pawel@pinterest.com)
 */
public class MessageReader {
    private static final Logger LOG = LoggerFactory.getLogger(MessageReader.class);

    protected SecorConfig mConfig;
    protected OffsetTracker mOffsetTracker;
    protected ConsumerConnector mConsumerConnector;
    protected ConsumerIterator mIterator;
    protected HashMap<TopicPartition, Long> mLastAccessTime;
    protected final int mTopicPartitionForgetSeconds;
    protected final int mCheckMessagesPerSecond;
    protected int mNMessages;
    protected KafkaMessageTimestampFactory mKafkaMessageTimestampFactory;

    public MessageReader(SecorConfig config, OffsetTracker offsetTracker) throws
            UnknownHostException {
        mConfig = config;
        mOffsetTracker = offsetTracker;

        mConsumerConnector = Consumer.createJavaConsumerConnector(createConsumerConfig());

        if (!mConfig.getKafkaTopicBlacklist().isEmpty() && !mConfig.getKafkaTopicFilter().isEmpty()) {
            throw new RuntimeException("Topic filter and blacklist cannot be both specified.");
        }
        TopicFilter topicFilter = !mConfig.getKafkaTopicBlacklist().isEmpty()? new Blacklist(mConfig.getKafkaTopicBlacklist()):
                new Whitelist(mConfig.getKafkaTopicFilter());
        LOG.debug("Use TopicFilter {}({})", topicFilter.getClass(), topicFilter);
        List<KafkaStream<byte[], byte[]>> streams =
            mConsumerConnector.createMessageStreamsByFilter(topicFilter);
        KafkaStream<byte[], byte[]> stream = streams.get(0);
        mIterator = stream.iterator();
        mLastAccessTime = new HashMap<TopicPartition, Long>();
        StatsUtil.setLabel("secor.kafka.consumer.id", IdUtil.getConsumerId());
        mTopicPartitionForgetSeconds = mConfig.getTopicPartitionForgetSeconds();
        mCheckMessagesPerSecond = mConfig.getMessagesPerSecond() / mConfig.getConsumerThreads();
        mKafkaMessageTimestampFactory = new KafkaMessageTimestampFactory(mConfig.getKafkaMessageTimestampClass());
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

    private ConsumerConfig createConsumerConfig() throws UnknownHostException {
        Properties props = new Properties();
        props.put("zookeeper.connect", mConfig.getZookeeperQuorum() + mConfig.getKafkaZookeeperPath());
        props.put("group.id", mConfig.getKafkaGroup());

        props.put("zookeeper.session.timeout.ms",
                  Integer.toString(mConfig.getZookeeperSessionTimeoutMs()));
        props.put("zookeeper.sync.time.ms", Integer.toString(mConfig.getZookeeperSyncTimeMs()));
        props.put("auto.commit.enable", "false");
        props.put("auto.offset.reset", mConfig.getConsumerAutoOffsetReset());
        props.put("consumer.timeout.ms", Integer.toString(mConfig.getConsumerTimeoutMs()));
        props.put("consumer.id", IdUtil.getConsumerId());
        // Properties required to upgrade from kafka 0.8.x to 0.9.x
        props.put("dual.commit.enabled", mConfig.getDualCommitEnabled());
        props.put("offsets.storage", mConfig.getOffsetsStorage());

        props.put("partition.assignment.strategy", mConfig.getPartitionAssignmentStrategy());
        if (mConfig.getRebalanceMaxRetries() != null &&
            !mConfig.getRebalanceMaxRetries().isEmpty()) {
            props.put("rebalance.max.retries", mConfig.getRebalanceMaxRetries());
        }
        if (mConfig.getRebalanceBackoffMs() != null &&
            !mConfig.getRebalanceBackoffMs().isEmpty()) {
            props.put("rebalance.backoff.ms", mConfig.getRebalanceBackoffMs());
        }
        if (mConfig.getSocketReceiveBufferBytes() != null &&
            !mConfig.getSocketReceiveBufferBytes().isEmpty()) {
            props.put("socket.receive.buffer.bytes", mConfig.getSocketReceiveBufferBytes());
        }
        if (mConfig.getFetchMessageMaxBytes() != null && !mConfig.getFetchMessageMaxBytes().isEmpty()) {
            props.put("fetch.message.max.bytes", mConfig.getFetchMessageMaxBytes());
        }
        if (mConfig.getFetchMinBytes() != null && !mConfig.getFetchMinBytes().isEmpty()) {
            props.put("fetch.min.bytes", mConfig.getFetchMinBytes());
        }
        if (mConfig.getFetchWaitMaxMs() != null && !mConfig.getFetchWaitMaxMs().isEmpty()) {
            props.put("fetch.wait.max.ms", mConfig.getFetchWaitMaxMs());
        }

        return new ConsumerConfig(props);
    }

    public boolean hasNext() {
        return mIterator.hasNext();
    }

    public Message read() {
        assert hasNext();
        mNMessages = (mNMessages + 1) % mCheckMessagesPerSecond;
        if (mNMessages % mCheckMessagesPerSecond == 0) {
            RateLimitUtil.acquire(mCheckMessagesPerSecond);
        }
        MessageAndMetadata<byte[], byte[]> kafkaMessage = mIterator.next();

        long timestamp = (mConfig.useKafkaTimestamp())
                ? mKafkaMessageTimestampFactory.getKafkaMessageTimestamp().getTimestamp(kafkaMessage)
                : 0l;
        Message message = new Message(kafkaMessage.topic(), kafkaMessage.partition(),
                                      kafkaMessage.offset(), kafkaMessage.key(),
                                      kafkaMessage.message(), timestamp);
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
}
