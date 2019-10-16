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

import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.common.TopicPartition;
import com.pinterest.secor.message.Message;
import com.pinterest.secor.timestamp.KafkaMessageTimestampFactory;
import com.pinterest.secor.util.IdUtil;
import kafka.consumer.Blacklist;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.ConsumerTimeoutException;
import kafka.consumer.KafkaStream;
import kafka.consumer.TopicFilter;
import kafka.consumer.Whitelist;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.util.List;
import java.util.Properties;

public class LegacyKafkaMessageIterator implements KafkaMessageIterator {
    private static final Logger LOG = LoggerFactory.getLogger(MessageReader.class);
    private SecorConfig mConfig;
    private ConsumerConnector mConsumerConnector;
    private ConsumerIterator mIterator;
    private KafkaMessageTimestampFactory mKafkaMessageTimestampFactory;

    public LegacyKafkaMessageIterator() {
    }

    @Override
    public boolean hasNext() {
        try {
            return mIterator.hasNext();
        } catch (ConsumerTimeoutException e) {
            throw new LegacyConsumerTimeoutException(e);
        }
    }

    @Override
    public Message next() {
        MessageAndMetadata<byte[], byte[]> kafkaMessage;
        try {
            kafkaMessage = mIterator.next();
        } catch (ConsumerTimeoutException e) {
            throw new LegacyConsumerTimeoutException(e);
        }

        long timestamp = 0L;
        if (mConfig.useKafkaTimestamp()) {
            timestamp = mKafkaMessageTimestampFactory.getKafkaMessageTimestamp().getTimestamp(kafkaMessage);
        }

        return new Message(kafkaMessage.topic(), kafkaMessage.partition(),
                kafkaMessage.offset(), kafkaMessage.key(),
                kafkaMessage.message(), timestamp, null);
    }

    @Override
    public void init(SecorConfig config) throws UnknownHostException {
        this.mConfig = config;

        mConsumerConnector = Consumer.createJavaConsumerConnector(createConsumerConfig());

        if (!mConfig.getKafkaTopicBlacklist().isEmpty() && !mConfig.getKafkaTopicFilter().isEmpty()) {
            throw new RuntimeException("Topic filter and blacklist cannot be both specified.");
        }
        TopicFilter topicFilter = !mConfig.getKafkaTopicBlacklist().isEmpty() ? new Blacklist(mConfig.getKafkaTopicBlacklist()) :
                new Whitelist(mConfig.getKafkaTopicFilter());
        LOG.debug("Use TopicFilter {}({})", topicFilter.getClass(), topicFilter);
        List<KafkaStream<byte[], byte[]>> streams =
                mConsumerConnector.createMessageStreamsByFilter(topicFilter);
        KafkaStream<byte[], byte[]> stream = streams.get(0);
        mIterator = stream.iterator();
        mKafkaMessageTimestampFactory = new KafkaMessageTimestampFactory(mConfig.getKafkaMessageTimestampClass());
    }

    @Override
    public void commit(TopicPartition topicPartition, long offset) {
        mConsumerConnector.commitOffsets();
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
}
