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

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.pinterest.secor.common.KafkaProperties;
import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.common.ZookeeperConnector;
import com.pinterest.secor.message.Message;
import com.pinterest.secor.message.MessageHeader;
import com.pinterest.secor.rebalance.RebalanceHandler;
import com.pinterest.secor.rebalance.RebalanceSubscriber;
import com.pinterest.secor.rebalance.SecorConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.regex.Pattern;

public class SecorKafkaMessageIterator implements KafkaMessageIterator, RebalanceSubscriber {
    private static final Logger LOG = LoggerFactory.getLogger(SecorKafkaMessageIterator.class);
    private KafkaConsumer<byte[], byte[]> mKafkaConsumer;
    private Deque<ConsumerRecord<byte[], byte[]>> mRecordsBatch;
    private ZookeeperConnector mZookeeperConnector;
    private int mPollTimeout;

    @Override
    public boolean hasNext() {
        return true;
    }

    @Override
    public Message next() {
        if (mRecordsBatch.isEmpty()) {
            mKafkaConsumer.poll(Duration.ofSeconds(mPollTimeout)).forEach(mRecordsBatch::add);
        }

        if (mRecordsBatch.isEmpty()) {
            return null;
        } else {
            ConsumerRecord<byte[], byte[]> consumerRecord = mRecordsBatch.pop();
            List<MessageHeader> headers = new ArrayList<>();
            consumerRecord.headers().forEach(header -> headers.add(new MessageHeader(header.key(), header.value())));
            return new Message(consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset(),
                    consumerRecord.key(), consumerRecord.value(), consumerRecord.timestamp(), headers);
        }
    }

    @Override
    public void init(SecorConfig config) throws UnknownHostException {
        mPollTimeout = config.getNewConsumerPollTimeoutSeconds();
        mZookeeperConnector = new ZookeeperConnector(config);
        mRecordsBatch = new ArrayDeque<>();
        mKafkaConsumer = new KafkaConsumer<>(KafkaProperties.getConsumerProperties(config));
    }

    @Override
    public void commit(com.pinterest.secor.common.TopicPartition topicPartition, long offset) {
        TopicPartition kafkaTopicPartition = new TopicPartition(topicPartition.getTopic(), topicPartition.getPartition());
        OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(offset);

        try {
            LOG.info("committing {} offset {} to kafka", topicPartition, offset);
            mKafkaConsumer.commitSync(ImmutableMap.of(kafkaTopicPartition, offsetAndMetadata));
        } catch (CommitFailedException e) {
            LOG.trace("kafka commit failed due to group re-balance", e);
        }
    }

    @Override
    public void subscribe(RebalanceHandler handler, SecorConfig config) {
        ConsumerRebalanceListener reBalanceListener = new SecorConsumerRebalanceListener(mKafkaConsumer, mZookeeperConnector, getSkipZookeeperOffsetSeek(config), config.getNewConsumerAutoOffsetReset(), handler);

        String[] subscribeList = config.getKafkaTopicList();
        if (subscribeList.length == 0 || Strings.isNullOrEmpty(subscribeList[0])) {
            mKafkaConsumer.subscribe(Pattern.compile(config.getKafkaTopicFilter()), reBalanceListener);
        } else {
            mKafkaConsumer.subscribe(Arrays.asList(subscribeList), reBalanceListener);
        }
    }

    private boolean getSkipZookeeperOffsetSeek(SecorConfig config) {
        String dualCommitEnabled = config.getDualCommitEnabled();
        String offsetStorage = config.getOffsetsStorage();
        return offsetStorage.equals("kafka") && dualCommitEnabled.equals("false");
    }

    @Override
    public long getKafkaCommitedOffsetCount(final com.pinterest.secor.common.TopicPartition topicPartition) {
        TopicPartition kafkaTopicPartition = new TopicPartition(topicPartition.getTopic(), topicPartition.getPartition());
        OffsetAndMetadata offsetAndMetadata = mKafkaConsumer.committed(kafkaTopicPartition);
        if (offsetAndMetadata != null) {
            return offsetAndMetadata.offset();
        } else {
            return -1;
        }
    }
}
