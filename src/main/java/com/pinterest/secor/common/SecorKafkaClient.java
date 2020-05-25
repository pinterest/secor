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
package com.pinterest.secor.common;

import com.pinterest.secor.message.Message;
import com.pinterest.secor.message.MessageHeader;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class SecorKafkaClient implements KafkaClient {
    public static final int MAX_READ_POLL_ATTEMPTS = 10;
    private static final Logger LOG = LoggerFactory.getLogger(SecorKafkaClient.class);
    private KafkaConsumer<byte[], byte[]> mKafkaConsumer;
    private AdminClient mKafkaAdminClient;
    private ZookeeperConnector mZookeeperConnector;
    private int mPollTimeout;

    @Override
    public int getNumPartitions(String topic) {
        Map<String, KafkaFuture<TopicDescription>> description = mKafkaAdminClient.describeTopics(Collections.singleton(topic)).values();
        int numPartitions;
        try {
            numPartitions = description.get(topic).get().partitions().size();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }

        return numPartitions;
    }

    @Override
    public Message getLastMessage(TopicPartition topicPartition) throws TException {
        org.apache.kafka.common.TopicPartition kafkaTopicPartition = new org.apache.kafka.common.TopicPartition(topicPartition.getTopic(), topicPartition.getPartition());
        mKafkaConsumer.assign(Collections.singleton(kafkaTopicPartition));
        long endOffset = mKafkaConsumer.endOffsets(Collections.singleton(kafkaTopicPartition)).get(kafkaTopicPartition);
        mKafkaConsumer.seek(kafkaTopicPartition, endOffset - 1);

        return readSingleMessage(mKafkaConsumer);
    }

    @Override
    public Message getCommittedMessage(TopicPartition topicPartition) throws Exception {
        org.apache.kafka.common.TopicPartition kafkaTopicPartition = new org.apache.kafka.common.TopicPartition(topicPartition.getTopic(), topicPartition.getPartition());
        mKafkaConsumer.assign(Collections.singleton(kafkaTopicPartition));
        long committedOffset = mZookeeperConnector.getCommittedOffsetCount(topicPartition);
        mKafkaConsumer.seek(kafkaTopicPartition, committedOffset - 1);

        return readSingleMessage(mKafkaConsumer);
    }

    private Message readSingleMessage(KafkaConsumer<byte[], byte[]> kafkaConsumer) {
        int pollAttempts = 0;
        Message message = null;
        while (pollAttempts < MAX_READ_POLL_ATTEMPTS) {
            Iterator<ConsumerRecord<byte[], byte[]>> records = kafkaConsumer.poll(Duration.ofSeconds(mPollTimeout)).iterator();
            if (!records.hasNext()) {
                pollAttempts++;
            } else {
                ConsumerRecord<byte[], byte[]> record = records.next();
                List<MessageHeader> headers = new ArrayList<>();
                record.headers().forEach(header -> headers.add(new MessageHeader(header.key(), header.value())));
                message = new Message(record.topic(), record.partition(), record.offset(), record.key(), record.value(), record.timestamp(), headers);
                break;
            }
        }

        if (message == null) {
            LOG.warn("unable to fetch message after " + MAX_READ_POLL_ATTEMPTS + " Retries");
        }
        return message;
    }

    @Override
    public void init(SecorConfig config) {
        mZookeeperConnector = new ZookeeperConnector(config);
        mPollTimeout = config.getNewConsumerPollTimeoutSeconds();
        Properties props = new Properties();
        props.put("bootstrap.servers", config.getKafkaSeedBrokerHost() + ":" + config.getKafkaSeedBrokerPort());
        props.put("enable.auto.commit", false);
        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", ByteArrayDeserializer.class);
        props.put("value.deserializer", ByteArrayDeserializer.class);
        props.put("max.poll.records", 1);
        mKafkaConsumer = new KafkaConsumer<>(props);
        mKafkaAdminClient = KafkaAdminClient.create(props);
    }
}
