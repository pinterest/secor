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
package com.pinterest.secor.common;

import com.google.common.net.HostAndPort;
import com.pinterest.secor.message.Message;
import com.pinterest.secor.timestamp.KafkaMessageTimestampFactory;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;
import org.apache.kafka.common.protocol.Errors;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Kafka client encapsulates the logic interacting with Kafka brokers.
 *
 * @author Pawel Garbacki (pawel@pinterest.com)
 */
public class KafkaClient {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaClient.class);

    private SecorConfig mConfig;
    private ZookeeperConnector mZookeeperConnector;
    private KafkaMessageTimestampFactory mKafkaMessageTimestampFactory;

    public KafkaClient(SecorConfig config) {
        mConfig = config;
        mZookeeperConnector = new ZookeeperConnector(mConfig);
        mKafkaMessageTimestampFactory = new KafkaMessageTimestampFactory(mConfig.getKafkaMessageTimestampClass());
    }

    public class MessageDoesNotExistException extends RuntimeException {}

    private HostAndPort findLeader(TopicPartition topicPartition) {
        SimpleConsumer consumer = null;
        try {
            LOG.debug("looking up leader for topic {} partition {}", topicPartition.getTopic(), topicPartition.getPartition());
            consumer = createConsumer(
                mConfig.getKafkaSeedBrokerHost(),
                mConfig.getKafkaSeedBrokerPort(),
                "leaderLookup");
            List<String> topics = new ArrayList<String>();
            topics.add(topicPartition.getTopic());
            TopicMetadataRequest request = new TopicMetadataRequest(topics);
            TopicMetadataResponse response = consumer.send(request);

            List<TopicMetadata> metaData = response.topicsMetadata();
            for (TopicMetadata item : metaData) {
                for (PartitionMetadata part : item.partitionsMetadata()) {
                    if (part.partitionId() == topicPartition.getPartition()) {
                        return HostAndPort.fromParts(part.leader().host(), part.leader().port());
                    }
                }
            }
        } finally {
            if (consumer != null) {
                consumer.close();
            }
        }
        return null;
    }

    private static String getClientName(TopicPartition topicPartition) {
        return "secorClient_" + topicPartition.getTopic() + "_" + topicPartition.getPartition();
    }

    private long findLastOffset(TopicPartition topicPartition, SimpleConsumer consumer) {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topicPartition.getTopic(),
                topicPartition.getPartition());
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo =
                new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(
                kafka.api.OffsetRequest.LatestTime(), 1));
        final String clientName = getClientName(topicPartition);
        OffsetRequest request = new OffsetRequest(requestInfo,
                                                  kafka.api.OffsetRequest.CurrentVersion(),
                                                  clientName);
        OffsetResponse response = consumer.getOffsetsBefore(request);

        if (response.hasError()) {
            throw new RuntimeException("Error fetching offset data. Reason: " +
                    response.errorCode(topicPartition.getTopic(), topicPartition.getPartition()));
        }
        long[] offsets = response.offsets(topicPartition.getTopic(),
                topicPartition.getPartition());
        return offsets[0] - 1;
    }

    private Message getMessage(TopicPartition topicPartition, long offset,
                               SimpleConsumer consumer) {
        LOG.debug("fetching message topic {} partition {} offset {}",
                topicPartition.getTopic(), topicPartition.getPartition(), offset);
        final int MAX_MESSAGE_SIZE_BYTES = mConfig.getMaxMessageSizeBytes();
        final String clientName = getClientName(topicPartition);
        kafka.api.FetchRequest request = new FetchRequestBuilder().clientId(clientName)
                .addFetch(topicPartition.getTopic(), topicPartition.getPartition(), offset,
                          MAX_MESSAGE_SIZE_BYTES)
                .build();
        FetchResponse response = consumer.fetch(request);
        if (response.hasError()) {
            consumer.close();
            int errorCode = response.errorCode(topicPartition.getTopic(), topicPartition.getPartition());

            if (errorCode == Errors.OFFSET_OUT_OF_RANGE.code()) {
              throw new MessageDoesNotExistException();
            } else {
              throw new RuntimeException("Error fetching offset data. Reason: " + errorCode);
            }
        }
        MessageAndOffset messageAndOffset = response.messageSet(
                topicPartition.getTopic(), topicPartition.getPartition()).iterator().next();
        byte[] keyBytes = null;
        if (messageAndOffset.message().hasKey()) {
            ByteBuffer key = messageAndOffset.message().key();
            keyBytes = new byte[key.limit()];
            key.get(keyBytes);
        }
        byte[] payloadBytes = null;
        if (!messageAndOffset.message().isNull()) {
            ByteBuffer payload = messageAndOffset.message().payload();
            payloadBytes = new byte[payload.limit()];
            payload.get(payloadBytes);
        }
        long timestamp = (mConfig.useKafkaTimestamp())
                ? mKafkaMessageTimestampFactory.getKafkaMessageTimestamp().getTimestamp(messageAndOffset)
                : 0l;

        return new Message(topicPartition.getTopic(), topicPartition.getPartition(),
                messageAndOffset.offset(), keyBytes, payloadBytes, timestamp);
    }

    private SimpleConsumer createConsumer(String host, int port, String clientName) {
        return new SimpleConsumer(host, port, 100000, 64 * 1024, clientName);
    }

    private SimpleConsumer createConsumer(TopicPartition topicPartition) {
        HostAndPort leader = findLeader(topicPartition);
        if (leader == null) {
            LOG.warn("no leader for topic {} partition {}", topicPartition.getTopic(), topicPartition.getPartition());
            return null;
        }
        LOG.debug("leader for topic {} partition {} is {}", topicPartition.getTopic(), topicPartition.getPartition(), leader);
        final String clientName = getClientName(topicPartition);
        return createConsumer(leader.getHostText(), leader.getPort(), clientName);
    }

    public int getNumPartitions(String topic) {
        SimpleConsumer consumer = null;
        try {
            consumer = createConsumer(
                mConfig.getKafkaSeedBrokerHost(),
                mConfig.getKafkaSeedBrokerPort(),
                "partitionLookup");
            List<String> topics = new ArrayList<String>();
            topics.add(topic);
            TopicMetadataRequest request = new TopicMetadataRequest(topics);
            TopicMetadataResponse response = consumer.send(request);
            if (response.topicsMetadata().size() != 1) {
                throw new RuntimeException("Expected one metadata for topic " + topic + " found " +
                    response.topicsMetadata().size());
            }
            TopicMetadata topicMetadata = response.topicsMetadata().get(0);
            return topicMetadata.partitionsMetadata().size();
        } finally {
            if (consumer != null) {
                consumer.close();
            }
        }
    }

    public Message getLastMessage(TopicPartition topicPartition) throws TException {
        SimpleConsumer consumer = null;
        try {
            consumer = createConsumer(topicPartition);
            if (consumer == null) {
                return null;
            }
            long lastOffset = findLastOffset(topicPartition, consumer);
            if (lastOffset < 1) {
                return null;
            }
            return getMessage(topicPartition, lastOffset, consumer);
        } finally {
            if (consumer != null) {
                consumer.close();
            }
        }
    }

    public Message getCommittedMessage(TopicPartition topicPartition) throws Exception {
        SimpleConsumer consumer = null;
        try {
            long committedOffset = mZookeeperConnector.getCommittedOffsetCount(topicPartition) - 1;
            if (committedOffset < 0) {
                return null;
            }
            consumer = createConsumer(topicPartition);
            if (consumer == null) {
                return null;
            }
            return getMessage(topicPartition, committedOffset, consumer);
        } catch (MessageDoesNotExistException e) {
          // If a RuntimeEMessageDoesNotExistException exception is raised,
          // the message at the last comitted offset does not exist in Kafka.
          // This is usually due to the message being compacted away by the
          // Kafka log compaction process.
          //
          // That is no an exceptional situation - in fact it can be normal if
          // the topic being consumed by Secor has a low volume. So in that
          // case, simply return null
          LOG.warn("no committed message for topic {} partition {}", topicPartition.getTopic(), topicPartition.getPartition());
          return null;
        } finally {
            if (consumer != null) {
                consumer.close();
            }
        }
    }
}
