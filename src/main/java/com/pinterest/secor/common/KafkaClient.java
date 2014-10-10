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

    public KafkaClient(SecorConfig config) {
        mConfig = config;
        mZookeeperConnector = new ZookeeperConnector(mConfig);
    }

    private HostAndPort findLeader(TopicPartition topicPartition) {
        SimpleConsumer consumer = null;
        try {
            LOG.info("looking up leader for topic " + topicPartition.getTopic() + " partition " +
                topicPartition.getPartition());
            consumer = new SimpleConsumer(mConfig.getKafkaSeedBrokerHost(),
                    mConfig.getKafkaSeedBrokerPort(),
                    100000, 64 * 1024, "leaderLookup");
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
        LOG.info("fetching message topic " + topicPartition.getTopic() + " partition " +
                topicPartition.getPartition() + " offset " + offset);
        final int MAX_MESSAGE_SIZE_BYTES = mConfig.getMaxMessageSizeBytes();
        final String clientName = getClientName(topicPartition);
        kafka.api.FetchRequest request = new FetchRequestBuilder().clientId(clientName)
                .addFetch(topicPartition.getTopic(), topicPartition.getPartition(), offset,
                          MAX_MESSAGE_SIZE_BYTES)
                .build();
        FetchResponse response = consumer.fetch(request);
        if (response.hasError()) {
            consumer.close();
            throw new RuntimeException("Error fetching offset data. Reason: " +
                    response.errorCode(topicPartition.getTopic(), topicPartition.getPartition()));
        }
        MessageAndOffset messageAndOffset = response.messageSet(
                topicPartition.getTopic(), topicPartition.getPartition()).iterator().next();
        ByteBuffer payload = messageAndOffset.message().payload();
        byte[] payloadBytes = new byte[payload.limit()];
        payload.get(payloadBytes);
        return new Message(topicPartition.getTopic(), topicPartition.getPartition(),
                messageAndOffset.offset(), payloadBytes);
    }

   public SimpleConsumer createConsumer(TopicPartition topicPartition) {
        HostAndPort leader = findLeader(topicPartition);
        LOG.info("leader for topic " + topicPartition.getTopic() + " partition " +
                 topicPartition.getPartition() + " is " + leader.toString());
        final String clientName = getClientName(topicPartition);
        return new SimpleConsumer(leader.getHostText(), leader.getPort(), 100000, 64 * 1024,
                                  clientName);
    }

    public int getNumPartitions(String topic) {
        SimpleConsumer consumer = null;
        try {
            consumer = new SimpleConsumer(mConfig.getKafkaSeedBrokerHost(),
                    mConfig.getKafkaSeedBrokerPort(),
                    100000, 64 * 1024, "partitionLookup");
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
        SimpleConsumer consumer = createConsumer(topicPartition);
        long lastOffset = findLastOffset(topicPartition, consumer);
        if (lastOffset < 1) {
            return null;
        }
        return getMessage(topicPartition, lastOffset, consumer);
    }

    public Message getCommittedMessage(TopicPartition topicPartition) throws Exception {
        long committedOffset = mZookeeperConnector.getCommittedOffsetCount(topicPartition) - 1;
        if (committedOffset < 0) {
            return null;
        }
        SimpleConsumer consumer = createConsumer(topicPartition);
        return getMessage(topicPartition, committedOffset, consumer);
    }
}
