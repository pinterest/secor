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
package com.pinterest.secor.tools;

import com.pinterest.secor.common.KafkaClient;
import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.common.TopicPartition;
import com.pinterest.secor.common.ZookeeperConnector;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * OffsetCheck compares secor committed offsets to kafka offsets and lets you fix secor
 * offsets to earliest kafka offset.
 *
 * @author Ramki Venkatachalam (ramki@pinterest.com)
 */
public class OffsetCheck {

  private static final Logger LOG = LoggerFactory.getLogger(OffsetCheck.class);

  private SecorConfig mConfig;
  private ZookeeperConnector mZookeeperConnector;
  private KafkaClient mKafkaClient;

  public OffsetCheck(SecorConfig config)
      throws Exception {
    mConfig = config;
    mZookeeperConnector = new ZookeeperConnector(mConfig);
    mKafkaClient = new KafkaClient(mConfig);
  }

  public void printOffsets(boolean fix) throws Exception {
    kafka.javaapi.consumer.SimpleConsumer  consumer = new SimpleConsumer(mConfig.getKafkaSeedBrokerHost(),
        mConfig.getKafkaSeedBrokerPort(),
        100000, 64 * 1024, "topicsInfo");

    List<String> topicsList = new ArrayList<String>();
    TopicMetadataRequest req = new TopicMetadataRequest(topicsList);
    kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);
    consumer.close();
    List<kafka.javaapi.TopicMetadata> topicMetadataList = resp.topicsMetadata();
    for (kafka.javaapi.TopicMetadata topicMetadata : topicMetadataList) {
        final String topic = topicMetadata.topic();
        if (!topic.matches(mConfig.getKafkaTopicFilter())) {
        LOG.info("skipping topic " + topic);
        continue;
      }
      System.out.println("Topic: " + topicMetadata.topic() + " #Partitions: " +
          topicMetadata.partitionsMetadata().size() );

      for (kafka.javaapi.PartitionMetadata part : topicMetadata.partitionsMetadata()) {
        TopicPartition topicPartition = new TopicPartition(topic, part.partitionId());

        // get offsets from kafka
        SimpleConsumer topicConsumer = mKafkaClient.createConsumer(topicPartition);
        long earliestOffset = mKafkaClient.earliestFirstOffset(topicPartition, topicConsumer);
        long lastOffset = mKafkaClient.findLastOffset(topicPartition, topicConsumer);
        topicConsumer.close();
        if (earliestOffset < 1 || lastOffset < 1) {
          // no messages in kafka
          continue;
        }

        // get committed offset from zookeeper
        long committedOffset = -1;
        try {
          committedOffset = mZookeeperConnector.getCommittedOffsetCount(topicPartition);
        } catch (Exception e) {
          //LOG.warn("No committed offset found for {}", topicPartition);
        }

        if (committedOffset < earliestOffset || committedOffset > lastOffset || committedOffset <
            1   ) {
          System.out.print("WARNING: Committed offset out of range for ");
        }
        System.out.println("topic " + topic + " partition " + part.partitionId() +
            " earliest offset:" + earliestOffset +
            ", committed offset:" + committedOffset +
            ", last offset:" + lastOffset + ". ");
        if (fix && (committedOffset < earliestOffset || committedOffset > lastOffset ||
            committedOffset < 1 )) {
            System.out.printf("Updating to : %d\n", earliestOffset);
            mZookeeperConnector.setCommittedOffsetCount(topicPartition, earliestOffset);
        }
      }
    }
  }

}
