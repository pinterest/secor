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
// Uncomment below to have this run using Kafka 0.10
package com.pinterest.secor.performance;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import kafka.admin.RackAwareMode;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.pinterest.secor.common.KafkaClient;
import com.pinterest.secor.common.OffsetTracker;
import com.pinterest.secor.common.OstrichAdminService;
import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.common.TopicPartition;
import com.pinterest.secor.consumer.Consumer;
import com.pinterest.secor.tools.LogFileDeleter;
import com.pinterest.secor.util.FileUtil;
import com.pinterest.secor.util.RateLimitUtil;

import kafka.admin.AdminUtils;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.common.TopicExistsException;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.utils.ZKStringSerializer$;
import org.I0Itec.zkclient.ZkConnection;

/**
 * A performance test for secor
 *
 * * Run: $ cd optimus/secor $ mvn package $ cd target $ java -ea
 * -Dlog4j.configuration=log4j.dev.properties
 * -Dconfig=secor.test.perf.backup.properties \ -cp
 * "secor-0.1-SNAPSHOT-tests.jar:lib/*:secor-0.1-SNAPSHOT.jar"
 * com.pinterest.secor.performance.PerformanceTest <num_topics> <num_partitions>
 * <num_records> <message_size>
 *
 * @author Praveen Murugesan(praveen@uber.com)
 *
 */
public class PerformanceTest010 {

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("USAGE: java " + PerformanceTest010.class.getName()
                    + " num_topics num_partitions num_records message_size");
            System.exit(1);
        }
        Random rnd = new Random();
        int num_topics = Integer.parseInt(args[0]);
        SecorConfig config = SecorConfig.load();
        String zkConfig = config.getZookeeperQuorum()
                + config.getKafkaZookeeperPath();
        // create topics list
        String perfTopicPrefix = config.getPerfTestTopicPrefix();
        List<String> topics = Lists.newLinkedList();
        for (int i = 0; i < num_topics; i++) {
            topics.add(perfTopicPrefix + rnd.nextInt(9999));
        }

        int num_partitions = Integer.parseInt(args[1]);

        // createTopics
        createTopics(topics, num_partitions, zkConfig);

        int numRecords = Integer.parseInt(args[2]);
        Properties props = new Properties();
        props.put("metadata.broker.list", "localhost:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("request.required.acks", "1");
        props.put("producer.type", "async");

        ProducerConfig producerConfig = new ProducerConfig(props);

        Producer<String, String> producer = new Producer<String, String>(
                producerConfig);
        long size = 0;
        int message_size = Integer.parseInt(args[3]);

        // produce messages
        for (String topic : topics) {
            for (long nEvents = 0; nEvents < numRecords; nEvents++) {
                String ip = String.valueOf(nEvents % num_partitions);
                byte[] payload = new byte[message_size];
                Arrays.fill(payload, (byte) 1);
                String msg = new String(payload, "UTF-8");
                size += msg.length();
                KeyedMessage<String, String> data = new KeyedMessage<String, String>(
                        topic, ip, msg);
                producer.send(data);
            }
        }
        producer.close();

        RateLimitUtil.configure(config);
        Map<TopicPartition, Long> lastOffsets = getTopicMetadata(topics,
                num_partitions, config);
        OstrichAdminService ostrichService = new OstrichAdminService(
                config.getOstrichPort());
        ostrichService.start();
        FileUtil.configure(config);

        LogFileDeleter logFileDeleter = new LogFileDeleter(config);
        logFileDeleter.deleteOldLogs();
        Thread.UncaughtExceptionHandler handler = new Thread.UncaughtExceptionHandler() {
            public void uncaughtException(Thread thread, Throwable exception) {
                exception.printStackTrace();
                System.out.println("Thread " + thread + " failed:"
                        + exception.getMessage());
                System.exit(1);
            }
        };
        System.out.println("starting " + config.getConsumerThreads()
                + " consumer threads");
        System.out.println("Rate limit:" + config.getMessagesPerSecond());
        LinkedList<Consumer> consumers = new LinkedList<Consumer>();
        long startMillis = System.currentTimeMillis();
        for (int i = 0; i < config.getConsumerThreads(); ++i) {
            Consumer consumer = new Consumer(config);
            consumer.setUncaughtExceptionHandler(handler);
            consumers.add(consumer);
            consumer.start();
        }

        while (true) {
            for (Consumer consumer : consumers) {
                for (String topic : topics) {
                    for (int i = 0; i < num_partitions; i++) {
                        OffsetTracker offsetTracker = consumer
                                .getOffsetTracker();
                        long val = (offsetTracker == null) ? -1
                                : offsetTracker
                                        .getLastSeenOffset(new TopicPartition(
                                                topic, i)) + 1;

                        System.out.println("topic:" + topic + " partition:" + i
                                + " secor offset:" + val + " elapsed:"
                                + (System.currentTimeMillis() - startMillis));
                        Long lastOffset = lastOffsets.get(new TopicPartition(
                                topic, i));
                        if (lastOffset != null && lastOffset == val) {
                            lastOffsets.remove(new TopicPartition(topic, i));
                        }
                    }
                }
            }

            // time break to measure
            Thread.sleep(1000);
            System.out.println("last offsets size:" + lastOffsets.size());
            if (lastOffsets.isEmpty()) {
                long endMillis = System.currentTimeMillis();
                System.out.println("Completed in:" + (endMillis - startMillis));
                System.out.println("Total bytes:" + size);
                // wait for the last file to be written
                Thread.sleep(60000);
                break;
            }
        }

        System.exit(1);
    }

    /**
     * Get topic partition to last offset map
     *
     * @param topics
     * @param num_partitions
     * @param config
     * @return
     */
    private static Map<TopicPartition, Long> getTopicMetadata(
            List<String> topics, int num_partitions, SecorConfig config) {
        KafkaClient mKafkaClient = new KafkaClient(config);

        Map<TopicPartition, Long> lastOffsets = Maps.newHashMap();
        for (String topic : topics) {
            for (int i = 0; i < num_partitions; i++) {
                TopicAndPartition topicAndPartition = new TopicAndPartition(
                        topic, i);
                SimpleConsumer consumer = mKafkaClient
                        .createConsumer(new TopicPartition(topic, i));
                Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
                requestInfo.put(topicAndPartition,
                        new PartitionOffsetRequestInfo(-1, 1));
                kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(
                        requestInfo, kafka.api.OffsetRequest.CurrentVersion(),
                        "TestPerformance");
                OffsetResponse response = consumer.getOffsetsBefore(request);
                if (response.hasError()) {
                    System.out
                            .println("Error fetching data Offset Data the Broker. Reason: "
                                    + response.errorCode(topic, i));
                    return null;
                }
                long[] offsets = response.offsets(topic, i);
                System.out.println("Topic: " + topic + " partition: " + i
                        + " offset: " + offsets[0]);
                lastOffsets.put(new TopicPartition(topic, i), offsets[0]);
            }
        }
        return lastOffsets;
    }

    /**
     * Helper to create topics
     *
     * @param topics
     * @param partitions
     * @param zkConfig
     * @throws InterruptedException
     */
    private static void createTopics(List<String> topics, int partitions,
            String zkConfig) throws InterruptedException {

        ZkConnection zkConnection = new ZkConnection(zkConfig);
        ZkClient zkClient = createZkClient(zkConfig);
        ZkUtils zkUtils = new ZkUtils(zkClient, zkConnection, false);

        try {
            Properties props = new Properties();
            int replicationFactor = 1;
            for (String topic : topics) {
                AdminUtils.createTopic(zkUtils, topic, partitions,
                        replicationFactor, props, RackAwareMode.Disabled$.MODULE$);
            }
        } catch (TopicExistsException e) {
            System.out.println(e.getMessage());
        } finally {
            zkClient.close();
        }

    }

    /**
     * Helper to create ZK client
     *
     * @param zkConfig
     * @return
     */
    private static ZkClient createZkClient(String zkConfig) {
        // Create a ZooKeeper client
        int sessionTimeoutMs = 10000;
        int connectionTimeoutMs = 10000;
        ZkClient zkClient = new ZkClient(zkConfig, sessionTimeoutMs,
                connectionTimeoutMs, ZKStringSerializer$.MODULE$);
        return zkClient;
    }

}