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
package com.pinterest.secor.tools;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.net.HostAndPort;
import com.pinterest.secor.common.KafkaClient;
import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.common.TopicPartition;
import com.pinterest.secor.common.ZookeeperConnector;
import com.pinterest.secor.message.Message;
import com.pinterest.secor.parser.MessageParser;
import com.pinterest.secor.parser.TimestampedMessageParser;
import com.pinterest.secor.util.ReflectionUtil;
import com.timgroup.statsd.NonBlockingStatsDClient;
import net.minidev.json.JSONArray;
import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.Map;

/**
 * Progress monitor exports offset lags per topic partition.
 *
 * @author Pawel Garbacki (pawel@pinterest.com)
 */
public class ProgressMonitor {

    private static final Logger LOG = LoggerFactory.getLogger(ProgressMonitor.class);
    private static final String PERIOD = ".";

    private SecorConfig mConfig;
    private ZookeeperConnector mZookeeperConnector;
    private KafkaClient mKafkaClient;
    private MessageParser mMessageParser;
    private String mPrefix;
    private NonBlockingStatsDClient mStatsDClient;

    public ProgressMonitor(SecorConfig config)
            throws Exception
    {
        mConfig = config;
        mZookeeperConnector = new ZookeeperConnector(mConfig);
        try {
            Class timestampClass = Class.forName(mConfig.getKafkaClientClass());
            this.mKafkaClient = (KafkaClient) timestampClass.newInstance();
            this.mKafkaClient.init(config);
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
        mMessageParser = (MessageParser) ReflectionUtil.createMessageParser(
                mConfig.getMessageParserClass(), mConfig);

        mPrefix = mConfig.getMonitoringPrefix();
        if (Strings.isNullOrEmpty(mPrefix)) {
            mPrefix = "secor";
        }

        if (mConfig.getStatsDHostPort() != null && !mConfig.getStatsDHostPort().isEmpty()) {
            HostAndPort hostPort = HostAndPort.fromString(mConfig.getStatsDHostPort());
            mStatsDClient = new NonBlockingStatsDClient(null, hostPort.getHost(), hostPort.getPort(),
                    mConfig.getStatsDDogstatsdConstantTags());
        }
    }

    private void makeRequest(String body) throws IOException {
        URL url = new URL("http://" + mConfig.getTsdbHostport() + "/api/put?details");
        HttpURLConnection connection = null;
        try {
            connection = (HttpURLConnection) url.openConnection();
            connection.setRequestProperty("Content-Type", "application/json");
            connection.setRequestProperty("Accepts", "application/json");
            connection.setRequestProperty("Accept", "*/*");
            if (body != null) {
                connection.setRequestMethod("POST");
                connection.setRequestProperty("Content-Length",
                        Integer.toString(body.getBytes().length));
            }
            connection.setUseCaches (false);
            connection.setDoInput(true);
            connection.setDoOutput(true);

            if (body != null) {
                // Send request.
                DataOutputStream dataOutputStream = new DataOutputStream(
                    connection.getOutputStream());
                dataOutputStream.writeBytes(body);
                dataOutputStream.flush();
                dataOutputStream.close();
            }

            // Get Response.
            InputStream inputStream = connection.getInputStream();
            BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
            Map response = (Map) JSONValue.parse(reader);
            if (!response.get("failed").equals(0)) {
                throw new RuntimeException("url " + url + " with body " + body + " failed " +
                    JSONObject.toJSONString(response));
            }
        } catch (IOException exception) {
            if (connection != null) {
                connection.disconnect();
            }
            throw exception;
        }
    }

    private void exportToTsdb(Stat stat)
            throws IOException {
        LOG.info("exporting metric to tsdb {}", stat);
        makeRequest(stat.toString());
    }

    public void exportStats() throws Exception {
        List<Stat> stats = getStats();
        LOG.info("Stats: {}", JSONArray.toJSONString(stats));

        // if there is a valid openTSDB port configured export to openTSDB
        if (mConfig.getTsdbHostport() != null && !mConfig.getTsdbHostport().isEmpty()) {
            for (Stat stat : stats) {
                exportToTsdb(stat);
            }
        }

        // if there is a valid statsD port configured export to statsD
        if (mStatsDClient != null) {
            exportToStatsD(stats);
        }
    }

    /**
     * Helper to publish stats to statsD client
     */
    private void exportToStatsD(List<Stat> stats) {
        // group stats by kafka group
        for (Stat stat : stats) {
            @SuppressWarnings("unchecked")
            Map<String, String> tags = (Map<String, String>) stat.get(Stat.STAT_KEYS.TAGS.getName());
            long value = Long.parseLong((String) stat.get(Stat.STAT_KEYS.VALUE.getName()));
            if (mConfig.getStatsdDogstatdsTagsEnabled()) {
                String metricName = (String) stat.get(Stat.STAT_KEYS.METRIC.getName());
                String[] tagArray = new String[tags.size()];
                int i = 0;
                for (Map.Entry<String, String> e : tags.entrySet()) {
                    tagArray[i++] = e.getKey() + ':' + e.getValue();
                }
                mStatsDClient.recordGaugeValue(metricName, value, tagArray);
            } else {
                StringBuilder builder = new StringBuilder();
                if (mConfig.getStatsDPrefixWithConsumerGroup()) {
                    builder.append(tags.get(Stat.STAT_KEYS.GROUP.getName()))
                            .append(PERIOD);
                }
                String metricName = builder
                        .append((String) stat.get(Stat.STAT_KEYS.METRIC.getName()))
                        .append(PERIOD)
                        .append(tags.get(Stat.STAT_KEYS.TOPIC.getName()))
                        .append(PERIOD)
                        .append(tags.get(Stat.STAT_KEYS.PARTITION.getName()))
                        .toString();
                mStatsDClient.recordGaugeValue(metricName, value);
            }
        }
    }

    private List<Stat> getStats() throws Exception {
        List<String> topics = mZookeeperConnector.getCommittedOffsetTopics();
        List<Stat> stats = Lists.newArrayList();

        for (String topic : topics) {
            if (topic.matches(mConfig.getMonitoringBlacklistTopics()) ||
                    !topic.matches(mConfig.getKafkaTopicFilter())) {
                LOG.info("skipping topic {}", topic);
                continue;
            }
            List<Integer> partitions = mZookeeperConnector.getCommittedOffsetPartitions(topic);
            for (Integer partition : partitions) {
                try {
                    TopicPartition topicPartition = new TopicPartition(topic, partition);
                    Message committedMessage = mKafkaClient.getCommittedMessage(topicPartition);
                    long committedOffset = -1;
                    long committedTimestampMillis = -1;
                    if (committedMessage == null) {
                        LOG.warn("no committed message found in topic {} partition {}", topic, partition);
                        continue;
                    } else {
                        committedOffset = committedMessage.getOffset();
                        committedTimestampMillis = getTimestamp(committedMessage);
                    }

                    Message lastMessage = mKafkaClient.getLastMessage(topicPartition);
                    if (lastMessage == null) {
                        LOG.warn("no message found in topic {} partition {}", topic, partition);
                    } else {
                        long lastOffset = lastMessage.getOffset();
                        long lastTimestampMillis = getTimestamp(lastMessage);
                        assert committedOffset <= lastOffset: Long.toString(committedOffset) + " <= " +
                            lastOffset;

                        long offsetLag = lastOffset - committedOffset;
                        long timestampMillisLag = lastTimestampMillis - committedTimestampMillis;
                        Map<String, String> tags = ImmutableMap.of(
                                Stat.STAT_KEYS.TOPIC.getName(), topic,
                                Stat.STAT_KEYS.PARTITION.getName(), Integer.toString(partition),
                                Stat.STAT_KEYS.GROUP.getName(), mConfig.getKafkaGroup()
                        );

                        long timestamp = System.currentTimeMillis() / 1000;
                        stats.add(Stat.createInstance(metricName("lag.offsets"), tags, Long.toString(offsetLag), timestamp));
                        stats.add(Stat.createInstance(metricName("lag.seconds"), tags, Long.toString(timestampMillisLag / 1000), timestamp));

                        LOG.debug("topic {} partition {} committed offset {} last offset {} committed timestamp {} last timestamp {}",
                                topic, partition, committedOffset, lastOffset,
                                (committedTimestampMillis / 1000), (lastTimestampMillis / 1000));
                    }
                } catch (RuntimeException e) {
                    LOG.warn(e.getMessage(), e);
                }
            }
        }

        return stats;
    }

    private String metricName(String key) {
        return Joiner.on(".").join(mPrefix, key);
    }

    private long getTimestamp(Message message) throws Exception {
        try {
            if (mMessageParser instanceof TimestampedMessageParser) {
                return ((TimestampedMessageParser)mMessageParser).getTimestampMillis(message);
            } else {
                return -1;
            }
        } catch (Exception ex){
            LOG.warn("Could not parse timestamp, returning -1: " + ex.getMessage());
            return -1;
        }

    }

    /**
     *
     * JSON hash map extension to store statistics
     *
     */
    private static class Stat extends JSONObject {

        // definition of all the stat keys
        public enum STAT_KEYS {
            METRIC("metric"),
            TAGS("tags"),
            VALUE("value"),
            TIMESTAMP("timestamp"),
            TOPIC("topic"),
            PARTITION("partition"),
            GROUP("group");

            STAT_KEYS(String name) {
                this.mName = name;
            }

            private final String mName;

            public String getName() {
                return this.mName;
            }
        }

        public static Stat createInstance(String metric, Map<String, String> tags, String value, long timestamp) {
            return new Stat(ImmutableMap.of(
                    STAT_KEYS.METRIC.getName(), metric,
                    STAT_KEYS.TAGS.getName(), tags,
                    STAT_KEYS.VALUE.getName(), value,
                    STAT_KEYS.TIMESTAMP.getName(), timestamp
            ));
        }

        public Stat(Map<String, Object> map) {
            super(map);
        }
    }

}
