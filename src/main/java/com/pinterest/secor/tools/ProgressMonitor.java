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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.pinterest.secor.common.KafkaClient;
import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.common.TopicPartition;
import com.pinterest.secor.common.ZookeeperConnector;
import com.pinterest.secor.message.Message;
import com.pinterest.secor.parser.MessageParser;
import com.pinterest.secor.parser.TimestampedMessageParser;
import com.pinterest.secor.util.ReflectionUtil;
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
    private SecorConfig mConfig;
    private ZookeeperConnector mZookeeperConnector;
    private KafkaClient mKafkaClient;
    private MessageParser mMessageParser;

    public ProgressMonitor(SecorConfig config)
            throws Exception
    {
        mConfig = config;
        mZookeeperConnector = new ZookeeperConnector(mConfig);
        mKafkaClient = new KafkaClient(mConfig);
        mMessageParser = (MessageParser) ReflectionUtil.createMessageParser(
                mConfig.getMessageParserClass(), mConfig);
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

    private void exportToTsdb(String metric, Map<String, String> tags, String value)
            throws IOException {
        JSONObject bodyJson = new JSONObject();
        bodyJson.put("metric", metric);
        bodyJson.put("timestamp", System.currentTimeMillis() / 1000);
        bodyJson.put("value", value);
        JSONObject tagsJson = new JSONObject();
        for (Map.Entry<String, String> entry : tags.entrySet()) {
            tagsJson.put(entry.getKey(), entry.getValue());
        }
        bodyJson.put("tags", tagsJson);
        LOG.info("exporting metric to tsdb " + bodyJson);
        makeRequest(bodyJson.toString());
    }

    public void exportStats() throws Exception {
        List<Stat> stats = getStats();
        if (mConfig.getProgressMonitorAsJson()) {
            System.out.println(JSONArray.toJSONString(stats));
        } else {
            for (Stat stat : stats) {
                exportToTsdb(stat.getMetric(), stat.getTags(), stat.getValue());
            }
        }
    }

    private List<Stat> getStats() throws Exception {
        List<String> topics = mZookeeperConnector.getCommittedOffsetTopics();
        List<Stat> stats = Lists.newArrayList();

        for (String topic : topics) {
            if (topic.matches(mConfig.getTsdbBlacklistTopics()) ||
                    !topic.matches(mConfig.getKafkaTopicFilter())) {
                LOG.info("skipping topic " + topic);
                continue;
            }
            List<Integer> partitions = mZookeeperConnector.getCommittedOffsetPartitions(topic);
            for (Integer partition : partitions) {
                TopicPartition topicPartition = new TopicPartition(topic, partition);
                Message committedMessage = mKafkaClient.getCommittedMessage(topicPartition);
                long committedOffset = - 1;
                long committedTimestampMillis = -1;
                if (committedMessage == null) {
                    LOG.warn("no committed message found in topic " + topic + " partition " +
                        partition);
                } else {
                    committedOffset = committedMessage.getOffset();
                    committedTimestampMillis = getTimestamp(committedMessage);
                }

                Message lastMessage = mKafkaClient.getLastMessage(topicPartition);
                if (lastMessage == null) {
                    LOG.warn("no message found in topic " + topic + " partition " + partition);
                } else {
                    long lastOffset = lastMessage.getOffset();
                    long lastTimestampMillis = getTimestamp(lastMessage);
                    assert committedOffset <= lastOffset: Long.toString(committedOffset) + " <= " +
                        lastOffset;

                    long offsetLag = lastOffset - committedOffset;
                    long timestampMillisLag = lastTimestampMillis - committedTimestampMillis;
                    Map<String, String> tags = ImmutableMap.of(
                            "topic", topic,
                            "partition", Integer.toString(partition)
                    );

                    stats.add(new Stat("secor.lag.offsets", tags, Long.toString(offsetLag)));
                    stats.add(new Stat("secor.lag.seconds", tags, Long.toString(timestampMillisLag / 1000)));

                    LOG.debug("topic " + topic + " partition " + partition + " committed offset " +
                        committedOffset + " last offset " + lastOffset + " committed timestamp " +
                            (committedTimestampMillis / 1000) + " last timestamp " +
                            (lastTimestampMillis / 1000));
                }
            }
        }

        return stats;
    }

    private long getTimestamp(Message message) throws Exception {
        if (mMessageParser instanceof TimestampedMessageParser) {
            return ((TimestampedMessageParser)mMessageParser).extractTimestampMillis(message);
        } else {
            return -1;
        }
    }

    private static class Stat {
        final String metric;
        final Map<String, String> tags;
        final String value;
        final long timeStamp;

        public Stat(String metric, Map<String, String> tags, String value)
        {
            this.metric = metric;
            this.tags = tags;
            this.value = value;
            this.timeStamp = System.currentTimeMillis() / 1000;
        }

        public String getMetric() {
            return this.metric;
        }

        public Map<String, String> getTags() {
            return this.tags;
        }

        public String getValue() {
            return this.value;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (!(o instanceof Stat)) {
                return false;
            }

            Stat stat = (Stat) o;

            if (metric != null ? !metric.equals(stat.metric) : stat.metric != null) {
                return false;
            }
            if (tags != null ? !tags.equals(stat.tags) : stat.tags != null) {
                return false;
            }
            if (value != null ? !value.equals(stat.value) : stat.value != null) {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode()
        {
            int result = metric != null ? metric.hashCode() : 0;
            result = 31 * result + (tags != null ? tags.hashCode() : 0);
            result = 31 * result + (value != null ? value.hashCode() : 0);
            return result;
        }

        public String toString() {
            JSONObject bodyJson = new JSONObject();
            bodyJson.put("metric", metric);
            bodyJson.put("timestamp", timeStamp);
            bodyJson.put("value", value);
            JSONObject tagsJson = new JSONObject();
            for (Map.Entry<String, String> entry : tags.entrySet()) {
                tagsJson.put(entry.getKey(), entry.getValue());
            }
            bodyJson.put("tags", tagsJson);

            LOG.info("exporting metric to tsdb " + bodyJson);
            return bodyJson.toString();
        }
    }
}
