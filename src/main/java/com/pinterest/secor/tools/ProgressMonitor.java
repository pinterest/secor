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
import com.pinterest.secor.message.Message;
import com.pinterest.secor.parser.ThriftMessageParser;
import com.pinterest.secor.util.StatsUtil;
import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Progress monitor exports offset lags par topic partition.
 *
 * @author Pawel Garbacki (pawel@pinterest.com)
 */
public class ProgressMonitor {
    private static final Logger LOG = LoggerFactory.getLogger(ProgressMonitor.class);
    private SecorConfig mConfig;
    private ZookeeperConnector mZookeeperConnector;
    private KafkaClient mKafkaClient;
    private ThriftMessageParser mThriftMessageParser;

    public ProgressMonitor(SecorConfig config) {
        mConfig = config;
        mZookeeperConnector = new ZookeeperConnector(mConfig);
        mKafkaClient = new KafkaClient(mConfig);
        mThriftMessageParser = new ThriftMessageParser(mConfig);
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
        List<String> topics = mZookeeperConnector.getCommittedOffsetTopics();
        for (String topic : topics) {
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
                    committedTimestampMillis = mThriftMessageParser.extractTimestampMillis(
                        committedMessage);
                }

                Message lastMessage = mKafkaClient.getLastMessage(topicPartition);
                if (lastMessage == null) {
                    LOG.warn("no message found in topic " + topic + " partition " + partition);
                } else {
                    long lastOffset = lastMessage.getOffset();
                    long lastTimestampMillis = mThriftMessageParser.extractTimestampMillis(
                        lastMessage);
                    assert committedOffset <= lastOffset: Long.toString(committedOffset) + " <= " +
                        lastOffset;
                    long offsetLag = lastOffset - committedOffset;
                    long timestampMillisLag = lastTimestampMillis - committedTimestampMillis;
                    HashMap<String, String> tags = new HashMap<String, String>();
                    tags.put("topic", topic);
                    tags.put("partition", Integer.toString(partition));
                    exportToTsdb("secor.lag.offsets", tags, Long.toString(offsetLag));
                    exportToTsdb("secor.lag.seconds", tags,
                        Long.toString(timestampMillisLag / 1000));
                    LOG.debug("topic " + topic + " partition " + partition + " committed offset " +
                        committedOffset + " last offset " + lastOffset + " committed timestamp " +
                            (committedTimestampMillis / 1000) + " last timestamp " +
                            (lastTimestampMillis / 1000));
                }
            }
        }
    }
}
