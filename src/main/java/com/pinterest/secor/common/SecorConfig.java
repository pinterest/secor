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

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.StringUtils;

import java.util.Map;
import java.util.Properties;

/**
 * One-stop shop for Secor configuration options.
 *
 * @author Pawel Garbacki (pawel@pinterest.com)
 */
public class SecorConfig {
    private final PropertiesConfiguration mProperties;

    private static final ThreadLocal<SecorConfig> mSecorConfig = new ThreadLocal<SecorConfig>() {

        @Override
        protected SecorConfig initialValue() {
            // Load the default configuration file first
            Properties systemProperties = System.getProperties();
            String configProperty = systemProperties.getProperty("config");

            PropertiesConfiguration properties;
            try {
                properties = new PropertiesConfiguration(configProperty);
            } catch (ConfigurationException e) {
                throw new RuntimeException("Error loading configuration from " + configProperty);
            }

            for (final Map.Entry<Object, Object> entry : systemProperties.entrySet()) {
                properties.setProperty(entry.getKey().toString(), entry.getValue());
            }

            return new SecorConfig(properties);
        }
    };

    public static SecorConfig load() throws ConfigurationException {
        return mSecorConfig.get();
    }

    /**
     * Exposed for testability
     * 
     * @param properties
     */
    public SecorConfig(PropertiesConfiguration properties) {
        mProperties = properties;
    }

    public String getKafkaSeedBrokerHost() {
        return getString("kafka.seed.broker.host");
    }

    public int getKafkaSeedBrokerPort() {
        return getInt("kafka.seed.broker.port");
    }

    public String getKafkaZookeeperPath() {
        return getString("kafka.zookeeper.path");
    }

    public String getZookeeperQuorum() {
        return StringUtils.join(getStringArray("zookeeper.quorum"), ',');
    }

    public int getConsumerTimeoutMs() {
        return getInt("kafka.consumer.timeout.ms");
    }

    public String getRebalanceMaxRetries() {
        return getString("kafka.rebalance.max.retries");
    }

    public String getRebalanceBackoffMs() {
        return getString("kafka.rebalance.backoff.ms");
    }

    public String getFetchMessageMaxBytes() {
        return getString("kafka.fetch.message.max.bytes");
    }

    public String getSocketReceieveBufferBytes() {
        return getString("kafka.socket.receive.buffer.bytes");
    }

    public String getFetchMinBytes() {
        return getString("kafka.fetch.min.bytes");
    }

    public String getFetchWaitMaxMs() {
        return getString("kafka.fetch.wait.max.ms");
    }

    public int getGeneration() {
        return getInt("secor.generation");
    }

    public int getConsumerThreads() {
        return getInt("secor.consumer.threads");
    }

    public long getMaxFileSizeBytes() {
        return getLong("secor.max.file.size.bytes");
    }

    public long getMaxFileAgeSeconds() {
        return getLong("secor.max.file.age.seconds");
    }

    public long getOffsetsPerPartition() {
        return getLong("secor.offsets.per.partition");
    }

    public int getMessagesPerSecond() {
        return getInt("secor.messages.per.second");
    }

    public String getS3Bucket() {
        return getString("secor.s3.bucket");
    }

    public String getS3Path() {
        return getString("secor.s3.path");
    }

    public String getLocalPath() {
        return getString("secor.local.path");
    }

    public String getKafkaTopicFilter() {
        return getString("secor.kafka.topic_filter");
    }

    public String getKafkaGroup() {
        return getString("secor.kafka.group");
    }

    public int getZookeeperSessionTimeoutMs() {
        return getInt("zookeeper.session.timeout.ms");
    }

    public int getZookeeperSyncTimeMs() {
        return getInt("zookeeper.sync.time.ms");
    }

    public String getMessageParserClass() {
        return getString("secor.message.parser.class");
    }

    public int getTopicPartitionForgetSeconds() {
        return getInt("secor.topic_partition.forget.seconds");
    }

    public int getLocalLogDeleteAgeHours() {
        return getInt("secor.local.log.delete.age.hours");
    }

    public int getOstrichPort() {
        return getInt("ostrich.port");
    }

    public String getAwsAccessKey() {
        return getString("aws.access.key");
    }

    public String getAwsSecretKey() {
        return getString("aws.secret.key");
    }

    public String getQuboleApiToken() {
        return getString("qubole.api.token");
    }

    public String getTsdbHostport() {
        return getString("tsdb.hostport");
    }

    public String getStatsDHostPort() {
        return getString("statsd.hostport");
    }

    public String getMonitoringBlacklistTopics() {
        return getString("monitoring.blacklist.topics");
    }

    public String getMessageTimestampName() {
        return getString("message.timestamp.name");
    }

    public String getMessageTimestampInputPattern() {
        return getString("message.timestamp.input.pattern");
    }

    public String getHivePrefix() { 
        return getString("secor.hive.prefix"); 
    }

    public String getCompressionCodec() {
        return getString("secor.compression.codec");
    }

    public int getMaxMessageSizeBytes() {
        return getInt("secor.max.message.size.bytes");
    }
    
    public String getFileReaderWriterFactory() {
    	return getString("secor.file.reader.writer.factory");
    }
    
    public String getPerfTestTopicPrefix() {
    	return getString("secor.kafka.perf_topic_prefix");
    }

    private void checkProperty(String name) {
        if (!mProperties.containsKey(name)) {
            throw new RuntimeException("Failed to find required configuration option '" +
                                       name + "'.");
        }
    }

    private String getString(String name) {
        checkProperty(name);
        return mProperties.getString(name);
    }

    private int getInt(String name) {
        checkProperty(name);
        return mProperties.getInt(name);
    }

    private long getLong(String name) {
        return mProperties.getLong(name);
    }

    private String[] getStringArray(String name) {
        return mProperties.getStringArray(name);
    }
}
