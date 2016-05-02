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

import com.google.api.client.repackaged.com.google.common.base.Strings;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.StringUtils;

import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;

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

    public String getPartitionAssignmentStrategy() {
        return getString("kafka.partition.assignment.strategy");
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

    public String getSocketReceiveBufferBytes() {
        return getString("kafka.socket.receive.buffer.bytes");
    }

    public String getFetchMinBytes() {
        return getString("kafka.fetch.min.bytes");
    }

    public String getFetchWaitMaxMs() {
        return getString("kafka.fetch.wait.max.ms");
    }

    public String getDualCommitEnabled() {
        return getString("kafka.dual.commit.enabled");
    }

    public String getOffsetsStorage() {
        return getString("kafka.offsets.storage");
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

    public String getS3FileSystem() { return getString("secor.s3.filesystem"); }

    public boolean getSeparateContainersForTopics() {
    	return getString("secor.swift.containers.for.each.topic").toLowerCase().equals("true");
    }
    
    public String getSwiftContainer() {
        return getString("secor.swift.container");
    }

    public String getSwiftPath() {
        return getString("secor.swift.path");
    }
    
    public String getS3Bucket() {
        return getString("secor.s3.bucket");
    }

    public String getS3Path() {
        return getString("secor.s3.path");
    }

    public String getS3AlternativePath() {
        return getString("secor.s3.alternative.path");
    }

    public String getS3AlterPathDate() {
        return getString("secor.s3.alter.path.date");
    }

    public String getS3Prefix() {
        return getS3FileSystem() + "://" + getS3Bucket() + "/" + getS3Path();
    }

    public String getS3AlternativePrefix() {
        return getS3FileSystem() + "://" + getS3Bucket() + "/" + getS3AlternativePath();
    }

    public String getLocalPath() {
        return getString("secor.local.path");
    }

    public String getKafkaTopicFilter() {
        return getString("secor.kafka.topic_filter");
    }

    public String getKafkaTopicBlacklist() {
        return getString("secor.kafka.topic_blacklist");
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

    public String getUploadManagerClass() {
        return getString("secor.upload.manager.class");
    }

    public String getMessageTransformerClass(){
    	return getString("secor.message.transformer.class");
    }
 
    public int getTopicPartitionForgetSeconds() {
        return getInt("secor.topic_partition.forget.seconds");
    }

    public int getLocalLogDeleteAgeHours() {
        return getInt("secor.local.log.delete.age.hours");
    }

    public String getFileExtension() {
        return getString("secor.file.extension");
    }

    public int getOstrichPort() {
        return getInt("ostrich.port");
    }

    public String getCloudService() {
        return getString("cloud.service");
    }
    
    public String getAwsAccessKey() {
        return getString("aws.access.key");
    }

    public String getAwsSecretKey() {
        return getString("aws.secret.key");
    }

    public String getAwsEndpoint() {
        return getString("aws.endpoint");
    }

    public String getAwsRole() {
        return getString("aws.role");
    }
    
    public boolean getAwsProxyEnabled(){
    	return getBoolean("aws.proxy.isEnabled");
    }
    
    public String getAwsProxyHttpHost() {
        return getString("aws.proxy.http.host");
    }
    
    public int getAwsProxyHttpPort() {
        return getInt("aws.proxy.http.port");
    }

    public String getAwsRegion() {
        return getString("aws.region");
    }

    public String getAwsSseType() {
        return getString("aws.sse.type");
    }

    public String getAwsSseKmsKey() {
        return getString("aws.sse.kms.key");
    }

    public String getAwsSseCustomerKey() {
        return getString("aws.sse.customer.key");
    }

    public String getSwiftTenant() {
        return getString("swift.tenant");
    }
    
    public String getSwiftUsername() {
        return getString("swift.username");
    }
    
    public String getSwiftPassword() {
        return getString("swift.password");
    }    
    
    public String getSwiftAuthUrl() {
        return getString("swift.auth.url");
    }
    
    public String getSwiftPublic() {
    	return getString("swift.public");
    }
    
    public String getSwiftPort() {
    	return getString("swift.port");
    }
    
    public String getSwiftGetAuth() {
    	return getString("swift.use.get.auth");
    }
    
    public String getSwiftApiKey() {
    	return getString("swift.api.key");
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

    public String getMonitoringPrefix() {
        return getString("monitoring.prefix");
    }

    public String getMessageTimestampName() {
        return getString("message.timestamp.name");
    }
    
    public String getMessageTimestampNameSeparator() {
        return getString("message.timestamp.name.separator");
    }

    public int getMessageTimestampId() {
        return getInt("message.timestamp.id");
    }

    public String getMessageTimestampType() {
        return getString("message.timestamp.type");
    }

    public String getMessageTimestampInputPattern() {
        return getString("message.timestamp.input.pattern");
    }

    public boolean isMessageTimestampRequired() {
        return mProperties.getBoolean("message.timestamp.required");
    }

    public int getFinalizerLookbackPeriods() {
        return getInt("secor.finalizer.lookback.periods", 10);
    }

    public String getHivePrefix() { 
        return getString("secor.hive.prefix"); 
    }

    public String getHiveTableName(String topic) {
        String key = "secor.hive.table.name." + topic;
        return mProperties.getString(key, null);
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

    public String getZookeeperPath() {
        return getString("secor.zookeeper.path");
    }

    public String getGsCredentialsPath() {
        return getString("secor.gs.credentials.path");
    }

    public String getGsBucket() {
        return getString("secor.gs.bucket");
    }

    public String getGsPath() {
        return getString("secor.gs.path");
    }

    public int getGsConnectTimeoutInMs() {
        return getInt("secor.gs.connect.timeout.ms", 3 * 60000);
    }

    public int getGsReadTimeoutInMs() {
        return getInt("secor.gs.read.timeout.ms", 3 * 60000);
    }

    public boolean getGsDirectUpload() {
        return getBoolean("secor.gs.upload.direct");
    }

    public int getFinalizerDelaySeconds() {
        return getInt("partitioner.finalizer.delay.seconds");
    }

    public boolean getS3MD5HashPrefix() {
      return getBoolean("secor.s3.prefix.md5hash");
    }

    public String getAzureEndpointsProtocol() { return getString("secor.azure.endpoints.protocol"); }

    public String getAzureAccountName() { return getString("secor.azure.account.name"); }

    public String getAzureAccountKey() { return getString("secor.azure.account.key"); }

    public String getAzureContainer() { return getString("secor.azure.container.name"); }

    public String getAzurePath() { return getString("secor.azure.path"); }

    public TimeZone getTimeZone() {
        String timezone = getString("secor.parser.timezone");
        return Strings.isNullOrEmpty(timezone) ? TimeZone.getTimeZone("UTC") : TimeZone.getTimeZone(timezone);
    }

    public boolean getBoolean(String name, boolean defaultValue) {
        return mProperties.getBoolean(name, defaultValue);
    }
    
    public boolean getBoolean(String name) {
        return mProperties.getBoolean(name);
    }

    public void checkProperty(String name) {
        if (!mProperties.containsKey(name)) {
            throw new RuntimeException("Failed to find required configuration option '" +
                                       name + "'.");
        }
    }

    public String getString(String name) {
        checkProperty(name);
        return mProperties.getString(name);
    }

    public int getInt(String name) {
        checkProperty(name);
        return mProperties.getInt(name);
    }

    public int getInt(String name, int defaultValue) {
        return mProperties.getInt(name, defaultValue);
    }

    public long getLong(String name) {
        return mProperties.getLong(name);
    }

    public String[] getStringArray(String name) {
        return mProperties.getStringArray(name);
    }
}
