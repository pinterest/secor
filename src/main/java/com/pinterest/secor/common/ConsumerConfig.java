package com.pinterest.secor.common;

import java.util.Properties;

public class ConsumerConfig {
    private Properties properties;

    public ConsumerConfig(String bootstrapServers, String consumerGroup) {
        properties = new Properties();
        properties.put("group.id", consumerGroup);
        createConfig(bootstrapServers);
    }

    public ConsumerConfig(String bootstrapServers) {
        properties = new Properties();
        createConfig(bootstrapServers);
    }

    private void createConfig(String bootstrapServers) {
        properties.put("bootstrap.servers", bootstrapServers);
        properties.put("max.poll.records", 1);
        properties.put("enable.auto.commit", false);
        properties.put("session.timeout.ms", "100000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.BytesDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.BytesDeserializer");
    }

    public Properties getProperties() {
        return properties;
    }
}
