package com.pinterest.secor.common;

import com.pinterest.secor.protobuf.Messages.UnitTestMessage1;
import com.pinterest.secor.protobuf.Messages.UnitTestMessage2;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.Test;

import java.net.URL;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class SecorConfigTest {

    @Test
    public void config_should_read_migration_required_properties_default_values() throws ConfigurationException {

        URL configFile = Thread.currentThread().getContextClassLoader().getResource("secor.common.properties");
        PropertiesConfiguration properties = new PropertiesConfiguration(configFile);

        SecorConfig secorConfig = new SecorConfig(properties);
        assertEquals("true", secorConfig.getDualCommitEnabled());
        assertEquals("zookeeper", secorConfig.getOffsetsStorage());
    }

    @Test
    public void config_should_read_migration_required() throws ConfigurationException {

        URL configFile = Thread.currentThread().getContextClassLoader().getResource("secor.kafka.migration.test.properties");
        PropertiesConfiguration properties = new PropertiesConfiguration(configFile);

        SecorConfig secorConfig = new SecorConfig(properties);
        assertEquals("false", secorConfig.getDualCommitEnabled());
        assertEquals("kafka", secorConfig.getOffsetsStorage());
    }

    @Test
    public void testProtobufMessageClassPerTopic() throws ConfigurationException {

        URL configFile = Thread.currentThread().getContextClassLoader().getResource("secor.test.protobuf.properties");
        PropertiesConfiguration properties = new PropertiesConfiguration(configFile);

        SecorConfig secorConfig = new SecorConfig(properties);
        Map<String, String> messageClassPerTopic = secorConfig.getProtobufMessageClassPerTopic();
        
        assertEquals(2, messageClassPerTopic.size());
        assertEquals(UnitTestMessage1.class.getName(), messageClassPerTopic.get("mytopic1"));
        assertEquals(UnitTestMessage2.class.getName(), messageClassPerTopic.get("mytopic2"));
    }

    @Test
    public void shouldReadMetricCollectorConfiguration() throws ConfigurationException {

        URL configFile = Thread.currentThread().getContextClassLoader().getResource("secor.test.monitoring.properties");
        PropertiesConfiguration properties = new PropertiesConfiguration(configFile);

        SecorConfig secorConfig = new SecorConfig(properties);

        assertEquals("com.pinterest.secor.monitoring.OstrichMetricCollector", secorConfig.getMetricsCollectorClass());
    }
}
