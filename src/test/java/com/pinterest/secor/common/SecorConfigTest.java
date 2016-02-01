package com.pinterest.secor.common;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.Test;
import java.net.URL;
import static org.junit.Assert.*;

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


}
