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
package com.pinterest.secor.common;

import com.pinterest.secor.protobuf.Messages.UnitTestMessage1;
import com.pinterest.secor.protobuf.Messages.UnitTestMessage2;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.Test;

import java.net.URL;
import java.util.HashMap;
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
    public void testAvroSpecificClassesAreParsedProperly() throws ConfigurationException {
        HashMap<String, String> expectedTopicsAndClasses = new HashMap<>();
        expectedTopicsAndClasses.put("test_topic_rac", "ai.humn.telematics.avro.dto.RacStandardExtendedDTO");
        expectedTopicsAndClasses.put("test_topic_obd", "ai.humn.telematics.avro.dto.ObdDataDTO");
        URL configFile = Thread.currentThread().getContextClassLoader().getResource("secor.humn.test.properties");
        PropertiesConfiguration properties = new PropertiesConfiguration(configFile);

        SecorConfig secorConfig = new SecorConfig(properties);
        Map<String, String> messageClassNamePerTopic = secorConfig.getAvroSpecificClassesSchemas();

        assertEquals(expectedTopicsAndClasses, messageClassNamePerTopic);
    }

    @Test
    public void shouldReadMetricCollectorConfiguration() throws ConfigurationException {

        URL configFile = Thread.currentThread().getContextClassLoader().getResource("secor.test.monitoring.properties");
        PropertiesConfiguration properties = new PropertiesConfiguration(configFile);

        SecorConfig secorConfig = new SecorConfig(properties);

        assertEquals("com.pinterest.secor.monitoring.OstrichMetricCollector", secorConfig.getMetricsCollectorClass());
    }
}
