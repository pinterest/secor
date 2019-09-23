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

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import junit.framework.TestCase;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.lang3.StringUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;

import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
public class SecorSchemaRegistryClientTest extends TestCase {

    private KafkaAvroDeserializer kafkaAvroDeserializer;
    private SchemaRegistryClient schemaRegistryClient;
    private SecorSchemaRegistryClient secorSchemaRegistryClient;
    private KafkaAvroSerializer avroSerializer;

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Override
    public void setUp() {
        initKafka();
        SecorConfig secorConfig = Mockito.mock(SecorConfig.class);
        when(secorConfig.getSchemaRegistryUrl()).thenReturn("schema-registry-url");
        secorSchemaRegistryClient = new SecorSchemaRegistryClient(secorConfig);
        secorSchemaRegistryClient.deserializer = kafkaAvroDeserializer;
        secorSchemaRegistryClient.schemaRegistryClient = schemaRegistryClient;
    }

    private void initKafka() {
        schemaRegistryClient = new MockSchemaRegistryClient();
        kafkaAvroDeserializer = new KafkaAvroDeserializer(schemaRegistryClient);
        avroSerializer = new KafkaAvroSerializer(schemaRegistryClient);
    }

    @Test
    public void testDecodeMessage() {
        Schema schemaV1 = SchemaBuilder.record("Foo")
                .fields()
                .name("data_field_1").type().intType().noDefault()
                .name("timestamp").type().longType().noDefault()
                .endRecord();
        //backward compatible schema change
        Schema schemaV2 = SchemaBuilder.record("Foo")
                .fields()
                .name("data_field_1").type().intType().noDefault()
                .name("data_field_2").type().stringType().noDefault()
                .name("timestamp").type().longType().noDefault()
                .endRecord();
        GenericRecord record1 = new GenericRecordBuilder(schemaV1)
                .set("data_field_1", 1)
                .set("timestamp", 1467176315L)
                .build();
        GenericRecord record2 = new GenericRecordBuilder(schemaV2)
                .set("data_field_1", 1)
                .set("data_field_2", "hello")
                .set("timestamp", 1467176316L)
                .build();
        GenericRecord output = secorSchemaRegistryClient.deserialize("test-avr-topic", avroSerializer.serialize("test-avr-topic", record1));
        assertEquals(secorSchemaRegistryClient.getSchema("test-avr-topic"), schemaV1);
        assertEquals(output.get("data_field_1"), 1);
        assertEquals(output.get("timestamp"), 1467176315L);

        output = secorSchemaRegistryClient.deserialize("test-avr-topic", avroSerializer.serialize("test-avr-topic", record2));
        assertEquals(secorSchemaRegistryClient.getSchema("test-avr-topic"), schemaV2);
        assertEquals(output.get("data_field_1"), 1);
        assertTrue(StringUtils.equals((output.get("data_field_2")).toString(), "hello"));
        assertEquals(output.get("timestamp"), 1467176316L);

        output = secorSchemaRegistryClient.deserialize("test-avr-topic", new byte[0]);
        assertNull(output);
    }

    @Test
    public void testGetSchema() throws IOException, RestClientException {
        Schema expectedSchema = SchemaBuilder.record("Foo")
                .fields()
                .name("data_field_1").type().intType().noDefault()
                .name("timestamp").type().longType().noDefault()
                .endRecord();
        schemaRegistryClient.register("test-avr-topic-2-value", expectedSchema);
        Schema schema = secorSchemaRegistryClient.getSchema("test-avr-topic-2");
        assertEquals(expectedSchema, schema);
    }

    @Test
    public void testGetSchemaDoesNotExist() {
        exception.expect(IllegalStateException.class);
        exception.expectMessage("Avro schema not found for topic test-avr-topic-3");
        secorSchemaRegistryClient.getSchema("test-avr-topic-3");
    }
}