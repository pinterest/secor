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

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public class SecorSchemaRegistryClient implements AvroSchemaRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(SecorSchemaRegistryClient.class);
    private static final Map<String, Schema> schemas = new ConcurrentHashMap<>();

    private final String schemaRegistryNameSuffix;
    protected SchemaRegistryClient schemaRegistryClient;
    protected KafkaAvroDeserializer deserializer;
    protected KafkaAvroSerializer serializer;

    public SecorSchemaRegistryClient(SecorConfig config) {
        try {
            Properties props = new Properties();
            props.put("schema.registry.url", config.getSchemaRegistryUrl());
            schemaRegistryClient = new CachedSchemaRegistryClient(config.getSchemaRegistryUrl(), 30);
            schemaRegistryNameSuffix = config.getSchemaRegistryNameSuffix();
            init(config);
        } catch (Exception e) {
            LOG.error("Error initalizing schema registry", e);
            throw new RuntimeException(e);
        }
    }

    //Allows the SchemaRegistryClient to be mocked in unit tests
    protected void init(SecorConfig config) {
        deserializer = new KafkaAvroDeserializer(schemaRegistryClient);
        serializer = new KafkaAvroSerializer(schemaRegistryClient);
    }

    public GenericRecord deserialize(String topic, byte[] message) {
        if (message.length == 0) {
            message = null;
        }
        GenericRecord record = (GenericRecord) deserializer.deserialize(topic, message);
        if (record != null) {
            Schema schema = record.getSchema();
            schemas.put(topic, schema);
        }
        return record;
    }

    /**
     * Get Avro schema of a topic. It uses the cache that either is set by calling {@link #deserialize(String, byte[])}
     * or querying this method to avoid hitting Schema Registry for each call.
     * It uses standard "subject name" strategy and it is topic_name-value.
     *
     * @param topic a Kafka topic to query the schema for
     * @return Schema object for the topic
     * @throws IllegalStateException if there is no schema registered for this topic or it is not able to fetch it
     */
    public Schema getSchema(String topic) {
        Schema schema = schemas.get(topic);
        if (schema == null) {
            try {
                SchemaMetadata schemaMetadata = schemaRegistryClient.getLatestSchemaMetadata(topic + schemaRegistryNameSuffix);
                schema = schemaRegistryClient.getByID(schemaMetadata.getId());
                schemas.put(topic, schema);
            } catch (IOException e) {
                throw new IllegalStateException("Unable to get Avro schema not found for topic " + topic);
            } catch (RestClientException e) {
                throw new IllegalStateException("Avro schema not found for topic " + topic);
            }
        }
        return schema;
    }

    @Override
    public byte[] serialize(String topic, GenericRecord record) throws IOException {
        return serializer.serialize(topic, record);

    }
}
