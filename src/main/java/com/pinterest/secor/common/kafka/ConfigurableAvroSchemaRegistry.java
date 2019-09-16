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
package com.pinterest.secor.common.kafka;

import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.common.kafka.AvroSchemaRegistry;
import com.pinterest.secor.parser.AvroMessageParser;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.common.errors.SerializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class ConfigurableAvroSchemaRegistry implements AvroSchemaRegistry {
    private static final Logger LOG = LoggerFactory.getLogger(AvroMessageParser.class);
    private final Map<String, Schema> schemas = new HashMap<>();
    private final Map<String, SpecificDatumReader<GenericRecord>> readers = new HashMap<>();

    public ConfigurableAvroSchemaRegistry(SecorConfig config) {
        schemas.putAll(config.getAvroMessageSchema().entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> getAvroSchema(e.getValue()))));
        readers.putAll(schemas.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> new SpecificDatumReader<>(e.getValue()))));
    }

    private Schema getAvroSchema(String path) {
        Schema schema = null;
        try {
            schema = new Schema.Parser().parse(getClass().getResourceAsStream(path));
        } catch (Exception ex) {
            LOG.error("Exception getting schema for file " + path, ex);
        }
        return schema;
    }

    public GenericRecord deserialize(String topic, byte[] payload) {
        GenericRecord record = null;
        try {
            Decoder decoder = DecoderFactory.get().binaryDecoder(payload, null);
            record = readers.get(topic).read(null, decoder);
        } catch (IOException ioe) {
            throw new SerializationException("Error deserializing Avro message");
        }
        return record;
    }

    public Schema getSchema(String topic) {
        return schemas.get(topic);
    }
}
