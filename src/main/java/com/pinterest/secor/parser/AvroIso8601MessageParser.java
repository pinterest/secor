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
package com.pinterest.secor.parser;

import com.pinterest.secor.common.kafka.AvroSchemaRegistry;
import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.message.Message;
import com.pinterest.secor.util.AvroSchemaRegistryFactory;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.DatatypeConverter;
import java.util.Date;

/**
 * AvroMessageParser extracts timestamp field (specified by 'message.timestamp.name')
 * from AVRO data and partitions data by date.
 * with support for ISO8601 date format
 */
public class AvroIso8601MessageParser extends TimestampedMessageParser {
    private static final Logger LOG = LoggerFactory.getLogger(AvroMessageParser.class);

    private final boolean m_timestampRequired;
    private final AvroSchemaRegistry schemaRegistry;

    public AvroIso8601MessageParser(SecorConfig config) {
        super(config);
        schemaRegistry = AvroSchemaRegistryFactory.getSchemaRegistry(config);
        m_timestampRequired = config.isMessageTimestampRequired();
    }

    @Override
    public long extractTimestampMillis(final Message message) {
        try {
            GenericRecord record = schemaRegistry.deserialize(message.getTopic(), message.getPayload());
            if (record != null) {
                Object fieldValue = record.get(mConfig.getMessageTimestampName());
                if (fieldValue != null) {
                    Date dateFormat = DatatypeConverter.parseDateTime(fieldValue.toString()).getTime();
                    return dateFormat.getTime();
                } else if (m_timestampRequired) {
                    throw new RuntimeException("Missing timestamp field for message: " + message);
                }
            } else {
                throw new RuntimeException("Record is empty: " + message);
            }
        } catch (SerializationException e) {
            LOG.error("Failed to parse record", e);
        }
        return 0;
    }

}
