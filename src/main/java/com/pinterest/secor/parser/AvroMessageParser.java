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
package com.pinterest.secor.parser;

import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.message.Message;
import io.confluent.kafka.serializers.KafkaAvroDecoder;
import kafka.utils.VerifiableProperties;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.errors.SerializationException;

import javax.xml.bind.DatatypeConverter;
import java.util.Properties;

/**
 * AvroMessageParser extracts timestamp field (specified by 'message.timestamp.name')
 * from Avro data after loading schema from registry and partitions data by date.
 */
public class AvroMessageParser extends TimestampedMessageParser {
    private final KafkaAvroDecoder avroDecoder;

    public AvroMessageParser(SecorConfig config) {
        super(config);
        Properties props = new Properties();
        props.put("schema.registry.url", config.getSchemaRegistryUrl());
        VerifiableProperties vProps = new VerifiableProperties(props);
        avroDecoder = new KafkaAvroDecoder(vProps);
    }

    @Override
    public long extractTimestampMillis(final Message message) {
        try {
            GenericRecord record = (GenericRecord) avroDecoder.fromBytes(message.getPayload());
            if (record != null) {
                Object fieldValue = record.get(mConfig.getMessageTimestampName());
                if (fieldValue != null) {
                    try {
                        return toMillis(Double.valueOf(fieldValue.toString()).longValue());
                    } catch (NumberFormatException nfe) {
                        try {
                            return toMillis(DatatypeConverter.parseDateTime(fieldValue.toString()).getTimeInMillis());
                        } catch (IllegalArgumentException exc) {
                            return 0;
                        }
                    }
                }
            }
        } catch (SerializationException exc) {
            return 0;
        }
        return 0;
    }

}
