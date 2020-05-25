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

import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.common.SecorSchemaRegistryClient;
import com.pinterest.secor.message.Message;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.kafka.common.errors.SerializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;

/**
 * AvroMessageParser extracts timestamp field (specified by 'message.timestamp.name')
 * and a custom field specified by ''message.split.field.name'.
 * from AVRO data and partitions data by date and custom field.
 *
 * By default the first partition will be the fieldValue=
 *
 * If you want to set it to something else set 'partitioner.granularity.field.prefix'
 *
 * This class was heavily based off SplitByFieldMessageParser (which supports JSON). Like
 * that other parser this parser doesn't support finalization of partitions.
 */
public class AvroSplitByFieldMessageParser extends TimestampedMessageParser implements Partitioner {
    private static final Logger LOG = LoggerFactory.getLogger(AvroSplitByFieldMessageParser.class);
    private final String mSplitFieldName;
    private final boolean m_timestampRequired;
    private final String mFieldPrefix;

    protected final SecorSchemaRegistryClient schemaRegistryClient;

    public AvroSplitByFieldMessageParser(SecorConfig config) {
        super(config);
        schemaRegistryClient = new SecorSchemaRegistryClient(config);
        m_timestampRequired = config.isMessageTimestampRequired();
        mSplitFieldName = config.getMessageSplitFieldName();
        mFieldPrefix = usingFieldPrefix(config);

    }

    static String usingFieldPrefix(SecorConfig config) {
        return config.getString("partitioner.granularity.field.prefix", config.getMessageSplitFieldName() + "=");
    }

    @Override
    public long extractTimestampMillis(Message message) throws Exception {
        throw new UnsupportedOperationException("Unsupported, use extractPartitions method instead");
    }

    @Override
    public String[] extractPartitions(Message message) throws Exception {
        GenericRecord record = schemaRegistryClient.deserialize(message.getTopic(), message.getPayload());
        if (record == null) {
            throw new RuntimeException("Failed to parse message as Avro object");
        }

        String eventType = extractEventType(record);
        long timestampMillis = extractTimestampMillis(record);

        String[] timestampPartitions = generatePartitions(timestampMillis, mUsingHourly, mUsingMinutely);
        return (String[]) ArrayUtils.addAll(new String[]{mFieldPrefix + eventType}, timestampPartitions);
    }

    @Override
    public String[] getFinalizedUptoPartitions(List<Message> lastMessages,
                                               List<Message> committedMessages) throws Exception {
        throw new UnsupportedOperationException("Partition finalization is not supported");
    }

    @Override
    public String[] getPreviousPartitions(String[] partitions) throws Exception {
        throw new UnsupportedOperationException("Partition finalization is not supported");
    }

    protected String extractEventType(GenericRecord record) {
        Object fieldValue = record.get(mSplitFieldName);
        if (fieldValue == null) {
            throw new RuntimeException("Could not find key " + mSplitFieldName + " in Avro message");
        }
        return fieldValue.toString();
    }

    protected long extractTimestampMillis(GenericRecord record) {
        try {
            if (record != null) {
                Object fieldValue = record.get(mConfig.getMessageTimestampName());
                if (fieldValue != null) {
                    return toMillis(Double.valueOf(fieldValue.toString()).longValue());
                }
            } else if (m_timestampRequired) {
                throw new RuntimeException("Missing timestamp field for message: " + record.toString());
            }
        } catch (SerializationException e) {
            LOG.error("Failed to parse record", e);
        }
        return 0;
    }

}
