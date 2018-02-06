package com.pinterest.secor.parser;

import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.common.SecorSchemaRegistryClient;
import com.pinterest.secor.message.Message;

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
    protected final SecorSchemaRegistryClient schemaRegistryClient;

    public AvroIso8601MessageParser(SecorConfig config) {
        super(config);
        schemaRegistryClient = new SecorSchemaRegistryClient(config);
        m_timestampRequired = config.isMessageTimestampRequired();
    }

    @Override
    public long extractTimestampMillis(final Message message) {
        try {
            GenericRecord record = schemaRegistryClient.decodeMessage(message.getTopic(), message.getPayload());
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
