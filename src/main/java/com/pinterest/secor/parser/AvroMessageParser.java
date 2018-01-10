package com.pinterest.secor.parser;

import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.message.Message;
import com.pinterest.secor.common.SecorSchemaRegistryClient;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * AvroMessageParser extracts timestamp field (specified by 'message.timestamp.name')
 * from AVRO data and partitions data by date.
 */
public class AvroMessageParser extends TimestampedMessageParser {
    private static final Logger LOG = LoggerFactory.getLogger(AvroMessageParser.class);

    private final boolean m_timestampRequired;
    protected final SecorSchemaRegistryClient schemaRegistryClient;

    public AvroMessageParser(SecorConfig config) {
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
                    return toMillis(Double.valueOf(fieldValue.toString()).longValue());
                }
            } else if (m_timestampRequired) {
                throw new RuntimeException("Missing timestamp field for message: " + message);
            }
        } catch (SerializationException e) {
            LOG.error("Failed to parse record", e);
        }
        return 0;
    }

}
