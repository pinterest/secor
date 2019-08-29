package com.pinterest.secor.parser;

import ai.humn.avro.extraction.InformationExtractor;
import com.pinterest.secor.common.ClasspathConfigurableAvroSchemaRegistry;
import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.message.Message;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClasspathAvroMessageParser extends TimestampedMessageParser {
    private static final Logger LOG = LoggerFactory.getLogger(AvroMessageParser.class);

    private final boolean m_timestampRequired;
    private final ClasspathConfigurableAvroSchemaRegistry schemaRegistry;

    public ClasspathAvroMessageParser(SecorConfig config) {
        super(config);
        schemaRegistry = new ClasspathConfigurableAvroSchemaRegistry(config);
        m_timestampRequired = config.isMessageTimestampRequired();
    }

    private long extractTimestampFromRecord(IndexedRecord record) {
        return InformationExtractor.getEventTimestamp(record);
    }

    @Override
    public long extractTimestampMillis(Message message) {
        try {
            GenericRecord record = schemaRegistry.deserialize(message.getTopic(), message.getPayload());
            return extractTimestampFromRecord(record);
        } catch (Exception e) {
            if (m_timestampRequired) {
                throw new RuntimeException("Missing timestamp field for message: " + message);
            } else {
                LOG.error("Failed to parse record", e);
            }
        }
        return 0L;
    }
}
