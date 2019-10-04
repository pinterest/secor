package com.pinterest.secor.common;

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

public class ClasspathConfigurableAvroSchemaRegistry implements AvroSchemaRegistry {
    private static final Logger LOG = LoggerFactory.getLogger(ClasspathConfigurableAvroSchemaRegistry.class);

    private final Map<String, Schema> schemas = new HashMap<>();
    private final Map<String, SpecificDatumReader<GenericRecord>> readers = new HashMap<>();

    public ClasspathConfigurableAvroSchemaRegistry(SecorConfig config) {
        schemas.putAll(
                config.getAvroSpecificClassesSchemas()
                        .entrySet()
                        .stream()
                        .collect(Collectors.toMap(Map.Entry::getKey, e -> getSchemaForSpecificClass(e.getValue())))
        );
        readers.putAll(
                schemas.entrySet()
                        .stream()
                        .collect(Collectors.toMap(Map.Entry::getKey, e -> new SpecificDatumReader<>(e.getValue())))
        );

        LOG.debug("Configured schemas: " + schemas.toString());
    }

    @Override
    public GenericRecord deserialize(String topic, byte[] payload) {
        try {
            Decoder decoder = DecoderFactory.get().binaryDecoder(payload, null);
            return readers.get(topic).read(null, decoder);
        } catch (IOException ioe) {
            throw new SerializationException("Error deserializing Avro message", ioe);
        }
    }

    public Schema getSchemaForSpecificClass(String className) {
        Schema schema = null;
        try {
            Class recordClass = Class.forName(className);
            schema = (Schema) recordClass.getDeclaredField("SCHEMA$").get(null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return schema;
    }

    @Override
    public Schema getSchema(String topic) {
        return schemas.get(topic);
    }
}
