package com.pinterest.secor.testing;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;

public class TestAvroRecord implements GenericRecord {
    @Override
    public void put(String key, Object v) {
        if (key == "someField") {
            value = (long) v;
        } else {
            throw new RuntimeException("Error");
        }
    }

    @Override
    public Object get(String key) {
        if (key == "someField") {
            return value;
        } else {
            return null;
        }
    }

    public static final Schema SCHEMA$ = SchemaBuilder.record("TestAvroRecord")
            .namespace("com.pinterest.secor.testing")
            .fields()
            .nullableLong("someField", 0L).endRecord();

    long value = 0L;

    @Override
    public void put(int i, Object v) {
        if (i == 0) {
            value = (Long) v;
        } else {
            throw new RuntimeException("This should not happen");
        }
    }

    @Override
    public Object get(int i) {
        if (i == 0) {
            return value;
        } else {
            throw new RuntimeException("This should not happen");
        }
    }

    @Override
    public Schema getSchema() {
        return SCHEMA$;
    }
}
