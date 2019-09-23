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
package com.pinterest.secor.io.impl;

import com.google.common.io.Files;
import com.pinterest.secor.common.*;
import com.pinterest.secor.io.FileReader;
import com.pinterest.secor.io.FileWriter;
import com.pinterest.secor.io.KeyValue;
import com.pinterest.secor.util.AvroSerializer;
import junit.framework.TestCase;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertArrayEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
public class AvroParquetFileReaderWriterFactoryTest extends TestCase {

    private AvroParquetFileReaderWriterFactory mFactory;
    private SpecificDatumWriter<GenericRecord> writer;
    private SecorConfig config;
    private GenericRecord msg1;
    private GenericRecord msg2;

    @Override
    public void setUp() throws Exception {
        config = Mockito.mock(SecorConfig.class);
        when(config.getSchemaRegistryUrl()).thenReturn("test");

        mFactory = new AvroParquetFileReaderWriterFactory(config);
    }

    @Test
    public void testAvroParquetReadWriteRoundTripUsingSchemaRegistry() throws Exception {
        Schema schema = SchemaBuilder.record("UnitTestRecord")
                .fields()
                .name("data").type().stringType().noDefault()
                .name("timestamp").type().nullable().longType().noDefault()
                .endRecord();

        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        msg1 = builder.set("data", "foo").set("timestamp", 1467176315L).build();
        msg2 = builder.set("data", "bar").set("timestamp", 1467176344L).build();

        writer = new SpecificDatumWriter(schema);

        SecorSchemaRegistryClient secorSchemaRegistryClient = Mockito.mock(SecorSchemaRegistryClient.class);
        when(secorSchemaRegistryClient.getSchema(anyString())).thenReturn(schema);
        when(secorSchemaRegistryClient.serialize(any(SpecificDatumWriter.class), any(String.class), any(GenericRecord.class))).thenReturn(AvroSerializer.serialize(writer, msg1), AvroSerializer.serialize(writer, msg2));
        when(secorSchemaRegistryClient.deserialize(any(String.class), any(byte[].class))).thenReturn(msg1, msg2, msg1, msg2);
        mFactory.schemaRegistry = secorSchemaRegistryClient;
        when(config.getFileReaderWriterFactory())
                .thenReturn(AvroParquetFileReaderWriterFactory.class.getName());

        testAvroParquetReadWriteRoundTrip(secorSchemaRegistryClient);
    }

    @Test
    public void testAvroParquetReadWriteRoundTripWithoutSchemaRegistry() throws Exception {
        Schema schema = new Schema.Parser().parse(getClass().getResourceAsStream("/unittest.avsc"));

        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        msg1 = builder.set("field1", "foo").set("field2", 1467176315L).build();
        msg2 = builder.set("field1", "bar").set("field2", 1467176344L).build();

        writer = new SpecificDatumWriter(schema);

        Map<String, String> avroSchemas = Stream
                .of(new String[][]{
                        {"test-avro-topic", "/unittest.avsc"}
                }).collect(Collectors.toMap(data -> data[0], data -> data[1]));
        when(config.getAvroMessageSchema()).thenReturn(avroSchemas);
        ConfigurableAvroSchemaRegistry configurableAvroSchemaRegistry = new ConfigurableAvroSchemaRegistry(config);
        mFactory.schemaRegistry = configurableAvroSchemaRegistry;
        when(config.getFileReaderWriterFactory())
                .thenReturn(AvroParquetFileReaderWriterFactory.class.getName());

        testAvroParquetReadWriteRoundTrip(configurableAvroSchemaRegistry);
    }


    @Ignore
    public void testAvroParquetReadWriterRoundTripWithConfluentSchemaRegister() throws Exception {

        Schema schema = SchemaBuilder.record("UnitTestRecord")
                .fields()
                .name("data").type().stringType().noDefault()
                .name("timestamp").type().nullable().longType().noDefault()
                .endRecord();

        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        msg1 = builder.set("data", "foo").set("timestamp", 1467176315L).build();
        writer = new SpecificDatumWriter(schema);
        SecorSchemaRegistryClient secorSchemaRegistryClient = new SecorSchemaRegistryClient(config);

        KeyValue kv1 = (new KeyValue(23232, AvroSerializer.serialize(writer, msg1)));


        Exception e = null;
        try {
            secorSchemaRegistryClient.deserialize("test-avro-topic", kv1.getValue());
        } catch (SerializationException se) {
            se.printStackTrace();
            e = se;
        } catch (Exception otherE) {
            //ignore
        }

        assertEquals(null, e);

    }

    private void testAvroParquetReadWriteRoundTrip(AvroSchemaRegistry schemaRegistry) throws Exception {
        LogFilePath tempLogFilePath = new LogFilePath(Files.createTempDir().toString(), "test-avro-topic",
                new String[] { "part-1" }, 0, 1, 23232, ".avro");

        FileWriter fileWriter = mFactory.BuildFileWriter(tempLogFilePath, null);

        KeyValue kv1 = (new KeyValue(23232, AvroSerializer.serialize(writer, msg1)));
        KeyValue kv2 = (new KeyValue(23233, AvroSerializer.serialize(writer, msg2)));

        fileWriter.write(kv1);
        fileWriter.write(kv2);
        fileWriter.close();

        FileReader fileReader = mFactory.BuildFileReader(tempLogFilePath, null);

        KeyValue kvout = fileReader.next();
        assertEquals(kv1.getOffset(), kvout.getOffset());
        assertArrayEquals(kv1.getValue(), kvout.getValue());
        assertEquals(msg1, schemaRegistry.deserialize("test-avro-topic", kvout.getValue()));

        kvout = fileReader.next();
        assertEquals(kv2.getOffset(), kvout.getOffset());
        assertArrayEquals(kv2.getValue(), kvout.getValue());
        assertEquals(msg2, schemaRegistry.deserialize("test-avro-topic", kvout.getValue()));
    }
}