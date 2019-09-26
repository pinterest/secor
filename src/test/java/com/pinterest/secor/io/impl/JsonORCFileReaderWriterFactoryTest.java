package com.pinterest.secor.io.impl;

import com.google.common.io.Files;
import com.pinterest.secor.common.LogFilePath;
import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.io.FileReader;
import com.pinterest.secor.io.FileWriter;
import com.pinterest.secor.io.KeyValue;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

public class JsonORCFileReaderWriterFactoryTest {

    private JsonORCFileReaderWriterFactory factory;
    private SecorConfig config;
    private CompressionCodec codec;

    @Before
    public void setUp() throws Exception {
        PropertiesConfiguration properties = new PropertiesConfiguration();
        properties.setProperty("secor.orc.schema.provider", "com.pinterest.secor.util.orc.schema.DefaultORCSchemaProvider");
        properties.setProperty("secor.orc.message.schema.test-topic", "struct<firstname:string\\,age:int>");
        config = new SecorConfig(properties);
        factory = new JsonORCFileReaderWriterFactory(config);
        codec = new GzipCodec();
    }

    @Test
    public void testJsonORCReadWriteRoundTrip() throws Exception {
        LogFilePath tempLogFilePath = new LogFilePath(Files.createTempDir().toString(),
            "test-topic",
            new String[]{"part-1"},
            0,
            1,
            0,
            ".log"
        );

        FileWriter fileWriter = factory.BuildFileWriter(tempLogFilePath, codec);
        KeyValue kv1 = new KeyValue(23232, "{\"firstname\":\"Jason\",\"age\":48}".getBytes());
        KeyValue kv2 = new KeyValue(23233, "{\"firstname\":\"Christina\",\"age\":37}".getBytes());
        fileWriter.write(kv1);
        fileWriter.write(kv2);
        fileWriter.close();

        FileReader fileReader = factory.BuildFileReader(tempLogFilePath, codec);
        KeyValue kv3 = fileReader.next();
        KeyValue kv4 = fileReader.next();
        fileReader.close();

        assertArrayEquals(kv1.getValue(), kv3.getValue());
        assertArrayEquals(kv2.getValue(), kv4.getValue());
    }
}