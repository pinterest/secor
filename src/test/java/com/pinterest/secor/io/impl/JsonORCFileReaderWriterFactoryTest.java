package com.pinterest.secor.io.impl;

import com.google.common.io.Files;
import com.pinterest.secor.common.LogFilePath;
import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.io.FileReader;
import com.pinterest.secor.io.FileWriter;
import com.pinterest.secor.io.KeyValue;
import com.pinterest.secor.util.orc.schema.DefaultORCSchemaProvider;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

public class JsonORCFileReaderWriterFactoryTest {

    private static final String DEFAULT_ORC_SCHEMA_PROVIDER = DefaultORCSchemaProvider.class.getCanonicalName();

    private CompressionCodec codec;

    @Before
    public void setUp() throws Exception {
        codec = new GzipCodec();
    }

    private LogFilePath getTempLogFilePath(String topic) {
        return new LogFilePath(Files.createTempDir().toString(),
            topic,
            new String[]{"part-1"},
            0, 1, 0, ".log"
        );
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNoSchema() throws Exception {
        PropertiesConfiguration properties = new PropertiesConfiguration();
        properties.setProperty("secor.orc.schema.provider", DEFAULT_ORC_SCHEMA_PROVIDER);

        SecorConfig config = new SecorConfig(properties);
        JsonORCFileReaderWriterFactory factory = new JsonORCFileReaderWriterFactory(config);

        LogFilePath tempLogFilePath = getTempLogFilePath("test-topic");

        // IllegalArgumentException is expected
        FileWriter fileWriter = factory.BuildFileWriter(tempLogFilePath, codec);
    }

    @Test
    public void testMapOfStringToString() throws Exception {
        PropertiesConfiguration properties = new PropertiesConfiguration();
        properties.setProperty("secor.orc.schema.provider", DEFAULT_ORC_SCHEMA_PROVIDER);
        properties.setProperty("secor.orc.message.schema.test-topic-map1", "struct<mappings:map<string\\,string>>");

        SecorConfig config = new SecorConfig(properties);
        JsonORCFileReaderWriterFactory factory = new JsonORCFileReaderWriterFactory(config);

        LogFilePath tempLogFilePath = getTempLogFilePath("test-topic-map1");

        FileWriter fileWriter = factory.BuildFileWriter(tempLogFilePath, codec);
        KeyValue written1 = new KeyValue(10001, "{\"mappings\":{\"key1\":\"value1\",\"key2\":\"value2\"}}".getBytes());
        fileWriter.write(written1);
        fileWriter.close();

        FileReader fileReader = factory.BuildFileReader(tempLogFilePath, codec);
        KeyValue read1 = fileReader.next();
        fileReader.close();

        assertArrayEquals(read1.getValue(), written1.getValue());
    }

    @Test
    public void testMapOfStringToInteger() throws Exception {
        PropertiesConfiguration properties = new PropertiesConfiguration();
        properties.setProperty("secor.orc.schema.provider", DEFAULT_ORC_SCHEMA_PROVIDER);
        properties.setProperty("secor.orc.message.schema.test-topic-map2", "struct<mappings:map<string\\,int>>");

        SecorConfig config = new SecorConfig(properties);
        JsonORCFileReaderWriterFactory factory = new JsonORCFileReaderWriterFactory(config);

        LogFilePath tempLogFilePath = getTempLogFilePath("test-topic-map2");

        FileWriter fileWriter = factory.BuildFileWriter(tempLogFilePath, codec);
        KeyValue written1 = new KeyValue(12345, "{\"mappings\":{\"key1\":1,\"key2\":-2}}".getBytes());
        KeyValue written2 = new KeyValue(12346, "{\"mappings\":{\"key3\":1523,\"key4\":3451325}}".getBytes());
        KeyValue written3 = new KeyValue(12347, "{\"mappings\":{\"key5\":0,\"key6\":-8382}}".getBytes());
        fileWriter.write(written1);
        fileWriter.write(written2);
        fileWriter.write(written3);
        fileWriter.close();

        FileReader fileReader = factory.BuildFileReader(tempLogFilePath, codec);
        KeyValue read1 = fileReader.next();
        KeyValue read2 = fileReader.next();
        KeyValue read3 = fileReader.next();
        fileReader.close();

        assertArrayEquals(read1.getValue(), written1.getValue());
        assertArrayEquals(read2.getValue(), written2.getValue());
        assertArrayEquals(read3.getValue(), written3.getValue());
    }

    @Test
    public void testMultipleMaps() throws Exception {
        PropertiesConfiguration properties = new PropertiesConfiguration();
        properties.setProperty("secor.orc.schema.provider", "com.pinterest.secor.util.orc.schema.DefaultORCSchemaProvider");
        properties.setProperty("secor.orc.message.schema.test-topic-multimaps", "struct<f1:map<string\\,int>\\,f2:map<string\\,string>>");

        SecorConfig config = new SecorConfig(properties);
        JsonORCFileReaderWriterFactory factory = new JsonORCFileReaderWriterFactory(config);

        LogFilePath tempLogFilePath = getTempLogFilePath("test-topic-multimaps");

        FileWriter fileWriter = factory.BuildFileWriter(tempLogFilePath, codec);
        KeyValue written1 = new KeyValue(13001, "{\"f1\":{\"k1\":0,\"k2\":1234},\"f2\":{\"k3\":\"test\"}}".getBytes());
        fileWriter.write(written1);
        fileWriter.close();

        FileReader fileReader = factory.BuildFileReader(tempLogFilePath, codec);
        KeyValue read1 = fileReader.next();
        fileReader.close();

        assertArrayEquals(read1.getValue(), written1.getValue());
    }

    @Test
    public void testJsonORCReadWriteRoundTrip() throws Exception {
        PropertiesConfiguration properties = new PropertiesConfiguration();
        properties.setProperty("secor.orc.schema.provider", DEFAULT_ORC_SCHEMA_PROVIDER);
        properties.setProperty("secor.orc.message.schema.test-topic", "struct<firstname:string\\,age:int\\,test:map<string\\,string>>");

        SecorConfig config = new SecorConfig(properties);
        JsonORCFileReaderWriterFactory factory = new JsonORCFileReaderWriterFactory(config);

        LogFilePath tempLogFilePath = getTempLogFilePath("test-topic");

        FileWriter fileWriter = factory.BuildFileWriter(tempLogFilePath, codec);
        KeyValue kv1 = new KeyValue(23232, "{\"firstname\":\"Jason\",\"age\":48,\"test\":{\"k1\":\"v1\",\"k2\":\"v2\"}}".getBytes());
        KeyValue kv2 = new KeyValue(23233, "{\"firstname\":\"Christina\",\"age\":37,\"test\":{\"k3\":\"v3\"}}".getBytes());
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