package com.pinterest.secor.io.impl;

import com.google.common.io.Files;
import com.google.gson.JsonObject;
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
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.Random;

import static org.junit.Assert.assertArrayEquals;

public class JsonORCFileReaderWriterFactoryTest {

    private static final String DEFAULT_ORC_SCHEMA_PROVIDER = DefaultORCSchemaProvider.class.getCanonicalName();

    private CompressionCodec codec;

    /**
     * We want to use a pre-determined seed to make the tests deterministic.
     */
    private Random random = new Random(0);

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
    public void testUnionType() throws Exception {
        PropertiesConfiguration properties = new PropertiesConfiguration();
        properties.setProperty("secor.orc.schema.provider", DEFAULT_ORC_SCHEMA_PROVIDER);
        properties.setProperty("secor.orc.message.schema.test-topic-union", "struct<values:uniontype<int\\,string>>");

        SecorConfig config = new SecorConfig(properties);
        JsonORCFileReaderWriterFactory factory = new JsonORCFileReaderWriterFactory(config);

        LogFilePath tempLogFilePath = getTempLogFilePath("test-topic-union");

        FileWriter fileWriter = factory.BuildFileWriter(tempLogFilePath, codec);
        KeyValue written1 = new KeyValue(10001, "{\"values\":\"stringvalue\"}".getBytes());
        KeyValue written2 = new KeyValue(10002, "{\"values\":1234}".getBytes());
        fileWriter.write(written1);
        fileWriter.write(written2);
        fileWriter.close();
    }

    @Test(expected = NotImplementedException.class)
    public void testUnionTypeWithNonPrimitive() throws Exception {
        PropertiesConfiguration properties = new PropertiesConfiguration();
        properties.setProperty("secor.orc.schema.provider", DEFAULT_ORC_SCHEMA_PROVIDER);
        properties.setProperty("secor.orc.message.schema.test-topic-union", "struct<v1:uniontype<int\\,struct<v2:string\\,v3:bigint>>>");

        SecorConfig config = new SecorConfig(properties);
        JsonORCFileReaderWriterFactory factory = new JsonORCFileReaderWriterFactory(config);

        LogFilePath tempLogFilePath = getTempLogFilePath("test-topic-union");

        FileWriter fileWriter = factory.BuildFileWriter(tempLogFilePath, codec);
        KeyValue written1 = new KeyValue(10001, "{\"v1\":1234}".getBytes());
        KeyValue written2 = new KeyValue(10002, "{\"v1\":{\"v2\":null,\"v3\":1048576}}".getBytes());
        fileWriter.write(written1);
        fileWriter.write(written2);
        fileWriter.close();
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

        assertArrayEquals(written1.getValue(), read1.getValue());
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

        assertArrayEquals(written1.getValue(), read1.getValue());
        assertArrayEquals(written2.getValue(), read2.getValue());
        assertArrayEquals(written3.getValue(), read3.getValue());
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

        assertArrayEquals(written1.getValue(), read1.getValue());
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
        KeyValue written1 = new KeyValue(23232, "{\"firstname\":\"Jason\",\"age\":48,\"test\":{\"k1\":\"v1\",\"k2\":\"v2\"}}".getBytes());
        KeyValue written2 = new KeyValue(23233, "{\"firstname\":\"Christina\",\"age\":37,\"test\":{\"k3\":\"v3\"}}".getBytes());
        fileWriter.write(written1);
        fileWriter.write(written2);
        fileWriter.close();

        FileReader fileReader = factory.BuildFileReader(tempLogFilePath, codec);
        KeyValue read1 = fileReader.next();
        KeyValue read2 = fileReader.next();
        fileReader.close();

        assertArrayEquals(written1.getValue(), read1.getValue());
        assertArrayEquals(written2.getValue(), read2.getValue());
    }

    /**
     * Generates a JsonObject of a specified keyset size.
     */
    private JsonObject makeJsonObject(int row, int keysetSize) {
        JsonObject obj = new JsonObject();
        JsonObject kvs = new JsonObject();

        for (int i = 0; i < keysetSize; i++) {
            String key = String.format("key-%d-%d", row, i);
            int value = random.nextInt();
            kvs.addProperty(key, value);
        }
        obj.add("kvs", kvs);

        return obj;
    }

    @Test
    public void testWithLargeKeySet() throws Exception {
        PropertiesConfiguration properties = new PropertiesConfiguration();
        properties.setProperty("secor.orc.schema.provider", DEFAULT_ORC_SCHEMA_PROVIDER);
        properties.setProperty("secor.orc.message.schema.test-large-keyset", "struct<kvs:map<string\\,int>>");

        SecorConfig config = new SecorConfig(properties);
        JsonORCFileReaderWriterFactory factory = new JsonORCFileReaderWriterFactory(config);
        LogFilePath tempLogFilePath = getTempLogFilePath("test-large-keyset");

        int rowCount = 100;
        KeyValue written[] = new KeyValue[rowCount];
        FileWriter fileWriter = factory.BuildFileWriter(tempLogFilePath, codec);
        for (int i = 0; i < rowCount; i++) {
            int keyCount = random.nextInt(5000) + 1;
            written[i] = new KeyValue(19000 + i, makeJsonObject(i, keyCount).toString().getBytes());
            fileWriter.write(written[i]);
        }
        fileWriter.close();

        FileReader fileReader = factory.BuildFileReader(tempLogFilePath, codec);
        for (int i = 0; i < rowCount; i++) {
            KeyValue read = fileReader.next();
            assertArrayEquals(written[i].getValue(), read.getValue());
        }
        fileReader.close();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWithNonStringKeys() throws Exception {
        PropertiesConfiguration properties = new PropertiesConfiguration();
        properties.setProperty("secor.orc.schema.provider", DEFAULT_ORC_SCHEMA_PROVIDER);
        properties.setProperty("secor.orc.message.schema.test-nonstring-keys", "struct<kvs:map<int\\,int>>");

        SecorConfig config = new SecorConfig(properties);
        JsonORCFileReaderWriterFactory factory = new JsonORCFileReaderWriterFactory(config);
        LogFilePath tempLogFilePath = getTempLogFilePath("test-nonstring-keys");

        FileWriter fileWriter = factory.BuildFileWriter(tempLogFilePath, codec);
        KeyValue written1 = new KeyValue(90001, "{\"kvs\":{1:2,3:4}}".getBytes());
        fileWriter.write(written1);
        fileWriter.close();
    }
}