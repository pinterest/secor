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

import java.util.Random;

import static org.junit.Assert.assertEquals;

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

    private void runCommonTest(String schema, String topic, String... jsonRecords) throws Exception {
        PropertiesConfiguration properties = new PropertiesConfiguration();
        properties.setProperty("secor.orc.schema.provider", DEFAULT_ORC_SCHEMA_PROVIDER);
        properties.setProperty(String.format("secor.orc.message.schema.%s", topic), schema);

        SecorConfig config = new SecorConfig(properties);
        JsonORCFileReaderWriterFactory factory = new JsonORCFileReaderWriterFactory(config);

        LogFilePath tempLogFilePath = getTempLogFilePath(topic);
        KeyValue[] written = writeRecords(factory, tempLogFilePath, jsonRecords);
        KeyValue[] read = readRecords(factory, tempLogFilePath, jsonRecords.length);

        for (int i = 0; i < jsonRecords.length; i++) {
            // String comparisons make debugging a bit easier, albeit induce greater memory footprint.
            // For example, byte array comparison yields an error message like:
            //
            //     java.lang.AssertionError: array lengths differed, expected.length=35 actual.length=32
            //
            // whereas string comparison produces a much more helpful message like:
            //
            //     org.junit.ComparisonFailure:
            //     Expected :{"mappings":{"key5":0,"key6":1.25}}
            //     Actual   :{"mappings":{"key5":0,"key6":1}}
            //
            // The original assertion code was:
            //
            //     assertArrayEquals(written[i].getValue(), read[i].getValue())
            //
            assertEquals(new String(written[i].getValue()), new String(read[i].getValue()));
        }
    }

    private KeyValue[] writeRecords(JsonORCFileReaderWriterFactory factory, LogFilePath tempLogFilePath,
                                    String... jsonRecords) throws Exception {
        FileWriter fileWriter = factory.BuildFileWriter(tempLogFilePath, codec);
        KeyValue[] keyValues = new KeyValue[jsonRecords.length];
        for (int offset = 0; offset < jsonRecords.length; offset++) {
            String jsonRecord = jsonRecords[offset];
            keyValues[offset] = new KeyValue(offset, jsonRecord.getBytes());
            fileWriter.write(keyValues[offset]);
        }
        fileWriter.close();

        return keyValues;
    }

    private KeyValue[] readRecords(JsonORCFileReaderWriterFactory factory, LogFilePath tempLogFilePath, int count)
            throws Exception {
        FileReader fileReader = factory.BuildFileReader(tempLogFilePath, codec);
        KeyValue[] keyValues = new KeyValue[count];
        for (int offset = 0; offset < count; offset++) {
            keyValues[offset] = fileReader.next();
        }
        fileReader.close();

        return keyValues;
    }

    @Test
    public void testMapOfStringToString() throws Exception {
        runCommonTest(
                "struct<mappings:map<string\\,string>>",
                "string-to-string",
                "{\"mappings\":{\"key1\":\"value1\",\"key2\":\"value2\"}}"
        );
    }

    @Test
    public void testMapOfStringToInteger() throws Exception {
        runCommonTest(
                "struct<mappings:map<string\\,int>>",
                "string-to-integer",
                "{\"mappings\":{\"key1\":1,\"key2\":-2}}",
                "{\"mappings\":{\"key3\":1523,\"key4\":3451325}}",
                "{\"mappings\":{\"key5\":0,\"key6\":-8382}}"
        );
    }

    @Test
    public void testMultipleMaps() throws Exception {
        runCommonTest(
                "struct<f1:map<string\\,int>\\,f2:map<string\\,string>>",
                "multiple-maps",
                "{\"f1\":{\"k1\":0,\"k2\":1234},\"f2\":{\"k3\":\"test\"}}"
        );
    }

    @Test
    public void testJsonORCReadWriteRoundTrip() throws Exception {
        runCommonTest(
                "struct<firstname:string\\,age:int\\,test:map<string\\,string>>",
                "round-trip",
                "{\"firstname\":\"Jason\",\"age\":48,\"test\":{\"k1\":\"v1\",\"k2\":\"v2\"}}",
                "{\"firstname\":\"Christina\",\"age\":37,\"test\":{\"k3\":\"v3\"}}"
        );
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
        int rowCount = 100;
        String[] jsonObjects = new String[rowCount];
        for (int i = 0; i < rowCount; i++) {
            int keyCount = random.nextInt(5000) + 1;
            jsonObjects[i] = makeJsonObject(i, keyCount).toString();
        }

        runCommonTest(
                "struct<kvs:map<string\\,int>>",
                "large-key-set",
                jsonObjects
        );
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWithNonStringKeys() throws Exception {
        runCommonTest(
                "struct<kvs:map<int\\,int>>",
                "non-string-keys",
                "{0:{1:2,3:4}}"
        );
    }

    @Test
    public void testUnionType() throws Exception {
        runCommonTest(
                "struct<values:uniontype<int\\,string>>",
                "union-type",
                "{\"values\":\"stringvalue\"}",
                "{\"values\":1234}",
                "{\"values\":null}"
        );
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testUnionTypeWithNonPrimitive() throws Exception {
        runCommonTest(
                "struct<v1:uniontype<int\\,struct<v2:string\\,v3:bigint>>>",
                "union-type-with-non-primitive",
                "{\"v1\":1234}",
                "{\"v1\":{\"v2\":null,\"v3\":1048576}}"
        );
    }
}