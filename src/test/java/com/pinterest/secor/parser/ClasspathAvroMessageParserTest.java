package com.pinterest.secor.parser;

import ai.humn.telematics.avro.DataHelper;
import ai.humn.telematics.avro.Serializer;
import ai.humn.telematics.avro.dto.RacDTO;
import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.message.Message;
import com.pinterest.secor.testing.TestAvroRecord;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import static com.pinterest.secor.testing.ConfigTestUtils.createMockTestConfig;

public class ClasspathAvroMessageParserTest {
    private final String racTestTopic = "test_topic_rac";
    private final String testTopic = "test_topic";

    @Test
    public void testTimestampExtractedCorrectly() throws Exception {
        SecorConfig secorConfig = createMockTestConfig();
        initParser(secorConfig);
        RacDTO racDTO = DataHelper.correctRacData();
        Serializer.configureEnvironment();
        Serializer serializer = new Serializer();

        byte[] bytes = serializer.serialize(racDTO);
        ClasspathAvroMessageParser parser = new ClasspathAvroMessageParser(secorConfig);
        Message message = new Message(racTestTopic, 1, 10, null, bytes, System.currentTimeMillis(), new ArrayList<>());
        long milis = parser.extractTimestampMillis(message);
        Assert.assertEquals(racDTO.getSysProcessedTime().longValue(), milis);
    }

    @Test
    public void testDefaultValueIsReturnedWhenDeserializationFailsAndTimestampNotRequired() throws Exception {
        SecorConfig secorConfig = createMockTestConfig();
        initParser(secorConfig);
        Mockito.when(secorConfig.isMessageTimestampRequired()).thenReturn(false);

        byte[] fakeSerializedMessage = new byte[1];
        ClasspathAvroMessageParser parser = new ClasspathAvroMessageParser(secorConfig);
        Message message = new Message(racTestTopic, 1, 10, null, fakeSerializedMessage, System.currentTimeMillis(), new ArrayList<>());
        long milis = parser.extractTimestampMillis(message);
        Assert.assertEquals(0L, milis);
    }

    @Test(expected = RuntimeException.class)
    public void testExceptionIsThrownWhenWrongTypeAndTimestamptRequired() throws Exception {
        SecorConfig secorConfig = createMockTestConfig();
        initParser(secorConfig);
        HashMap<String, String> testRecordSchema = new HashMap<>();
        testRecordSchema.put(testTopic, TestAvroRecord.class.getCanonicalName());
        Mockito.when(secorConfig.getAvroSpecificClassesSchemas()).thenReturn(testRecordSchema);
        TestAvroRecord record = new TestAvroRecord();
        byte[] bytes = serializeTestRecord(record);
        ClasspathAvroMessageParser parser = new ClasspathAvroMessageParser(secorConfig);
        Message message = new Message(testTopic, 1, 10, null, bytes, System.currentTimeMillis(), new ArrayList<>());
        long milis = parser.extractTimestampMillis(message);
        Assert.assertEquals(0L, milis);
    }

    @Test
    public void testDefaultValueIsReturnedWhenWrongTypeAndTimestamptNotRequired() throws Exception {
        SecorConfig secorConfig = createMockTestConfig();
        initParser(secorConfig);
        HashMap<String, String> testRecordSchema = new HashMap<>();
        testRecordSchema.put(testTopic, TestAvroRecord.class.getCanonicalName());
        Mockito.when(secorConfig.getAvroSpecificClassesSchemas()).thenReturn(testRecordSchema);
        Mockito.when(secorConfig.isMessageTimestampRequired()).thenReturn(false);
        TestAvroRecord record = new TestAvroRecord();
        byte[] bytes = serializeTestRecord(record);
        ClasspathAvroMessageParser parser = new ClasspathAvroMessageParser(secorConfig);
        Message message = new Message(testTopic, 1, 10, null, bytes, System.currentTimeMillis(), new ArrayList<>());
        long milis = parser.extractTimestampMillis(message);
        Assert.assertEquals(0L, milis);
    }

    private void initParser(SecorConfig config) {
        Mockito.when(TimestampedMessageParser.usingDateFormat(config)).thenReturn("yyyy-MM-dd");
        Mockito.when(TimestampedMessageParser.usingHourFormat(config)).thenReturn("HH");
        Mockito.when(TimestampedMessageParser.usingMinuteFormat(config)).thenReturn("mm");
        Mockito.when(TimestampedMessageParser.usingDatePrefix(config)).thenReturn("dt=");
        Mockito.when(TimestampedMessageParser.usingHourPrefix(config)).thenReturn("hr=");
        Mockito.when(TimestampedMessageParser.usingMinutePrefix(config)).thenReturn("min=");

    }

    private byte[] serializeTestRecord(TestAvroRecord record) {
        DatumWriter<TestAvroRecord> writer = new GenericDatumWriter<>(record.getSchema());
        byte[] data;
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        try {
            Encoder encoder = EncoderFactory.get().directBinaryEncoder(stream, null);
            writer.write(record, encoder);
            encoder.flush();
            data = stream.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return data;
    }

}

