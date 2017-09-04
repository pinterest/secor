package com.pinterest.secor.parser;

import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.message.Message;
import junit.framework.TestCase;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.modules.junit4.PowerMockRunner;

import com.pinterest.secor.thrift.UnitTestMessage;

@RunWith(PowerMockRunner.class)
public class ThriftMessageParserTest extends TestCase {
    private SecorConfig mConfig;
    private long timestamp;

    @Override
    public void setUp() throws Exception {
        mConfig = Mockito.mock(SecorConfig.class);
        Mockito.when(TimestampedMessageParser.usingDateFormat(mConfig)).thenReturn("yyyy-MM-dd");
        Mockito.when(TimestampedMessageParser.usingHourFormat(mConfig)).thenReturn("HH");
        Mockito.when(TimestampedMessageParser.usingMinuteFormat(mConfig)).thenReturn("mm");
        Mockito.when(TimestampedMessageParser.usingDatePrefix(mConfig)).thenReturn("dt=");
        Mockito.when(TimestampedMessageParser.usingHourPrefix(mConfig)).thenReturn("hr=");
        Mockito.when(TimestampedMessageParser.usingMinutePrefix(mConfig)).thenReturn("min=");

        timestamp = System.currentTimeMillis();
    }

    private Message buildMessage(long timestamp, int timestampTwo, long timestampThree) throws Exception {
        UnitTestMessage thriftMessage = new UnitTestMessage(timestamp, "notimportant", timestampTwo, timestampThree);
        TSerializer serializer = new TSerializer(new TBinaryProtocol.Factory());
        byte[] data = serializer.serialize(thriftMessage);

        return new Message("test", 0, 0, null, data, timestamp);
    }

    @Test
    public void testExtractTimestampFromKafkaTimestamp() throws Exception {
        Mockito.when(mConfig.getBoolean("kafka.useTimestamp", false)).thenReturn(true);
        Mockito.when(mConfig.getMessageTimestampName()).thenReturn("blasdjlkjasdkl");
        Mockito.when(mConfig.getMessageTimestampId()).thenReturn(1);
        Mockito.when(mConfig.getMessageTimestampType()).thenReturn("i64");
        Mockito.when(mConfig.getThriftProtocolClass()).thenReturn("org.apache.thrift.protocol.TBinaryProtocol");

        ThriftMessageParser parser = new ThriftMessageParser(mConfig);

        assertEquals(1405970352000L, parser.extractTimestampMillis(buildMessage(1405970352L, 1, 2L)));
        assertEquals(1405970352123L, parser.extractTimestampMillis(buildMessage(1405970352123L, 1, 2L)));
    }

    @Test
    public void testExtractTimestamp() throws Exception {
        Mockito.when(mConfig.getMessageTimestampName()).thenReturn("blasdjlkjasdkl");
        Mockito.when(mConfig.getMessageTimestampId()).thenReturn(1);
        Mockito.when(mConfig.getMessageTimestampType()).thenReturn("i64");
        Mockito.when(mConfig.getThriftProtocolClass()).thenReturn("org.apache.thrift.protocol.TBinaryProtocol");

        ThriftMessageParser parser = new ThriftMessageParser(mConfig);

        assertEquals(1405970352000L, parser.extractTimestampMillis(buildMessage(1405970352L, 1, 2L)));
        assertEquals(1405970352123L, parser.extractTimestampMillis(buildMessage(1405970352123L, 1, 2L)));
    }

    @Test
    public void testExtractTimestampTwo() throws Exception {
        Mockito.when(mConfig.getMessageTimestampName()).thenReturn("timestampTwo");
        Mockito.when(mConfig.getMessageTimestampId()).thenReturn(3);
        Mockito.when(mConfig.getMessageTimestampType()).thenReturn("i32");
        Mockito.when(mConfig.getThriftProtocolClass()).thenReturn("org.apache.thrift.protocol.TBinaryProtocol");

        ThriftMessageParser parser = new ThriftMessageParser(mConfig);

        assertEquals(1405970352000L, parser.extractTimestampMillis(buildMessage(1L, 1405970352, 2L)));
        assertEquals(145028289000L, parser.extractTimestampMillis(buildMessage(1L, 145028289, 2L)));
    }

    @Test
    public void testExtractTimestampThree() throws Exception {
        Mockito.when(mConfig.getMessageTimestampName()).thenReturn("timestampThree");
        Mockito.when(mConfig.getMessageTimestampId()).thenReturn(6);
        Mockito.when(mConfig.getMessageTimestampType()).thenReturn("i64");
        Mockito.when(mConfig.getThriftProtocolClass()).thenReturn("org.apache.thrift.protocol.TBinaryProtocol");
        
        ThriftMessageParser parser = new ThriftMessageParser(mConfig);

        assertEquals(1405970352000L, parser.extractTimestampMillis(buildMessage(1L, 2, 1405970352L)));
        assertEquals(1405970352123L, parser.extractTimestampMillis(buildMessage(1L, 2, 1405970352123L)));
    }

    @Test(expected = NullPointerException.class)
    public void testAttemptExtractInvalidField() throws Exception {
        Mockito.when(mConfig.getMessageTimestampName()).thenReturn("requiredField");
        Mockito.when(mConfig.getMessageTimestampId()).thenReturn(2);
        Mockito.when(mConfig.getMessageTimestampType()).thenReturn("i64");

        ThriftMessageParser parser = new ThriftMessageParser(mConfig);

        parser.extractTimestampMillis(buildMessage(1L, 2, 3L));
    }
}
