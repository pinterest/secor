/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.pinterest.secor.parser;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.modules.junit4.PowerMockRunner;

import com.google.protobuf.CodedOutputStream;
import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.message.Message;
import com.pinterest.secor.protobuf.Messages.UnitTestMessage1;
import com.pinterest.secor.protobuf.Messages.UnitTestMessage2;

import junit.framework.TestCase;

@RunWith(PowerMockRunner.class)
public class ProtobufMessageParserTest extends TestCase {
    private SecorConfig mConfig;
    private long timestamp;

    private Message buildMessage(long timestamp) throws Exception {
        byte data[] = new byte[16];
        CodedOutputStream output = CodedOutputStream.newInstance(data);
        output.writeUInt64(1, timestamp);
        return new Message("test", 0, 0, null, data, timestamp);
    }

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

    @Test
    public void testExtractTimestampMillisFromKafkaTimestamp() throws Exception {
        Mockito.when(mConfig.getBoolean("kafka.useTimestamp", false)).thenReturn(true);
        ProtobufMessageParser parser = new ProtobufMessageParser(mConfig);

        assertEquals(1405970352000l, parser.extractTimestampMillis(buildMessage(1405970352l)));
        assertEquals(1405970352123l, parser.extractTimestampMillis(buildMessage(1405970352123l)));
    }

    @Test
    public void testExtractTimestampMillis() throws Exception {
        ProtobufMessageParser parser = new ProtobufMessageParser(mConfig);

        assertEquals(1405970352000l, parser.extractTimestampMillis(buildMessage(1405970352l)));
        assertEquals(1405970352123l, parser.extractTimestampMillis(buildMessage(1405970352123l)));
    }

    @Test
    public void testExtractPathTimestampMillis() throws Exception {
        Map<String, String> classPerTopic = new HashMap<String, String>();
        classPerTopic.put("test", UnitTestMessage1.class.getName());
        Mockito.when(mConfig.getMessageTimestampName()).thenReturn("timestamp");
        Mockito.when(mConfig.getProtobufMessageClassPerTopic()).thenReturn(classPerTopic);

        ProtobufMessageParser parser = new ProtobufMessageParser(mConfig);

        UnitTestMessage1 message = UnitTestMessage1.newBuilder().setTimestamp(1405970352L).build();
        assertEquals(1405970352000l,
                parser.extractTimestampMillis(new Message("test", 0, 0, null, message.toByteArray(), timestamp)));

        message = UnitTestMessage1.newBuilder().setTimestamp(1405970352123l).build();
        assertEquals(1405970352123l,
                parser.extractTimestampMillis(new Message("test", 0, 0, null, message.toByteArray(), timestamp)));
    }

    @Test
    public void testExtractNestedTimestampMillis() throws Exception {
        Map<String, String> classPerTopic = new HashMap<String, String>();
        classPerTopic.put("*", UnitTestMessage2.class.getName());
        Mockito.when(mConfig.getMessageTimestampName()).thenReturn("internal.timestamp");
        Mockito.when(mConfig.getProtobufMessageClassPerTopic()).thenReturn(classPerTopic);

        ProtobufMessageParser parser = new ProtobufMessageParser(mConfig);

        UnitTestMessage2 message = UnitTestMessage2.newBuilder()
                .setInternal(UnitTestMessage2.Internal.newBuilder().setTimestamp(1405970352L).build()).build();
        assertEquals(1405970352000l,
                parser.extractTimestampMillis(new Message("test", 0, 0, null, message.toByteArray(), timestamp)));

        message = UnitTestMessage2.newBuilder()
                .setInternal(UnitTestMessage2.Internal.newBuilder().setTimestamp(1405970352123l).build()).build();
        assertEquals(1405970352123l,
                parser.extractTimestampMillis(new Message("test", 0, 0, null, message.toByteArray(), timestamp)));
    }
}
