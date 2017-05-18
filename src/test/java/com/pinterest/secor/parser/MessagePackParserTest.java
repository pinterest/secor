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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.message.Message;
import junit.framework.TestCase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.msgpack.jackson.dataformat.MessagePackFactory;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.HashMap;
import java.util.TimeZone;

@RunWith(PowerMockRunner.class)
public class MessagePackParserTest extends TestCase {

    SecorConfig mConfig;
    private MessagePackParser mMessagePackParser;
    private Message mMessageWithSecondsTimestamp;
    private Message mMessageWithMillisTimestamp;
    private Message mMessageWithMillisFloatTimestamp;
    private Message mMessageWithMillisStringTimestamp;
    private ObjectMapper mObjectMapper;
    private long timestamp;

    @Override
    public void setUp() throws Exception {
        mConfig = Mockito.mock(SecorConfig.class);
        Mockito.when(mConfig.getMessageTimestampName()).thenReturn("ts");
        Mockito.when(mConfig.getTimeZone()).thenReturn(TimeZone.getTimeZone("UTC"));
        Mockito.when(TimestampedMessageParser.usingDateFormat(mConfig)).thenReturn("yyyy-MM-dd");
        Mockito.when(TimestampedMessageParser.usingHourFormat(mConfig)).thenReturn("HH");
        Mockito.when(TimestampedMessageParser.usingMinuteFormat(mConfig)).thenReturn("mm");
        Mockito.when(TimestampedMessageParser.usingDatePrefix(mConfig)).thenReturn("dt=");
        Mockito.when(TimestampedMessageParser.usingHourPrefix(mConfig)).thenReturn("hr=");
        Mockito.when(TimestampedMessageParser.usingMinutePrefix(mConfig)).thenReturn("min=");

        mMessagePackParser = new MessagePackParser(mConfig);
        mObjectMapper = new ObjectMapper(new MessagePackFactory());

        timestamp = System.currentTimeMillis();

        HashMap<String, Object> mapWithSecondTimestamp = new HashMap<String, Object>();
        mapWithSecondTimestamp.put("ts", 1405970352);
        mMessageWithSecondsTimestamp = new Message("test", 0, 0, null,
                mObjectMapper.writeValueAsBytes(mapWithSecondTimestamp), timestamp);

        HashMap<String, Object> mapWithMillisTimestamp = new HashMap<String, Object>();
        mapWithMillisTimestamp.put("ts", 1405970352123l);
        mapWithMillisTimestamp.put("isActive", true);
        mapWithMillisTimestamp.put("email", "alice@example.com");
        mapWithMillisTimestamp.put("age", 27);
        mMessageWithMillisTimestamp = new Message("test", 0, 0, null,
                mObjectMapper.writeValueAsBytes(mapWithMillisTimestamp), timestamp);


        HashMap<String, Object> mapWithMillisFloatTimestamp = new HashMap<String, Object>();
        mapWithMillisFloatTimestamp.put("ts", 1405970352123.0);
        mapWithMillisFloatTimestamp.put("isActive", false);
        mapWithMillisFloatTimestamp.put("email", "bob@example.com");
        mapWithMillisFloatTimestamp.put("age", 35);
        mMessageWithMillisFloatTimestamp = new Message("test", 0, 0, null,
                mObjectMapper.writeValueAsBytes(mapWithMillisFloatTimestamp), timestamp);

        HashMap<String, Object> mapWithMillisStringTimestamp = new HashMap<String, Object>();
        mapWithMillisStringTimestamp.put("ts", "1405970352123");
        mapWithMillisStringTimestamp.put("isActive", null);
        mapWithMillisStringTimestamp.put("email", "charlie@example.com");
        mapWithMillisStringTimestamp.put("age", 67);
        mMessageWithMillisStringTimestamp = new Message("test", 0, 0, null,
                mObjectMapper.writeValueAsBytes(mapWithMillisStringTimestamp), timestamp);
    }

    @Test
    public void testExtractTimestampMillisFromKafkaTimestamp() throws Exception {
        Mockito.when(mConfig.useKafkaTimestamp()).thenReturn(true);
        mMessagePackParser = new MessagePackParser(mConfig);

        assertEquals(timestamp, mMessagePackParser.getTimestampMillis(
                mMessageWithSecondsTimestamp));
        assertEquals(timestamp, mMessagePackParser.getTimestampMillis(
                mMessageWithMillisTimestamp));
        assertEquals(timestamp, mMessagePackParser.getTimestampMillis(
                mMessageWithMillisFloatTimestamp));
        assertEquals(timestamp, mMessagePackParser.getTimestampMillis(
                mMessageWithMillisStringTimestamp));
    }

    @Test
    public void testExtractTimestampMillis() throws Exception {
        assertEquals(1405970352000l, mMessagePackParser.getTimestampMillis(
                mMessageWithSecondsTimestamp));
        assertEquals(1405970352123l, mMessagePackParser.getTimestampMillis(
                mMessageWithMillisTimestamp));
        assertEquals(1405970352123l, mMessagePackParser.getTimestampMillis(
                mMessageWithMillisFloatTimestamp));
        assertEquals(1405970352123l, mMessagePackParser.getTimestampMillis(
                mMessageWithMillisStringTimestamp));
    }

    @Test(expected=NullPointerException.class)
    public void testMissingTimestamp() throws Exception {
        HashMap<String, Object> mapWithoutTimestamp = new HashMap<String, Object>();
        mapWithoutTimestamp.put("email", "mary@example.com");
        Message nMessageWithoutTimestamp = new Message("test", 0, 0, null,
                mObjectMapper.writeValueAsBytes(mapWithoutTimestamp), timestamp);
        mMessagePackParser.getTimestampMillis(nMessageWithoutTimestamp);
    }

    @Test(expected=NumberFormatException.class)
    public void testUnsupportedTimestampFormat() throws Exception {
        HashMap<String, Object> mapWitUnsupportedFormatTimestamp = new HashMap<String, Object>();
        mapWitUnsupportedFormatTimestamp.put("ts", "2014-11-14T18:12:52.878Z");
        Message nMessageWithUnsupportedFormatTimestamp = new Message("test", 0, 0, null,
                mObjectMapper.writeValueAsBytes(mapWitUnsupportedFormatTimestamp), timestamp);
        mMessagePackParser.getTimestampMillis(nMessageWithUnsupportedFormatTimestamp);
    }

    @Test(expected=NullPointerException.class)
    public void testNullTimestamp() throws Exception {
        HashMap<String, Object> mapWitNullTimestamp = new HashMap<String, Object>();
        mapWitNullTimestamp.put("ts", null);
        Message nMessageWithNullTimestamp = new Message("test", 0, 0, null,
                mObjectMapper.writeValueAsBytes(mapWitNullTimestamp), timestamp);
        mMessagePackParser.getTimestampMillis(nMessageWithNullTimestamp);
    }

    @Test
    public void testExtractPartitions() throws Exception {
        String expectedPartition = "dt=2014-07-21";

        String resultSeconds[] = mMessagePackParser.extractPartitions(mMessageWithSecondsTimestamp);
        assertEquals(1, resultSeconds.length);
        assertEquals(expectedPartition, resultSeconds[0]);

        String resultMillis[] = mMessagePackParser.extractPartitions(mMessageWithMillisTimestamp);
        assertEquals(1, resultMillis.length);
        assertEquals(expectedPartition, resultMillis[0]);
    }
}
