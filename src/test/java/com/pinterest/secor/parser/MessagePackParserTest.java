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

@RunWith(PowerMockRunner.class)
public class MessagePackParserTest extends TestCase {

    private MessagePackParser mMessagePackParser;
    private Message mMessageWithSecondsTimestamp;
    private Message mMessageWithMillisTimestamp;
    private Message mMessageWithMillisFloatTimestamp;
    private Message mMessageWithMillisStringTimestamp;
    private ObjectMapper mObjectMapper;

    @Override
    public void setUp() throws Exception {
        SecorConfig mConfig = Mockito.mock(SecorConfig.class);
        Mockito.when(mConfig.getMessageTimestampName()).thenReturn("ts");
        mMessagePackParser = new MessagePackParser(mConfig);
        mObjectMapper = new ObjectMapper(new MessagePackFactory());

        HashMap<String, Object> mapWithSecondTimestamp = new HashMap<String, Object>();
        mapWithSecondTimestamp.put("ts", 1405970352);
        mMessageWithSecondsTimestamp = new Message("test", 0, 0,
                mObjectMapper.writeValueAsBytes(mapWithSecondTimestamp));

        HashMap<String, Object> mapWithMillisTimestamp = new HashMap<String, Object>();
        mapWithMillisTimestamp.put("ts", 1405970352123l);
        mapWithMillisTimestamp.put("isActive", true);
        mapWithMillisTimestamp.put("email", "alice@example.com");
        mapWithMillisTimestamp.put("age", 27);
        mMessageWithMillisTimestamp = new Message("test", 0, 0,
                mObjectMapper.writeValueAsBytes(mapWithMillisTimestamp));


        HashMap<String, Object> mapWithMillisFloatTimestamp = new HashMap<String, Object>();
        mapWithMillisFloatTimestamp.put("ts", 1405970352123.0);
        mapWithMillisFloatTimestamp.put("isActive", false);
        mapWithMillisFloatTimestamp.put("email", "bob@example.com");
        mapWithMillisFloatTimestamp.put("age", 35);
        mMessageWithMillisFloatTimestamp = new Message("test", 0, 0,
                mObjectMapper.writeValueAsBytes(mapWithMillisFloatTimestamp));

        HashMap<String, Object> mapWithMillisStringTimestamp = new HashMap<String, Object>();
        mapWithMillisStringTimestamp.put("ts", "1405970352123");
        mapWithMillisStringTimestamp.put("isActive", null);
        mapWithMillisStringTimestamp.put("email", "charlie@example.com");
        mapWithMillisStringTimestamp.put("age", 67);
        mMessageWithMillisStringTimestamp = new Message("test", 0, 0,
                mObjectMapper.writeValueAsBytes(mapWithMillisStringTimestamp));

    }

    @Test
    public void testExtractTimestampMillis() throws Exception {
        assertEquals(1405970352000l, mMessagePackParser.extractTimestampMillis(
                mMessageWithSecondsTimestamp));
        assertEquals(1405970352123l, mMessagePackParser.extractTimestampMillis(
                mMessageWithMillisTimestamp));
        assertEquals(1405970352123l, mMessagePackParser.extractTimestampMillis(
                mMessageWithMillisFloatTimestamp));
        assertEquals(1405970352123l, mMessagePackParser.extractTimestampMillis(
                mMessageWithMillisStringTimestamp));
    }

    @Test(expected=NullPointerException.class)
    public void testMissingTimestamp() throws Exception {
        HashMap<String, Object> mapWithoutTimestamp = new HashMap<String, Object>();
        mapWithoutTimestamp.put("email", "mary@example.com");
        Message nMessageWithoutTimestamp = new Message("test", 0, 0,
                mObjectMapper.writeValueAsBytes(mapWithoutTimestamp));
        mMessagePackParser.extractTimestampMillis(nMessageWithoutTimestamp);
    }

    @Test(expected=NumberFormatException.class)
    public void testUnsupportedTimestampFormat() throws Exception {
        HashMap<String, Object> mapWitUnsupportedFormatTimestamp = new HashMap<String, Object>();
        mapWitUnsupportedFormatTimestamp.put("ts", "2014-11-14T18:12:52.878Z");
        Message nMessageWithUnsupportedFormatTimestamp = new Message("test", 0, 0,
                mObjectMapper.writeValueAsBytes(mapWitUnsupportedFormatTimestamp));
        mMessagePackParser.extractTimestampMillis(nMessageWithUnsupportedFormatTimestamp);
    }

    @Test(expected=NullPointerException.class)
    public void testNullTimestamp() throws Exception {
        HashMap<String, Object> mapWitNullTimestamp = new HashMap<String, Object>();
        mapWitNullTimestamp.put("ts", null);
        Message nMessageWithNullTimestamp = new Message("test", 0, 0,
                mObjectMapper.writeValueAsBytes(mapWitNullTimestamp));
        mMessagePackParser.extractTimestampMillis(nMessageWithNullTimestamp);
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
