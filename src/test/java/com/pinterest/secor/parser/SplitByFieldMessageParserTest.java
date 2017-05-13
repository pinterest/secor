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

import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.message.Message;
import junit.framework.TestCase;
import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

@RunWith(PowerMockRunner.class)
public class SplitByFieldMessageParserTest extends TestCase {

    private SecorConfig mConfig;
    private Message mMessageWithTypeAndTimestamp;
    private Message mMessageWithoutTimestamp;
    private Message mMessageWithoutType;
    private long timestamp;

    @Override
    public void setUp() throws Exception {
        mConfig = Mockito.mock(SecorConfig.class);
        Mockito.when(mConfig.getMessageSplitFieldName()).thenReturn("type");
        Mockito.when(mConfig.getMessageTimestampName()).thenReturn("timestamp");
        Mockito.when(mConfig.getFinalizerDelaySeconds()).thenReturn(3600);
        Mockito.when(mConfig.getTimeZone()).thenReturn(TimeZone.getTimeZone("UTC"));

        Mockito.when(TimestampedMessageParser.usingDateFormat(mConfig)).thenReturn("yyyy-MM-dd");
        Mockito.when(TimestampedMessageParser.usingHourFormat(mConfig)).thenReturn("HH");
        Mockito.when(TimestampedMessageParser.usingMinuteFormat(mConfig)).thenReturn("mm");
        Mockito.when(TimestampedMessageParser.usingDatePrefix(mConfig)).thenReturn("dt=");
        Mockito.when(TimestampedMessageParser.usingHourPrefix(mConfig)).thenReturn("hr=");
        Mockito.when(TimestampedMessageParser.usingMinutePrefix(mConfig)).thenReturn("min=");

        timestamp = System.currentTimeMillis();

        byte messageWithTypeAndTimestamp[] =
                "{\"type\":\"event1\",\"timestamp\":\"1405911096000\",\"id\":0,\"guid\":\"0436b17b-e78a-4e82-accf-743bf1f0b884\",\"isActive\":false,\"balance\":\"$3,561.87\",\"picture\":\"http://placehold.it/32x32\",\"age\":23,\"eyeColor\":\"green\",\"name\":\"Mercedes Brewer\",\"gender\":\"female\",\"company\":\"MALATHION\",\"email\":\"mercedesbrewer@malathion.com\",\"phone\":\"+1 (848) 471-3000\",\"address\":\"786 Gilmore Court, Brule, Maryland, 3200\",\"about\":\"Quis nostrud Lorem deserunt esse ut reprehenderit aliqua nisi et sunt mollit est. Cupidatat incididunt minim anim eiusmod culpa elit est dolor ullamco. Aliqua cillum eiusmod ullamco nostrud Lorem sit amet Lorem aliquip esse esse velit.\\r\\n\",\"registered\":\"2014-01-14T13:07:28 +08:00\",\"latitude\":47.672012,\"longitude\":102.788623,\"tags\":[\"amet\",\"amet\",\"dolore\",\"eu\",\"qui\",\"fugiat\",\"laborum\"],\"friends\":[{\"id\":0,\"name\":\"Rebecca Hardy\"},{\"id\":1,\"name\":\"Sutton Briggs\"},{\"id\":2,\"name\":\"Dena Campos\"}],\"greeting\":\"Hello, Mercedes Brewer! You have 7 unread messages.\",\"favoriteFruit\":\"strawberry\"}".getBytes("UTF-8");
        mMessageWithTypeAndTimestamp = new Message("test", 0, 0, null, messageWithTypeAndTimestamp, timestamp);

        byte messageWithoutTimestamp[] =
                "{\"type\":\"event2\",\"id\":0,\"guid\":\"0436b17b-e78a-4e82-accf-743bf1f0b884\",\"isActive\":false,\"balance\":\"$3,561.87\",\"picture\":\"http://placehold.it/32x32\",\"age\":23,\"eyeColor\":\"green\",\"name\":\"Mercedes Brewer\",\"gender\":\"female\",\"company\":\"MALATHION\",\"email\":\"mercedesbrewer@malathion.com\",\"phone\":\"+1 (848) 471-3000\",\"address\":\"786 Gilmore Court, Brule, Maryland, 3200\",\"about\":\"Quis nostrud Lorem deserunt esse ut reprehenderit aliqua nisi et sunt mollit est. Cupidatat incididunt minim anim eiusmod culpa elit est dolor ullamco. Aliqua cillum eiusmod ullamco nostrud Lorem sit amet Lorem aliquip esse esse velit.\\r\\n\",\"registered\":\"2014-01-14T13:07:28 +08:00\",\"latitude\":47.672012,\"longitude\":102.788623,\"tags\":[\"amet\",\"amet\",\"dolore\",\"eu\",\"qui\",\"fugiat\",\"laborum\"],\"friends\":[{\"id\":0,\"name\":\"Rebecca Hardy\"},{\"id\":1,\"name\":\"Sutton Briggs\"},{\"id\":2,\"name\":\"Dena Campos\"}],\"greeting\":\"Hello, Mercedes Brewer! You have 7 unread messages.\",\"favoriteFruit\":\"strawberry\"}".getBytes("UTF-8");
        mMessageWithoutTimestamp = new Message("test", 0, 0, null, messageWithoutTimestamp, timestamp);

        byte messageWithoutType[] =
                "{\"timestamp\":\"1405911096123\",\"id\":0,\"guid\":\"0436b17b-e78a-4e82-accf-743bf1f0b884\",\"isActive\":false,\"balance\":\"$3,561.87\",\"picture\":\"http://placehold.it/32x32\",\"age\":23,\"eyeColor\":\"green\",\"name\":\"Mercedes Brewer\",\"gender\":\"female\",\"company\":\"MALATHION\",\"email\":\"mercedesbrewer@malathion.com\",\"phone\":\"+1 (848) 471-3000\",\"address\":\"786 Gilmore Court, Brule, Maryland, 3200\",\"about\":\"Quis nostrud Lorem deserunt esse ut reprehenderit aliqua nisi et sunt mollit est. Cupidatat incididunt minim anim eiusmod culpa elit est dolor ullamco. Aliqua cillum eiusmod ullamco nostrud Lorem sit amet Lorem aliquip esse esse velit.\\r\\n\",\"registered\":\"2014-01-14T13:07:28 +08:00\",\"latitude\":47.672012,\"longitude\":102.788623,\"tags\":[\"amet\",\"amet\",\"dolore\",\"eu\",\"qui\",\"fugiat\",\"laborum\"],\"friends\":[{\"id\":0,\"name\":\"Rebecca Hardy\"},{\"id\":1,\"name\":\"Sutton Briggs\"},{\"id\":2,\"name\":\"Dena Campos\"}],\"greeting\":\"Hello, Mercedes Brewer! You have 7 unread messages.\",\"favoriteFruit\":\"strawberry\"}".getBytes("UTF-8");
        mMessageWithoutType = new Message("test", 0, 0, null, messageWithoutType, timestamp);
    }

    @Test
    public void testExtractTypeAndTimestamp() throws Exception {
        SplitByFieldMessageParser jsonMessageParser = new SplitByFieldMessageParser(mConfig);

        assertEquals(1405911096000l, jsonMessageParser.extractTimestampMillis((JSONObject) JSONValue.parse(mMessageWithTypeAndTimestamp.getPayload())));
        assertEquals(1405911096123l, jsonMessageParser.extractTimestampMillis((JSONObject) JSONValue.parse(mMessageWithoutType.getPayload())));

        assertEquals("event1", jsonMessageParser.extractEventType((JSONObject) JSONValue.parse(mMessageWithTypeAndTimestamp.getPayload())));
        assertEquals("event2", jsonMessageParser.extractEventType((JSONObject) JSONValue.parse(mMessageWithoutTimestamp.getPayload())));
    }

    @Test(expected = RuntimeException.class)
    public void testExtractTimestampMillisExceptionNoTimestamp() throws Exception {
        SplitByFieldMessageParser jsonMessageParser = new SplitByFieldMessageParser(mConfig);

        // Throws exception if there's no timestamp, for any reason.
        jsonMessageParser.extractTimestampMillis((JSONObject) JSONValue.parse(mMessageWithoutTimestamp.getPayload()));
    }

    @Test(expected = ClassCastException.class)
    public void testExtractTimestampMillisException1() throws Exception {
        SplitByFieldMessageParser jsonMessageParser = new SplitByFieldMessageParser(mConfig);

        byte emptyBytes1[] = {};
        jsonMessageParser.extractTimestampMillis((JSONObject) JSONValue.parse(emptyBytes1));
    }

    @Test(expected = ClassCastException.class)
    public void testExtractTimestampMillisException2() throws Exception {
        SplitByFieldMessageParser jsonMessageParser = new SplitByFieldMessageParser(mConfig);

        byte emptyBytes2[] = "".getBytes();
        jsonMessageParser.extractTimestampMillis((JSONObject) JSONValue.parse(emptyBytes2));
    }

    @Test(expected = RuntimeException.class)
    public void testExtractTimestampMillisExceptionNoType() throws Exception {
        SplitByFieldMessageParser jsonMessageParser = new SplitByFieldMessageParser(mConfig);

        // Throws exception if there's no timestamp, for any reason.
        jsonMessageParser.extractEventType((JSONObject) JSONValue.parse(mMessageWithoutType.getPayload()));
    }

    @Test
    public void testExtractPartitions() throws Exception {
        SplitByFieldMessageParser jsonMessageParser = new SplitByFieldMessageParser(mConfig);

        String expectedEventTypePartition = "event1";
        String expectedDtPartition = "dt=2014-07-21";

        String result[] = jsonMessageParser.extractPartitions(mMessageWithTypeAndTimestamp);
        assertEquals(2, result.length);
        assertEquals(expectedEventTypePartition, result[0]);
        assertEquals(expectedDtPartition, result[1]);
    }

    @Test
    public void testExtractHourlyPartitions() throws Exception {
        Mockito.when(TimestampedMessageParser.usingHourly(mConfig)).thenReturn(true);
        SplitByFieldMessageParser jsonMessageParser = new SplitByFieldMessageParser(mConfig);

        String expectedEventTypePartition = "event1";
        String expectedDtPartition = "dt=2014-07-21";
        String expectedHrPartition = "hr=02";

        String result[] = jsonMessageParser.extractPartitions(mMessageWithTypeAndTimestamp);
        assertEquals(3, result.length);
        assertEquals(expectedEventTypePartition, result[0]);
        assertEquals(expectedDtPartition, result[1]);
        assertEquals(expectedHrPartition, result[2]);
    }

    @Test
    public void testExtractHourlyPartitionsForNonUTCTimezone() throws Exception {
        Mockito.when(mConfig.getTimeZone()).thenReturn(TimeZone.getTimeZone("IST"));
        Mockito.when(TimestampedMessageParser.usingHourly(mConfig)).thenReturn(true);
        SplitByFieldMessageParser jsonMessageParser = new SplitByFieldMessageParser(mConfig);

        String expectedEventTypePartition = "event1";
        String expectedDtPartition = "dt=2014-07-21";
        String expectedHrPartition = "hr=08";

        String result[] = jsonMessageParser.extractPartitions(mMessageWithTypeAndTimestamp);
        assertEquals(3, result.length);
        assertEquals(expectedEventTypePartition, result[0]);
        assertEquals(expectedDtPartition, result[1]);
        assertEquals(expectedHrPartition, result[2]);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetFinalizedUptoPartitions() throws Exception {
        SplitByFieldMessageParser jsonMessageParser = new SplitByFieldMessageParser(mConfig);

        List<Message> lastMessages = new ArrayList<Message>();
        lastMessages.add(mMessageWithTypeAndTimestamp);
        List<Message> committedMessages = new ArrayList<Message>();
        committedMessages.add(mMessageWithTypeAndTimestamp);

        jsonMessageParser.getFinalizedUptoPartitions(lastMessages, committedMessages);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetPreviousPartitions() throws Exception {
        SplitByFieldMessageParser jsonMessageParser = new SplitByFieldMessageParser(mConfig);

        String partitions[] = {"event1", "dt=2014-07-21"};
        jsonMessageParser.getPreviousPartitions(partitions);
    }

}
