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
    private Message mMessageWithNestedTypeAndTimestamp;
    private Message mMessageWithNestedTypeAndCustomTimestamp;
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

        byte messageWithNestedTypeAndTimestamp[] =
                "{\"magic\": \"atMSG\", \"type\": \"DT\", \"headers\": null, \"messageSchemaId\": null, \"messageSchema\": null, \"message\": {\"data\": {\"id\": 347933, \"col_1\": 3175315, \"col_2\": 6950383, \"col_3\": \"2f88df63-4872-11ea-be95-02054bc356132f88df6a-4872-11ea-be95-02054bc356132f88df75-4872-11ea-be95-02054bc356132f88df79-4872-11ea-be95-02054bc35613\", \"col_4\": 1, \"col_5\": \"2016-08-21 08:50:46\", \"col_6\": \"2f88dfc0-4872-11ea-be95-02054bc356132f88dfc4-4872-11ea-be95-02054bc356132f88dfcb-4872-11ea-be95-02054bc356132f88dfcf-4872-11ea-be95-02054bc35613\", \"col_7\": \"2014-05-11 14:54:36\", \"col_8\": \"2f88dff4-4872-11ea-be95-02054bc356132f88dff8-4872-11ea-be95-02054bc356132f88dffd-4872-11ea-be95-02054bc356132f88e000-4872-11ea-be95-02054bc35613\", \"col_9\": \"2f88e013-4872-11ea-be95-02054bc356132f88e016-4872-11ea-be95-02054bc356132f88e01b-4872-11ea-be95-02054bc356132f88e01f-4872-11ea-be95-02054bc35613\", \"col_10\": 65653.3984375, \"col_11\": 82962.5, \"col_12\": \"2f88e04d-4872-11ea-be95-02054bc356132f88e052-4872-11ea-be95-02054bc356132f88e057-4872-11ea-be95-02054bc356132f88e05a-4872-11ea-be95-02054bc35613\", \"col_13\": \"2f88e06e-4872-11ea-be95-02054bc356132f88e071-4872-11ea-be95-02054bc356132f88e076-4872-11ea-be95-02054bc356132f88e07a-4872-11ea-be95-02054bc35613\", \"col_14\": 0, \"col_15\": \"2f88e093-4872-11ea-be95-02054bc356132f88e096-4872-11ea-be95-02054bc356132f88e09b-4872-11ea-be95-02054bc356132f88e09f-4872-11ea-be95-02054bc35613\", \"col_16\": 8541227, \"col_17\": \"2f88e0b6-4872-11ea-be95-02054bc356132f88e0b9-4872-11ea-be95-02054bc356132f88e0be-4872-11ea-be95-02054bc356132f88e0c1-4872-11ea-be95-02054bc35613\", \"col_18\": 4648263, \"col_19\": 7617628, \"col_20\": 4143357, \"col_21\": 7863890, \"col_22\": 6889386, \"col_23\": 855259, \"col_24\": 3608124, \"col_25\": 5474845, \"col_26\": 6549853, \"col_27\": 6324729, \"col_28\": 1974088, \"col_29\": 896242, \"col_30\": 8558940, \"col_31\": 105988, \"col_32\": 4853101, \"col_33\": 3947546, \"col_34\": 5178424, \"col_35\": 4049483, \"col_36\": 4712140, \"col_37\": \"2f88e105-4872-11ea-be95-02054bc356132f88e109-4872-11ea-be95-02054bc356132f88e10f-4872-11ea-be95-02054bc356132f88e113-4872-11ea-be95-02054bc356132f88e117-4872-11ea-be95-02054bc356132f88e11a-4872-11ea-be95-02054bc356132f88e11f-4872-11ea-be95-02054bc356132f88e122-4872-11ea-be95-02054bc356132f88e126-4872-11ea-be95-02054bc356132f88e12a-4872-11ea-be95-02054bc356132f88e12e-4872-11ea-be95-02054bc356132f88e131-4872-11ea-be95-02054bc356132f88e135-4872-11ea-be95-02054bc356132f88e139-4872-11ea-be95-02054bc356132f88e13d-4872-11ea-be95-02054bc356132f88e141-4872-11ea-be95-02054bc356132f88e145-4872-11ea-be95-02054bc35613\", \"col_38\": 1412250, \"col_39\": 2924820, \"col_40\": 387351, \"col_41\": 3162285, \"__database\": \"my_db\", \"__table\": \"table_eighteen\"}, \"beforeData\": null, \"headers\": {\"operation\": \"REFRESH\", \"changeSequence\": \"\", \"timestamp\": \"1405911096000\", \"streamPosition\": \"\", \"transactionId\": \"\", \"changeMask\": null, \"columnMask\": null, \"transactionEventCounter\": null, \"transactionLastEvent\": null}}}".getBytes("UTF-8");
        mMessageWithNestedTypeAndTimestamp = new Message("test", 0, 0, null, messageWithNestedTypeAndTimestamp, timestamp);

        byte messageWithNestedTypeAndCustomTimestamp[] =
                "{\"magic\": \"atMSG\", \"type\": \"DT\", \"headers\": null, \"messageSchemaId\": null, \"messageSchema\": null, \"message\": {\"data\": {\"id\": 347933, \"col_1\": 3175315, \"col_2\": 6950383, \"col_3\": \"2f88df63-4872-11ea-be95-02054bc356132f88df6a-4872-11ea-be95-02054bc356132f88df75-4872-11ea-be95-02054bc356132f88df79-4872-11ea-be95-02054bc35613\", \"col_4\": 1, \"col_5\": \"2016-08-21 08:50:46\", \"col_6\": \"2f88dfc0-4872-11ea-be95-02054bc356132f88dfc4-4872-11ea-be95-02054bc356132f88dfcb-4872-11ea-be95-02054bc356132f88dfcf-4872-11ea-be95-02054bc35613\", \"col_7\": \"2014-05-11 14:54:36\", \"col_8\": \"2f88dff4-4872-11ea-be95-02054bc356132f88dff8-4872-11ea-be95-02054bc356132f88dffd-4872-11ea-be95-02054bc356132f88e000-4872-11ea-be95-02054bc35613\", \"col_9\": \"2f88e013-4872-11ea-be95-02054bc356132f88e016-4872-11ea-be95-02054bc356132f88e01b-4872-11ea-be95-02054bc356132f88e01f-4872-11ea-be95-02054bc35613\", \"col_10\": 65653.3984375, \"col_11\": 82962.5, \"col_12\": \"2f88e04d-4872-11ea-be95-02054bc356132f88e052-4872-11ea-be95-02054bc356132f88e057-4872-11ea-be95-02054bc356132f88e05a-4872-11ea-be95-02054bc35613\", \"col_13\": \"2f88e06e-4872-11ea-be95-02054bc356132f88e071-4872-11ea-be95-02054bc356132f88e076-4872-11ea-be95-02054bc356132f88e07a-4872-11ea-be95-02054bc35613\", \"col_14\": 0, \"col_15\": \"2f88e093-4872-11ea-be95-02054bc356132f88e096-4872-11ea-be95-02054bc356132f88e09b-4872-11ea-be95-02054bc356132f88e09f-4872-11ea-be95-02054bc35613\", \"col_16\": 8541227, \"col_17\": \"2f88e0b6-4872-11ea-be95-02054bc356132f88e0b9-4872-11ea-be95-02054bc356132f88e0be-4872-11ea-be95-02054bc356132f88e0c1-4872-11ea-be95-02054bc35613\", \"col_18\": 4648263, \"col_19\": 7617628, \"col_20\": 4143357, \"col_21\": 7863890, \"col_22\": 6889386, \"col_23\": 855259, \"col_24\": 3608124, \"col_25\": 5474845, \"col_26\": 6549853, \"col_27\": 6324729, \"col_28\": 1974088, \"col_29\": 896242, \"col_30\": 8558940, \"col_31\": 105988, \"col_32\": 4853101, \"col_33\": 3947546, \"col_34\": 5178424, \"col_35\": 4049483, \"col_36\": 4712140, \"col_37\": \"2f88e105-4872-11ea-be95-02054bc356132f88e109-4872-11ea-be95-02054bc356132f88e10f-4872-11ea-be95-02054bc356132f88e113-4872-11ea-be95-02054bc356132f88e117-4872-11ea-be95-02054bc356132f88e11a-4872-11ea-be95-02054bc356132f88e11f-4872-11ea-be95-02054bc356132f88e122-4872-11ea-be95-02054bc356132f88e126-4872-11ea-be95-02054bc356132f88e12a-4872-11ea-be95-02054bc356132f88e12e-4872-11ea-be95-02054bc356132f88e131-4872-11ea-be95-02054bc356132f88e135-4872-11ea-be95-02054bc356132f88e139-4872-11ea-be95-02054bc356132f88e13d-4872-11ea-be95-02054bc356132f88e141-4872-11ea-be95-02054bc356132f88e145-4872-11ea-be95-02054bc35613\", \"col_38\": 1412250, \"col_39\": 2924820, \"col_40\": 387351, \"col_41\": 3162285, \"__database\": \"my_db\", \"__table\": \"table_eighteen\"}, \"beforeData\": null, \"headers\": {\"operation\": \"REFRESH\", \"changeSequence\": \"\", \"timestamp\": \"2020-02-07T00:12:14.000\", \"streamPosition\": \"\", \"transactionId\": \"\", \"changeMask\": null, \"columnMask\": null, \"transactionEventCounter\": null, \"transactionLastEvent\": null}}}".getBytes("UTF-8");
        mMessageWithNestedTypeAndCustomTimestamp = new Message("test", 0, 0, null, messageWithNestedTypeAndCustomTimestamp, timestamp);
   }

    @Test
    public void testExtractTypeAndTimestamp() throws Exception {
        SplitByFieldMessageParser jsonMessageParser = new SplitByFieldMessageParser(mConfig);

        assertEquals(1405911096000l, jsonMessageParser.extractTimestampMillis((JSONObject) JSONValue.parse(mMessageWithTypeAndTimestamp.getPayload())));
        assertEquals(1405911096123l, jsonMessageParser.extractTimestampMillis((JSONObject) JSONValue.parse(mMessageWithoutType.getPayload())));

        assertEquals("event1", jsonMessageParser.extractEventType((JSONObject) JSONValue.parse(mMessageWithTypeAndTimestamp.getPayload())));
        assertEquals("event2", jsonMessageParser.extractEventType((JSONObject) JSONValue.parse(mMessageWithoutTimestamp.getPayload())));
    }

    @Test
    public void testExtractTimestampMillisNoExceptionNoTimestamp() throws Exception {
        SplitByFieldMessageParser jsonMessageParser = new SplitByFieldMessageParser(mConfig);

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
    public void testComplexKeyExtractPartitionsWithCustomDate() throws Exception {
        Mockito.when(mConfig.getMessageSplitFieldName()).thenReturn("message.data.__database");
        Mockito.when(mConfig.getMessageTimestampNameSeparator()).thenReturn(".");
        Mockito.when(mConfig.getMessageTimestampName()).thenReturn("message.headers.timestamp");
        Mockito.when(mConfig.getMessageTimestampInputPattern()).thenReturn("yyyy-MM-dd'T'kk:mm:ss.SSS");

        SplitByFieldMessageParser jsonMessageParser = new SplitByFieldMessageParser(mConfig);

        String expectedEventTypePartition = "my_db";
        String expectedDtPartition = "dt=2020-02-07";

        String result[] = jsonMessageParser.extractPartitions(mMessageWithNestedTypeAndCustomTimestamp);
        assertEquals(2, result.length);
        assertEquals(expectedEventTypePartition, result[0]);
        assertEquals(expectedDtPartition, result[1]);
    }

    @Test
    public void testComplexKeyExtractPartitions() throws Exception {
        Mockito.when(mConfig.getMessageSplitFieldName()).thenReturn("message.data.__database");
        Mockito.when(mConfig.getMessageTimestampNameSeparator()).thenReturn(".");
        Mockito.when(mConfig.getMessageTimestampName()).thenReturn("message.headers.timestamp");

        SplitByFieldMessageParser jsonMessageParser = new SplitByFieldMessageParser(mConfig);

        String expectedEventTypePartition = "my_db";
        String expectedDtPartition = "dt=2014-07-21";

        String result[] = jsonMessageParser.extractPartitions(mMessageWithNestedTypeAndTimestamp);
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
