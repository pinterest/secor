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

import com.pinterest.secor.common.*;
import com.pinterest.secor.message.Message;

import java.util.Arrays;
import java.util.List;
import junit.framework.TestCase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;

@RunWith(PowerMockRunner.class)
public class JsonMessageParserTest extends TestCase {

    private SecorConfig mConfig;
    private Message mMessageWithSecondsTimestamp;
    private Message mMessageWithMillisTimestamp;
    private Message mMessageWithMillisFloatTimestamp;
    private Message mMessageWithoutTimestamp;

    @Override
    public void setUp() throws Exception {
        mConfig = Mockito.mock(SecorConfig.class);
        Mockito.when(mConfig.getMessageTimestampName()).thenReturn("timestamp");

        byte messageWithSecondsTimestamp[] =
                "{\"timestamp\":\"1405911096\",\"id\":0,\"guid\":\"0436b17b-e78a-4e82-accf-743bf1f0b884\",\"isActive\":false,\"balance\":\"$3,561.87\",\"picture\":\"http://placehold.it/32x32\",\"age\":23,\"eyeColor\":\"green\",\"name\":\"Mercedes Brewer\",\"gender\":\"female\",\"company\":\"MALATHION\",\"email\":\"mercedesbrewer@malathion.com\",\"phone\":\"+1 (848) 471-3000\",\"address\":\"786 Gilmore Court, Brule, Maryland, 3200\",\"about\":\"Quis nostrud Lorem deserunt esse ut reprehenderit aliqua nisi et sunt mollit est. Cupidatat incididunt minim anim eiusmod culpa elit est dolor ullamco. Aliqua cillum eiusmod ullamco nostrud Lorem sit amet Lorem aliquip esse esse velit.\\r\\n\",\"registered\":\"2014-01-14T13:07:28 +08:00\",\"latitude\":47.672012,\"longitude\":102.788623,\"tags\":[\"amet\",\"amet\",\"dolore\",\"eu\",\"qui\",\"fugiat\",\"laborum\"],\"friends\":[{\"id\":0,\"name\":\"Rebecca Hardy\"},{\"id\":1,\"name\":\"Sutton Briggs\"},{\"id\":2,\"name\":\"Dena Campos\"}],\"greeting\":\"Hello, Mercedes Brewer! You have 7 unread messages.\",\"favoriteFruit\":\"strawberry\"}".getBytes("UTF-8");
        mMessageWithSecondsTimestamp = new Message("test", 0, 0, messageWithSecondsTimestamp);

        byte messageWithMillisTimestamp[] =
                "{\"timestamp\":\"1405911096123\",\"id\":0,\"guid\":\"0436b17b-e78a-4e82-accf-743bf1f0b884\",\"isActive\":false,\"balance\":\"$3,561.87\",\"picture\":\"http://placehold.it/32x32\",\"age\":23,\"eyeColor\":\"green\",\"name\":\"Mercedes Brewer\",\"gender\":\"female\",\"company\":\"MALATHION\",\"email\":\"mercedesbrewer@malathion.com\",\"phone\":\"+1 (848) 471-3000\",\"address\":\"786 Gilmore Court, Brule, Maryland, 3200\",\"about\":\"Quis nostrud Lorem deserunt esse ut reprehenderit aliqua nisi et sunt mollit est. Cupidatat incididunt minim anim eiusmod culpa elit est dolor ullamco. Aliqua cillum eiusmod ullamco nostrud Lorem sit amet Lorem aliquip esse esse velit.\\r\\n\",\"registered\":\"2014-01-14T13:07:28 +08:00\",\"latitude\":47.672012,\"longitude\":102.788623,\"tags\":[\"amet\",\"amet\",\"dolore\",\"eu\",\"qui\",\"fugiat\",\"laborum\"],\"friends\":[{\"id\":0,\"name\":\"Rebecca Hardy\"},{\"id\":1,\"name\":\"Sutton Briggs\"},{\"id\":2,\"name\":\"Dena Campos\"}],\"greeting\":\"Hello, Mercedes Brewer! You have 7 unread messages.\",\"favoriteFruit\":\"strawberry\"}".getBytes("UTF-8");
        mMessageWithMillisTimestamp = new Message("test", 0, 0, messageWithMillisTimestamp);

        byte messageWithMillisFloatTimestamp[] =
                "{\"timestamp\":\"1405911096123.0\",\"id\":0,\"guid\":\"0436b17b-e78a-4e82-accf-743bf1f0b884\",\"isActive\":false,\"balance\":\"$3,561.87\",\"picture\":\"http://placehold.it/32x32\",\"age\":23,\"eyeColor\":\"green\",\"name\":\"Mercedes Brewer\",\"gender\":\"female\",\"company\":\"MALATHION\",\"email\":\"mercedesbrewer@malathion.com\",\"phone\":\"+1 (848) 471-3000\",\"address\":\"786 Gilmore Court, Brule, Maryland, 3200\",\"about\":\"Quis nostrud Lorem deserunt esse ut reprehenderit aliqua nisi et sunt mollit est. Cupidatat incididunt minim anim eiusmod culpa elit est dolor ullamco. Aliqua cillum eiusmod ullamco nostrud Lorem sit amet Lorem aliquip esse esse velit.\\r\\n\",\"registered\":\"2014-01-14T13:07:28 +08:00\",\"latitude\":47.672012,\"longitude\":102.788623,\"tags\":[\"amet\",\"amet\",\"dolore\",\"eu\",\"qui\",\"fugiat\",\"laborum\"],\"friends\":[{\"id\":0,\"name\":\"Rebecca Hardy\"},{\"id\":1,\"name\":\"Sutton Briggs\"},{\"id\":2,\"name\":\"Dena Campos\"}],\"greeting\":\"Hello, Mercedes Brewer! You have 7 unread messages.\",\"favoriteFruit\":\"strawberry\"}".getBytes("UTF-8");
        mMessageWithMillisFloatTimestamp = new Message("test", 0, 0, messageWithMillisFloatTimestamp);

        byte messageWithoutTimestamp[] =
                "{\"id\":0,\"guid\":\"0436b17b-e78a-4e82-accf-743bf1f0b884\",\"isActive\":false,\"balance\":\"$3,561.87\",\"picture\":\"http://placehold.it/32x32\",\"age\":23,\"eyeColor\":\"green\",\"name\":\"Mercedes Brewer\",\"gender\":\"female\",\"company\":\"MALATHION\",\"email\":\"mercedesbrewer@malathion.com\",\"phone\":\"+1 (848) 471-3000\",\"address\":\"786 Gilmore Court, Brule, Maryland, 3200\",\"about\":\"Quis nostrud Lorem deserunt esse ut reprehenderit aliqua nisi et sunt mollit est. Cupidatat incididunt minim anim eiusmod culpa elit est dolor ullamco. Aliqua cillum eiusmod ullamco nostrud Lorem sit amet Lorem aliquip esse esse velit.\\r\\n\",\"registered\":\"2014-01-14T13:07:28 +08:00\",\"latitude\":47.672012,\"longitude\":102.788623,\"tags\":[\"amet\",\"amet\",\"dolore\",\"eu\",\"qui\",\"fugiat\",\"laborum\"],\"friends\":[{\"id\":0,\"name\":\"Rebecca Hardy\"},{\"id\":1,\"name\":\"Sutton Briggs\"},{\"id\":2,\"name\":\"Dena Campos\"}],\"greeting\":\"Hello, Mercedes Brewer! You have 7 unread messages.\",\"favoriteFruit\":\"strawberry\"}".getBytes("UTF-8");
        mMessageWithoutTimestamp = new Message("test", 0, 0, messageWithoutTimestamp);
    }

    @Test
    public void testExtractTimestampMillis() throws Exception {
        JsonMessageParser jsonMessageParser = new JsonMessageParser(mConfig);

        assertEquals(1405911096000l, jsonMessageParser.extractTimestampMillis(mMessageWithSecondsTimestamp));
        assertEquals(1405911096123l, jsonMessageParser.extractTimestampMillis(mMessageWithMillisTimestamp));
        assertEquals(1405911096123l, jsonMessageParser.extractTimestampMillis(mMessageWithMillisFloatTimestamp));

        // Return 0 if there's no timestamp, for any reason.

        assertEquals(0l, jsonMessageParser.extractTimestampMillis(mMessageWithoutTimestamp));
    }

    @Test(expected=ClassCastException.class)
    public void testExtractTimestampMillisException1() throws Exception {
        JsonMessageParser jsonMessageParser = new JsonMessageParser(mConfig);

        byte emptyBytes1[] = {};
        jsonMessageParser.extractTimestampMillis(new Message("test", 0, 0, emptyBytes1));
    }

    @Test(expected=ClassCastException.class)
    public void testExtractTimestampMillisException2() throws Exception {
        JsonMessageParser jsonMessageParser = new JsonMessageParser(mConfig);

        byte emptyBytes2[] = "".getBytes();
        jsonMessageParser.extractTimestampMillis(new Message("test", 0, 0, emptyBytes2));
    }

    @Test
    public void testExtractPartitions() throws Exception {
        JsonMessageParser jsonMessageParser = new JsonMessageParser(mConfig);

        String expectedPartition = "dt=2014-07-21";

        String resultSeconds[] = jsonMessageParser.extractPartitions(mMessageWithSecondsTimestamp);
        assertEquals(1, resultSeconds.length);
        assertEquals(expectedPartition, resultSeconds[0]);

        String resultMillis[] = jsonMessageParser.extractPartitions(mMessageWithMillisTimestamp);
        assertEquals(1, resultMillis.length);
        assertEquals(expectedPartition, resultMillis[0]);
    }

    @Test
    public void testExtractHourlyPartitions() throws Exception {
        Mockito.when(TimestampedMessageParser.usingHourly(mConfig)).thenReturn(true);
        JsonMessageParser jsonMessageParser = new JsonMessageParser(mConfig);

        String expectedDtPartition = "dt=2014-07-21";
        String expectedHrPartition = "hr=02";

        String resultSeconds[] = jsonMessageParser.extractPartitions(mMessageWithSecondsTimestamp);
        assertEquals(2, resultSeconds.length);
        assertEquals(expectedDtPartition, resultSeconds[0]);
        assertEquals(expectedHrPartition, resultSeconds[1]);

        String resultMillis[] = jsonMessageParser.extractPartitions(mMessageWithMillisTimestamp);
        assertEquals(2, resultMillis.length);
        assertEquals(expectedDtPartition, resultMillis[0]);
        assertEquals(expectedHrPartition, resultMillis[1]);
    }

    @Test
    public void testDailyGetFinalizedUptoPartitions() throws Exception {
        JsonMessageParser jsonMessageParser = new JsonMessageParser(mConfig);

        List<Message> lastMessages = new ArrayList<Message>();
        lastMessages.add(mMessageWithSecondsTimestamp);
        List<Message> committedMessages = new ArrayList<Message>();
        committedMessages.add(mMessageWithMillisTimestamp);
        String uptoPartitions[] = jsonMessageParser.getFinalizedUptoPartitions(lastMessages,
            committedMessages);
        assertEquals(1, uptoPartitions.length);
        assertEquals("dt=2014-07-21", uptoPartitions[0]);

        String[] previous = jsonMessageParser.getPreviousPartitions(uptoPartitions);
        assertEquals(1, previous.length);
        assertEquals("dt=2014-07-20", previous[0]);
    }

    @Test
    public void testHourlyGetFinalizedUptoPartitions() throws Exception {
        Mockito.when(TimestampedMessageParser.usingHourly(mConfig)).thenReturn(true);
        JsonMessageParser jsonMessageParser = new JsonMessageParser(mConfig);

        List<Message> lastMessages = new ArrayList<Message>();
        lastMessages.add(mMessageWithSecondsTimestamp);
        List<Message> committedMessages = new ArrayList<Message>();
        committedMessages.add(mMessageWithMillisTimestamp);
        String uptoPartitions[] = jsonMessageParser.getFinalizedUptoPartitions(lastMessages,
            committedMessages);
        assertEquals(2, uptoPartitions.length);
        assertEquals("dt=2014-07-21", uptoPartitions[0]);
        assertEquals("hr=01", uptoPartitions[1]);

        String[][] expectedPartitions = new String[][] {
          new String[]{"dt=2014-07-21", "hr=00"},
          new String[]{"dt=2014-07-20"},  // there is day partition for previous day
          new String[]{"dt=2014-07-20", "hr=23"},
          new String[]{"dt=2014-07-20", "hr=22"},
          new String[]{"dt=2014-07-20", "hr=21"},
          new String[]{"dt=2014-07-20", "hr=20"},
          new String[]{"dt=2014-07-20", "hr=19"},
          new String[]{"dt=2014-07-20", "hr=18"},
          new String[]{"dt=2014-07-20", "hr=17"},
          new String[]{"dt=2014-07-20", "hr=16"},
          new String[]{"dt=2014-07-20", "hr=15"},
          new String[]{"dt=2014-07-20", "hr=14"},
          new String[]{"dt=2014-07-20", "hr=13"},
          new String[]{"dt=2014-07-20", "hr=12"},
          new String[]{"dt=2014-07-20", "hr=11"},
          new String[]{"dt=2014-07-20", "hr=10"},
          new String[]{"dt=2014-07-20", "hr=09"},
          new String[]{"dt=2014-07-20", "hr=08"},
          new String[]{"dt=2014-07-20", "hr=07"},
          new String[]{"dt=2014-07-20", "hr=06"},
          new String[]{"dt=2014-07-20", "hr=05"},
          new String[]{"dt=2014-07-20", "hr=04"},
          new String[]{"dt=2014-07-20", "hr=03"},
          new String[]{"dt=2014-07-20", "hr=02"},
          new String[]{"dt=2014-07-20", "hr=01"},
          new String[]{"dt=2014-07-20", "hr=00"},
          new String[]{"dt=2014-07-19"},  // there is day partition for 2nd last day
          new String[]{"dt=2014-07-19", "hr=23"}
        };

        String[] partitions = uptoPartitions;
        List<String[]> partitionsList = new ArrayList<String[]>();
        for (int i = 0; i < 28; i++ ) {
            String[] previous = jsonMessageParser.getPreviousPartitions(partitions);
            partitionsList.add(previous);
            partitions = previous;
        }

        assertEquals(partitionsList.size(), expectedPartitions.length);
        for (int i = 0; i < partitionsList.size(); i++) {
            List<String> expectedPartition = Arrays.asList(expectedPartitions[i]);
            List<String> retrievedPartition = Arrays.asList(partitionsList.get(i));
            assertEquals(expectedPartition, retrievedPartition);
        }
    }

}
