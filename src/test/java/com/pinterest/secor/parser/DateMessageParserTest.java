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

import junit.framework.TestCase;

import java.util.TimeZone;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.stubbing.OngoingStubbing;
import org.powermock.modules.junit4.PowerMockRunner;

import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.message.Message;

@RunWith(PowerMockRunner.class)
public class DateMessageParserTest extends TestCase {

    private SecorConfig mConfig;
    private Message mFormat1;
    private Message mFormat2;
    private Message mFormat3;
    private Message mInvalidDate;
    private Message mISOFormat;
    private Message mNanosecondISOFormat;
    private Message mNestedISOFormat;
    private OngoingStubbing<String> getTimestamp;

    @Override
    public void setUp() throws Exception {
        mConfig = Mockito.mock(SecorConfig.class);
        Mockito.when(mConfig.getTimeZone()).thenReturn(TimeZone.getTimeZone("UTC"));

        byte format1[] = "{\"timestamp\":\"2014-07-30 10:53:20\",\"id\":0,\"guid\":\"0436b17b-e78a-4e82-accf-743bf1f0b884\",\"isActive\":false,\"balance\":\"$3,561.87\",\"picture\":\"http://placehold.it/32x32\",\"age\":23,\"eyeColor\":\"green\",\"name\":\"Mercedes Brewer\",\"gender\":\"female\",\"company\":\"MALATHION\",\"email\":\"mercedesbrewer@malathion.com\",\"phone\":\"+1 (848) 471-3000\",\"address\":\"786 Gilmore Court, Brule, Maryland, 3200\",\"about\":\"Quis nostrud Lorem deserunt esse ut reprehenderit aliqua nisi et sunt mollit est. Cupidatat incididunt minim anim eiusmod culpa elit est dolor ullamco. Aliqua cillum eiusmod ullamco nostrud Lorem sit amet Lorem aliquip esse esse velit.\\r\\n\",\"registered\":\"2014-01-14T13:07:28 +08:00\",\"latitude\":47.672012,\"longitude\":102.788623,\"tags\":[\"amet\",\"amet\",\"dolore\",\"eu\",\"qui\",\"fugiat\",\"laborum\"],\"friends\":[{\"id\":0,\"name\":\"Rebecca Hardy\"},{\"id\":1,\"name\":\"Sutton Briggs\"},{\"id\":2,\"name\":\"Dena Campos\"}],\"greeting\":\"Hello, Mercedes Brewer! You have 7 unread messages.\",\"favoriteFruit\":\"strawberry\"}"
            .getBytes("UTF-8");
        mFormat1 = new Message("test", 0, 0, null, format1);

        byte format2[] = "{\"timestamp\":\"2014/10/25\",\"id\":0,\"guid\":\"0436b17b-e78a-4e82-accf-743bf1f0b884\",\"isActive\":false,\"balance\":\"$3,561.87\",\"picture\":\"http://placehold.it/32x32\",\"age\":23,\"eyeColor\":\"green\",\"name\":\"Mercedes Brewer\",\"gender\":\"female\",\"company\":\"MALATHION\",\"email\":\"mercedesbrewer@malathion.com\",\"phone\":\"+1 (848) 471-3000\",\"address\":\"786 Gilmore Court, Brule, Maryland, 3200\",\"about\":\"Quis nostrud Lorem deserunt esse ut reprehenderit aliqua nisi et sunt mollit est. Cupidatat incididunt minim anim eiusmod culpa elit est dolor ullamco. Aliqua cillum eiusmod ullamco nostrud Lorem sit amet Lorem aliquip esse esse velit.\\r\\n\",\"registered\":\"2014-01-14T13:07:28 +08:00\",\"latitude\":47.672012,\"longitude\":102.788623,\"tags\":[\"amet\",\"amet\",\"dolore\",\"eu\",\"qui\",\"fugiat\",\"laborum\"],\"friends\":[{\"id\":0,\"name\":\"Rebecca Hardy\"},{\"id\":1,\"name\":\"Sutton Briggs\"},{\"id\":2,\"name\":\"Dena Campos\"}],\"greeting\":\"Hello, Mercedes Brewer! You have 7 unread messages.\",\"favoriteFruit\":\"strawberry\"}"
            .getBytes("UTF-8");
        mFormat2 = new Message("test", 0, 0, null, format2);

        byte format3[] = "{\"timestamp\":\"02001.July.04 AD 12:08 PM\",\"id\":0,\"guid\":\"0436b17b-e78a-4e82-accf-743bf1f0b884\",\"isActive\":false,\"balance\":\"$3,561.87\",\"picture\":\"http://placehold.it/32x32\",\"age\":23,\"eyeColor\":\"green\",\"name\":\"Mercedes Brewer\",\"gender\":\"female\",\"company\":\"MALATHION\",\"email\":\"mercedesbrewer@malathion.com\",\"phone\":\"+1 (848) 471-3000\",\"address\":\"786 Gilmore Court, Brule, Maryland, 3200\",\"about\":\"Quis nostrud Lorem deserunt esse ut reprehenderit aliqua nisi et sunt mollit est. Cupidatat incididunt minim anim eiusmod culpa elit est dolor ullamco. Aliqua cillum eiusmod ullamco nostrud Lorem sit amet Lorem aliquip esse esse velit.\\r\\n\",\"registered\":\"2014-01-14T13:07:28 +08:00\",\"latitude\":47.672012,\"longitude\":102.788623,\"tags\":[\"amet\",\"amet\",\"dolore\",\"eu\",\"qui\",\"fugiat\",\"laborum\"],\"friends\":[{\"id\":0,\"name\":\"Rebecca Hardy\"},{\"id\":1,\"name\":\"Sutton Briggs\"},{\"id\":2,\"name\":\"Dena Campos\"}],\"greeting\":\"Hello, Mercedes Brewer! You have 7 unread messages.\",\"favoriteFruit\":\"strawberry\"}"
            .getBytes("UTF-8");
        mFormat3 = new Message("test", 0, 0, null, format3);

        byte invalidDate[] = "{\"timestamp\":\"11111111\",\"id\":0,\"guid\":\"0436b17b-e78a-4e82-accf-743bf1f0b884\",\"isActive\":false,\"balance\":\"$3,561.87\",\"picture\":\"http://placehold.it/32x32\",\"age\":23,\"eyeColor\":\"green\",\"name\":\"Mercedes Brewer\",\"gender\":\"female\",\"company\":\"MALATHION\",\"email\":\"mercedesbrewer@malathion.com\",\"phone\":\"+1 (848) 471-3000\",\"address\":\"786 Gilmore Court, Brule, Maryland, 3200\",\"about\":\"Quis nostrud Lorem deserunt esse ut reprehenderit aliqua nisi et sunt mollit est. Cupidatat incididunt minim anim eiusmod culpa elit est dolor ullamco. Aliqua cillum eiusmod ullamco nostrud Lorem sit amet Lorem aliquip esse esse velit.\\r\\n\",\"registered\":\"2014-01-14T13:07:28 +08:00\",\"latitude\":47.672012,\"longitude\":102.788623,\"tags\":[\"amet\",\"amet\",\"dolore\",\"eu\",\"qui\",\"fugiat\",\"laborum\"],\"friends\":[{\"id\":0,\"name\":\"Rebecca Hardy\"},{\"id\":1,\"name\":\"Sutton Briggs\"},{\"id\":2,\"name\":\"Dena Campos\"}],\"greeting\":\"Hello, Mercedes Brewer! You have 7 unread messages.\",\"favoriteFruit\":\"strawberry\"}"
            .getBytes("UTF-8");
        mInvalidDate = new Message("test", 0, 0, null, invalidDate);

        byte isoFormat[] = "{\"timestamp\":\"2006-01-02T15:04:05Z\",\"id\":0,\"guid\":\"0436b17b-e78a-4e82-accf-743bf1f0b884\",\"isActive\":false,\"balance\":\"$3,561.87\",\"picture\":\"http://placehold.it/32x32\",\"age\":23,\"eyeColor\":\"green\",\"name\":\"Mercedes Brewer\",\"gender\":\"female\",\"company\":\"MALATHION\",\"email\":\"mercedesbrewer@malathion.com\",\"phone\":\"+1 (848) 471-3000\",\"address\":\"786 Gilmore Court, Brule, Maryland, 3200\",\"about\":\"Quis nostrud Lorem deserunt esse ut reprehenderit aliqua nisi et sunt mollit est. Cupidatat incididunt minim anim eiusmod culpa elit est dolor ullamco. Aliqua cillum eiusmod ullamco nostrud Lorem sit amet Lorem aliquip esse esse velit.\\r\\n\",\"registered\":\"2014-01-14T13:07:28 +08:00\",\"latitude\":47.672012,\"longitude\":102.788623,\"tags\":[\"amet\",\"amet\",\"dolore\",\"eu\",\"qui\",\"fugiat\",\"laborum\"],\"friends\":[{\"id\":0,\"name\":\"Rebecca Hardy\"},{\"id\":1,\"name\":\"Sutton Briggs\"},{\"id\":2,\"name\":\"Dena Campos\"}],\"greeting\":\"Hello, Mercedes Brewer! You have 7 unread messages.\",\"favoriteFruit\":\"strawberry\"}"
            .getBytes("UTF-8");
        mISOFormat = new Message("test", 0, 0, null, isoFormat);

        byte nanosecondISOFormat[] = "{\"timestamp\":\"2006-01-02T23:59:59.999999999Z\"}"
            .getBytes("UTF-8");
        mNanosecondISOFormat = new Message("test", 0, 0, null, nanosecondISOFormat);

        byte nestedISOFormat[] = "{\"meta_data\":{\"created\":\"2016-01-11T11:50:28.647Z\"},\"id\":0,\"guid\":\"0436b17b-e78a-4e82-accf-743bf1f0b884\",\"isActive\":false,\"balance\":\"$3,561.87\",\"picture\":\"http://placehold.it/32x32\",\"age\":23,\"eyeColor\":\"green\",\"name\":\"Mercedes Brewer\",\"gender\":\"female\",\"company\":\"MALATHION\",\"email\":\"mercedesbrewer@malathion.com\",\"phone\":\"+1 (848) 471-3000\",\"address\":\"786 Gilmore Court, Brule, Maryland, 3200\",\"about\":\"Quis nostrud Lorem deserunt esse ut reprehenderit aliqua nisi et sunt mollit est. Cupidatat incididunt minim anim eiusmod culpa elit est dolor ullamco. Aliqua cillum eiusmod ullamco nostrud Lorem sit amet Lorem aliquip esse esse velit.\\r\\n\",\"registered\":\"2014-01-14T13:07:28 +08:00\",\"latitude\":47.672012,\"longitude\":102.788623,\"tags\":[\"amet\",\"amet\",\"dolore\",\"eu\",\"qui\",\"fugiat\",\"laborum\"],\"friends\":[{\"id\":0,\"name\":\"Rebecca Hardy\"},{\"id\":1,\"name\":\"Sutton Briggs\"},{\"id\":2,\"name\":\"Dena Campos\"}],\"greeting\":\"Hello, Mercedes Brewer! You have 7 unread messages.\",\"favoriteFruit\":\"strawberry\"}"
            .getBytes("UTF-8");
        mNestedISOFormat = new Message("test", 0, 0, null, nestedISOFormat);
    }

    @Test
    public void testExtractDateUsingInputPattern() throws Exception {
        Mockito.when(mConfig.getMessageTimestampName()).thenReturn("timestamp");
        Mockito.when(mConfig.getString("partitioner.granularity.date.prefix", "dt=")).thenReturn("dt=");
        Mockito.when(mConfig.getString("partitioner.granularity.date.format", "yyyy-MM-dd")).thenReturn("yyyy-MM-dd");

        Mockito.when(mConfig.getMessageTimestampInputPattern()).thenReturn("yyyy-MM-dd HH:mm:ss");
        assertEquals("dt=2014-07-30", new DateMessageParser(mConfig).extractPartitions(mFormat1)[0]);

        Mockito.when(mConfig.getMessageTimestampInputPattern()).thenReturn("yyyy/MM/d");
        assertEquals("dt=2014-10-25", new DateMessageParser(mConfig).extractPartitions(mFormat2)[0]);

        Mockito.when(mConfig.getMessageTimestampInputPattern()).thenReturn("yyyyy.MMMMM.dd GGG hh:mm aaa");
        assertEquals("dt=2001-07-04", new DateMessageParser(mConfig).extractPartitions(mFormat3)[0]);

        Mockito.when(mConfig.getMessageTimestampInputPattern()).thenReturn("yyyy-MM-dd'T'HH:mm:ss'Z'");
        assertEquals("dt=2006-01-02", new DateMessageParser(mConfig).extractPartitions(mISOFormat)[0]);

        Mockito.when(mConfig.getMessageTimestampInputPattern()).thenReturn("yyyy-MM-dd'T'HH:mm:ss");
        assertEquals("dt=2006-01-02", new DateMessageParser(mConfig).extractPartitions(mISOFormat)[0]);

        Mockito.when(mConfig.getMessageTimestampInputPattern()).thenReturn("yyyy-MM-dd'T'HH:mm:ss");
        assertEquals("dt=2006-01-02", new DateMessageParser(mConfig).extractPartitions(mNanosecondISOFormat)[0]);
    }

    @Test
    public void testExtractDateWithWrongEntries() throws Exception {
        Mockito.when(mConfig.getMessageTimestampName()).thenReturn("timestamp");
        Mockito.when(mConfig.getString("partitioner.granularity.date.format", "yyyy-MM-dd")).thenReturn("yyyy-MM-dd");

        // invalid date
        Mockito.when(mConfig.getMessageTimestampInputPattern()).thenReturn("yyyy-MM-dd HH:mm:ss"); // any pattern
        assertEquals(DateMessageParser.defaultDate, new DateMessageParser(
            mConfig).extractPartitions(mInvalidDate)[0]);

        // invalid pattern
        Mockito.when(mConfig.getMessageTimestampInputPattern()).thenReturn("yyy-MM-dd :s");
        assertEquals(DateMessageParser.defaultDate, new DateMessageParser(
            mConfig).extractPartitions(mFormat1)[0]);
    }

    @Test
    public void testDatePrefix() throws Exception {
        Mockito.when(mConfig.getMessageTimestampName()).thenReturn("timestamp");
        Mockito.when(mConfig.getMessageTimestampInputPattern()).thenReturn("yyyy-MM-dd HH:mm:ss");
        Mockito.when(mConfig.getString("partitioner.granularity.date.prefix", "dt=")).thenReturn("foo");
        Mockito.when(mConfig.getString("partitioner.granularity.date.format", "yyyy-MM-dd")).thenReturn("yyyy-MM-dd");

        assertEquals("foo2014-07-30", new DateMessageParser(mConfig).extractPartitions(mFormat1)[0]);
    }

    @Test
    public void testNestedField() throws Exception {
        Mockito.when(mConfig.getMessageTimestampNameSeparator()).thenReturn(".");
        Mockito.when(mConfig.getMessageTimestampName()).thenReturn("meta_data.created");
        Mockito.when(mConfig.getMessageTimestampInputPattern()).thenReturn("yyyy-MM-dd'T'hh:mm:ss.SSS'Z'");
        Mockito.when(mConfig.getString("partitioner.granularity.date.prefix", "dt=")).thenReturn("dt=");
        Mockito.when(mConfig.getString("partitioner.granularity.date.format", "yyyy-MM-dd")).thenReturn("yyyy-MM-dd");

        assertEquals("dt=2016-01-11", new DateMessageParser(mConfig).extractPartitions(mNestedISOFormat)[0]);
    }

    @Test
    public void testCustomDateFormat() throws Exception {
        Mockito.when(mConfig.getMessageTimestampName()).thenReturn("timestamp");
        Mockito.when(mConfig.getMessageTimestampInputPattern()).thenReturn("yyyy-MM-dd HH:mm:ss");
        Mockito.when(mConfig.getString("partitioner.granularity.date.prefix", "dt=")).thenReturn("");
        Mockito.when(mConfig.getString("partitioner.granularity.date.format", "yyyy-MM-dd")).thenReturn("'yr='yyyy'/mo='MM'/dy='dd'/hr='HH");

        assertEquals("yr=2014/mo=07/dy=30/hr=10", new DateMessageParser(mConfig).extractPartitions(mFormat1)[0]);
    }
}
