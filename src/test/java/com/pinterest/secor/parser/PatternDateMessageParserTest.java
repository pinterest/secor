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

import java.util.Date;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.modules.junit4.PowerMockRunner;

import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.message.Message;

@RunWith(PowerMockRunner.class)
public class PatternDateMessageParserTest extends TestCase {

    private SecorConfig mConfig;
    private Message mFormat1;
    private Message mFormat2;
    private Message mFormat3;
    private Message mFormat4;
    private Message mFormat5;
    private Message mFormat6;
    private Message mInvalidDate;

    @Override
    public void setUp() throws Exception {
        mConfig = Mockito.mock(SecorConfig.class);
        Mockito.when(mConfig.getMessageTimestampName()).thenReturn("timestamp");
        Mockito.when(mConfig.getPartitionPrefixIdentifier()).thenReturn("event");

        byte format1[] = "{\"timestamp\":\"2014-07-30 10:53:20\",\"id\":0,\"guid\":\"0436b17b-e78a-4e82-accf-743bf1f0b884\",\"isActive\":false,\"balance\":\"$3,561.87\",\"picture\":\"http://placehold.it/32x32\",\"age\":23,\"eyeColor\":\"green\",\"name\":\"Mercedes Brewer\",\"gender\":\"female\",\"company\":\"MALATHION\",\"email\":\"mercedesbrewer@malathion.com\",\"phone\":\"+1 (848) 471-3000\",\"address\":\"786 Gilmore Court, Brule, Maryland, 3200\",\"about\":\"Quis nostrud Lorem deserunt esse ut reprehenderit aliqua nisi et sunt mollit est. Cupidatat incididunt minim anim eiusmod culpa elit est dolor ullamco. Aliqua cillum eiusmod ullamco nostrud Lorem sit amet Lorem aliquip esse esse velit.\\r\\n\",\"registered\":\"2014-01-14T13:07:28 +08:00\",\"latitude\":47.672012,\"longitude\":102.788623,\"tags\":[\"amet\",\"amet\",\"dolore\",\"eu\",\"qui\",\"fugiat\",\"laborum\"],\"friends\":[{\"id\":0,\"name\":\"Rebecca Hardy\"},{\"id\":1,\"name\":\"Sutton Briggs\"},{\"id\":2,\"name\":\"Dena Campos\"}],\"greeting\":\"Hello, Mercedes Brewer! You have 7 unread messages.\",\"favoriteFruit\":\"strawberry\"}"
                .getBytes("UTF-8");
        mFormat1 = new Message("test", 0, 0, null, format1);

        byte format2[] = "{\"timestamp\":\"2014/10/25\",\"id\":0,\"guid\":\"0436b17b-e78a-4e82-accf-743bf1f0b884\",\"isActive\":false,\"balance\":\"$3,561.87\",\"picture\":\"http://placehold.it/32x32\",\"age\":23,\"eyeColor\":\"green\",\"name\":\"Mercedes Brewer\",\"gender\":\"female\",\"company\":\"MALATHION\",\"email\":\"mercedesbrewer@malathion.com\",\"phone\":\"+1 (848) 471-3000\",\"address\":\"786 Gilmore Court, Brule, Maryland, 3200\",\"about\":\"Quis nostrud Lorem deserunt esse ut reprehenderit aliqua nisi et sunt mollit est. Cupidatat incididunt minim anim eiusmod culpa elit est dolor ullamco. Aliqua cillum eiusmod ullamco nostrud Lorem sit amet Lorem aliquip esse esse velit.\\r\\n\",\"registered\":\"2014-01-14T13:07:28 +08:00\",\"latitude\":47.672012,\"longitude\":102.788623,\"tags\":[\"amet\",\"amet\",\"dolore\",\"eu\",\"qui\",\"fugiat\",\"laborum\"],\"friends\":[{\"id\":0,\"name\":\"Rebecca Hardy\"},{\"id\":1,\"name\":\"Sutton Briggs\"},{\"id\":2,\"name\":\"Dena Campos\"}],\"greeting\":\"Hello, Mercedes Brewer! You have 7 unread messages.\",\"favoriteFruit\":\"strawberry\"}"
                .getBytes("UTF-8");
        mFormat2 = new Message("test", 0, 0, null, format2);

        byte format3[] = "{\"timestamp\":\"02001.July.04 AD 12:08 PM\",\"id\":0,\"guid\":\"0436b17b-e78a-4e82-accf-743bf1f0b884\",\"isActive\":false,\"balance\":\"$3,561.87\",\"picture\":\"http://placehold.it/32x32\",\"age\":23,\"eyeColor\":\"green\",\"name\":\"Mercedes Brewer\",\"gender\":\"female\",\"company\":\"MALATHION\",\"email\":\"mercedesbrewer@malathion.com\",\"phone\":\"+1 (848) 471-3000\",\"address\":\"786 Gilmore Court, Brule, Maryland, 3200\",\"about\":\"Quis nostrud Lorem deserunt esse ut reprehenderit aliqua nisi et sunt mollit est. Cupidatat incididunt minim anim eiusmod culpa elit est dolor ullamco. Aliqua cillum eiusmod ullamco nostrud Lorem sit amet Lorem aliquip esse esse velit.\\r\\n\",\"registered\":\"2014-01-14T13:07:28 +08:00\",\"latitude\":47.672012,\"longitude\":102.788623,\"tags\":[\"amet\",\"amet\",\"dolore\",\"eu\",\"qui\",\"fugiat\",\"laborum\"],\"friends\":[{\"id\":0,\"name\":\"Rebecca Hardy\"},{\"id\":1,\"name\":\"Sutton Briggs\"},{\"id\":2,\"name\":\"Dena Campos\"}],\"greeting\":\"Hello, Mercedes Brewer! You have 7 unread messages.\",\"favoriteFruit\":\"strawberry\"}"
                .getBytes("UTF-8");
        mFormat3 = new Message("test", 0, 0, null, format3);
        
        byte format4[] = "{\"timestamp\":1450601098673,\"id\":0,\"guid\":\"0436b17b-e78a-4e82-accf-743bf1f0b884\",\"isActive\":false,\"balance\":\"$3,561.87\",\"picture\":\"http://placehold.it/32x32\",\"age\":23,\"eyeColor\":\"green\",\"name\":\"Mercedes Brewer\",\"gender\":\"female\",\"company\":\"MALATHION\",\"email\":\"mercedesbrewer@malathion.com\",\"phone\":\"+1 (848) 471-3000\",\"address\":\"786 Gilmore Court, Brule, Maryland, 3200\",\"about\":\"Quis nostrud Lorem deserunt esse ut reprehenderit aliqua nisi et sunt mollit est. Cupidatat incididunt minim anim eiusmod culpa elit est dolor ullamco. Aliqua cillum eiusmod ullamco nostrud Lorem sit amet Lorem aliquip esse esse velit.\\r\\n\",\"registered\":\"2014-01-14T13:07:28 +08:00\",\"latitude\":47.672012,\"longitude\":102.788623,\"tags\":[\"amet\",\"amet\",\"dolore\",\"eu\",\"qui\",\"fugiat\",\"laborum\"],\"friends\":[{\"id\":0,\"name\":\"Rebecca Hardy\"},{\"id\":1,\"name\":\"Sutton Briggs\"},{\"id\":2,\"name\":\"Dena Campos\"}],\"greeting\":\"Hello, Mercedes Brewer! You have 7 unread messages.\",\"favoriteFruit\":\"strawberry\"}"
                .getBytes("UTF-8");
        mFormat4 = new Message("test", 0, 0, null, format4);
        
        byte format5[] = "{\"event\":\"XYZ\",\"timestamp\":\"2014-07-30 10:53:20\",\"id\":0,\"guid\":\"0436b17b-e78a-4e82-accf-743bf1f0b884\",\"isActive\":false,\"balance\":\"$3,561.87\",\"picture\":\"http://placehold.it/32x32\",\"age\":23,\"eyeColor\":\"green\",\"name\":\"Mercedes Brewer\",\"gender\":\"female\",\"company\":\"MALATHION\",\"email\":\"mercedesbrewer@malathion.com\",\"phone\":\"+1 (848) 471-3000\",\"address\":\"786 Gilmore Court, Brule, Maryland, 3200\",\"about\":\"Quis nostrud Lorem deserunt esse ut reprehenderit aliqua nisi et sunt mollit est. Cupidatat incididunt minim anim eiusmod culpa elit est dolor ullamco. Aliqua cillum eiusmod ullamco nostrud Lorem sit amet Lorem aliquip esse esse velit.\\r\\n\",\"registered\":\"2014-01-14T13:07:28 +08:00\",\"latitude\":47.672012,\"longitude\":102.788623,\"tags\":[\"amet\",\"amet\",\"dolore\",\"eu\",\"qui\",\"fugiat\",\"laborum\"],\"friends\":[{\"id\":0,\"name\":\"Rebecca Hardy\"},{\"id\":1,\"name\":\"Sutton Briggs\"},{\"id\":2,\"name\":\"Dena Campos\"}],\"greeting\":\"Hello, Mercedes Brewer! You have 7 unread messages.\",\"favoriteFruit\":\"strawberry\"}"
                .getBytes("UTF-8");
        mFormat5 = new Message("test", 0, 0, null, format5);
        
        byte format6[] = "{\"event\":\"ABC\",\"timestamp\":\"2014-07-30 10:53:20\",\"id\":0,\"guid\":\"0436b17b-e78a-4e82-accf-743bf1f0b884\",\"isActive\":false,\"balance\":\"$3,561.87\",\"picture\":\"http://placehold.it/32x32\",\"age\":23,\"eyeColor\":\"green\",\"name\":\"Mercedes Brewer\",\"gender\":\"female\",\"company\":\"MALATHION\",\"email\":\"mercedesbrewer@malathion.com\",\"phone\":\"+1 (848) 471-3000\",\"address\":\"786 Gilmore Court, Brule, Maryland, 3200\",\"about\":\"Quis nostrud Lorem deserunt esse ut reprehenderit aliqua nisi et sunt mollit est. Cupidatat incididunt minim anim eiusmod culpa elit est dolor ullamco. Aliqua cillum eiusmod ullamco nostrud Lorem sit amet Lorem aliquip esse esse velit.\\r\\n\",\"registered\":\"2014-01-14T13:07:28 +08:00\",\"latitude\":47.672012,\"longitude\":102.788623,\"tags\":[\"amet\",\"amet\",\"dolore\",\"eu\",\"qui\",\"fugiat\",\"laborum\"],\"friends\":[{\"id\":0,\"name\":\"Rebecca Hardy\"},{\"id\":1,\"name\":\"Sutton Briggs\"},{\"id\":2,\"name\":\"Dena Campos\"}],\"greeting\":\"Hello, Mercedes Brewer! You have 7 unread messages.\",\"favoriteFruit\":\"strawberry\"}"
                .getBytes("UTF-8");
        mFormat6 = new Message("test", 0, 0, null, format6);

        byte invalidDate[] = "{\"timestamp\":\"11111111\",\"id\":0,\"guid\":\"0436b17b-e78a-4e82-accf-743bf1f0b884\",\"isActive\":false,\"balance\":\"$3,561.87\",\"picture\":\"http://placehold.it/32x32\",\"age\":23,\"eyeColor\":\"green\",\"name\":\"Mercedes Brewer\",\"gender\":\"female\",\"company\":\"MALATHION\",\"email\":\"mercedesbrewer@malathion.com\",\"phone\":\"+1 (848) 471-3000\",\"address\":\"786 Gilmore Court, Brule, Maryland, 3200\",\"about\":\"Quis nostrud Lorem deserunt esse ut reprehenderit aliqua nisi et sunt mollit est. Cupidatat incididunt minim anim eiusmod culpa elit est dolor ullamco. Aliqua cillum eiusmod ullamco nostrud Lorem sit amet Lorem aliquip esse esse velit.\\r\\n\",\"registered\":\"2014-01-14T13:07:28 +08:00\",\"latitude\":47.672012,\"longitude\":102.788623,\"tags\":[\"amet\",\"amet\",\"dolore\",\"eu\",\"qui\",\"fugiat\",\"laborum\"],\"friends\":[{\"id\":0,\"name\":\"Rebecca Hardy\"},{\"id\":1,\"name\":\"Sutton Briggs\"},{\"id\":2,\"name\":\"Dena Campos\"}],\"greeting\":\"Hello, Mercedes Brewer! You have 7 unread messages.\",\"favoriteFruit\":\"strawberry\"}"
                .getBytes("UTF-8");
        mInvalidDate = new Message("test", 0, 0, null, invalidDate);
        
    }

    @Test
    public void testExtractDateUsingInputPattern() throws Exception {
    	Mockito.when(mConfig.getMessageTimestampInputPattern()).thenReturn("yyyy-MM-dd HH:mm:ss");
    	Mockito.when(mConfig.getPartitionOutputDtFormat()).thenReturn("'dt='yyyy-MM-dd");
        assertEquals("dt=2014-07-30", new PatternDateMessageParser(mConfig).extractPartitions(mFormat1)[0]);

        Mockito.when(mConfig.getMessageTimestampInputPattern()).thenReturn("yyyy/MM/d");
        Mockito.when(mConfig.getPartitionOutputDtFormat()).thenReturn("yyyy-MM-dd");
        assertEquals("2014-10-25", new PatternDateMessageParser(mConfig).extractPartitions(mFormat2)[0]);

        Mockito.when(mConfig.getMessageTimestampInputPattern()).thenReturn("yyyyy.MMMMM.dd GGG hh:mm aaa");
        assertEquals("2001-07-04", new PatternDateMessageParser(mConfig).extractPartitions(mFormat3)[0]);
        
        Mockito.when(mConfig.getMessageTimestampInputPattern()).thenReturn("");
        Mockito.when(mConfig.getPartitionOutputDtFormat()).thenReturn("yyyy/MM/dd");
        assertEquals("2015/12/20", new PatternDateMessageParser(mConfig).extractPartitions(mFormat4)[0]);
    }
    
    @Test
    public void testExtractPartitionName() throws Exception {
    	
        Mockito.when(mConfig.getMessageTimestampInputPattern()).thenReturn("yyyy-MM-dd HH:mm:ss");
    	Mockito.when(mConfig.getPartitionOutputDtFormat()).thenReturn("yyyy-MM-dd");
    	Mockito.when(mConfig.isPartitionPrefixEnabled()).thenReturn(false);
        assertEquals("2014-07-30", new PatternDateMessageParser(mConfig).extractPartitions(mFormat5)[0]);
        assertEquals("2014-07-30", new PatternDateMessageParser(mConfig).extractPartitions(mFormat6)[0]);

        Mockito.when(mConfig.getMessageTimestampInputPattern()).thenReturn("yyyy-MM-dd HH:mm:ss");
    	Mockito.when(mConfig.getPartitionOutputDtFormat()).thenReturn("yyyy-MM-dd");
    	Mockito.when(mConfig.getPartitionPrefixMapping()).thenReturn("{\"XYZ\":\"1\",\"ABC\":\"2\"}");
    	Mockito.when(mConfig.isPartitionPrefixEnabled()).thenReturn(true);
        assertEquals("1/2014-07-30", new PatternDateMessageParser(mConfig).extractPartitions(mFormat5)[0]);
        assertEquals("2/2014-07-30", new PatternDateMessageParser(mConfig).extractPartitions(mFormat6)[0]);
    }

    @Test
    public void testExtractDateWithWrongEntries() throws Exception {
        // invalid date
    	Mockito.when(mConfig.getMessageTimestampInputPattern()).thenReturn("yyyy-MM-dd HH:mm:ss"); // any pattern
        assertEquals(PatternDateMessageParser.defaultDate, new PatternDateMessageParser(mConfig).extractPartitions(mInvalidDate)[0]);

        // invalid pattern
        Mockito.when(mConfig.getMessageTimestampInputPattern()).thenReturn("yyy-MM-dd :s");
        assertEquals(PatternDateMessageParser.defaultDate, new PatternDateMessageParser(mConfig).extractPartitions(mFormat1)[0]);
    }
}