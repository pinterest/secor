////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/*
 * Copyright © 2017 Unified Social, Inc.
 * 180 Madison Avenue, 23rd Floor, New York, NY 10016, U.S.A.
 * All rights reserved.
 *
 * This software (the "Software") is provided pursuant to the license agreement you entered into with Unified Social,
 * Inc. (the "License Agreement").  The Software is the confidential and proprietary information of Unified Social,
 * Inc., and you shall use it only in accordance with the terms and conditions of the License Agreement.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND "AS AVAILABLE."  UNIFIED SOCIAL, INC. MAKES NO WARRANTIES OF ANY KIND, WHETHER
 * EXPRESS OR IMPLIED, INCLUDING, BUT NOT LIMITED TO THE IMPLIED WARRANTIES AND CONDITIONS OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NON-INFRINGEMENT.
 */

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

package com.pinterest.secor.parser;

import java.util.LinkedHashMap;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.modules.junit4.PowerMockRunner;

import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.message.Message;
import junit.framework.TestCase;

@RunWith(PowerMockRunner.class)
public class JsonTimestampPathParserTest extends TestCase {

    @Rule
    public ExpectedException mExpectedException = ExpectedException.none();

    private SecorConfig mConfig;
    private Message     mMessageWithSecondsTimestamp;

    @Override
    public void setUp () throws Exception {
        mConfig = Mockito.mock(SecorConfig.class);
        Mockito.when(mConfig.isMessageTimestampRequired()).thenReturn(true);
        Mockito.when(mConfig.getMessageTimestampName()).thenReturn("timestamp");

        String encoding = "UTF-8";

        byte messageWithSecondsTimestamp[] =
            "{\"timestamp\":\"1405911096\",\"id\":0,\"guid\":\"0436b17b-e78a-4e82-accf-743bf1f0b884\",\"isActive\":false,\"balance\":\"$3,561.87\",\"picture\":\"http://placehold.it/32x32\",\"age\":23,\"eyeColor\":\"green\",\"name\":\"Mercedes Brewer\",\"gender\":\"female\",\"company\":\"MALATHION\",\"email\":\"mercedesbrewer@malathion.com\",\"phone\":\"+1 (848) 471-3000\",\"address\":\"786 Gilmore Court, Brule, Maryland, 3200\",\"about\":\"Quis nostrud Lorem deserunt esse ut reprehenderit aliqua nisi et sunt mollit est. Cupidatat incididunt minim anim eiusmod culpa elit est dolor ullamco. Aliqua cillum eiusmod ullamco nostrud Lorem sit amet Lorem aliquip esse esse velit.\\r\\n\",\"registered\":\"2014-01-14T13:07:28 +08:00\",\"latitude\":47.672012,\"longitude\":102.788623,\"tags\":[\"amet\",\"amet\",\"dolore\",\"eu\",\"qui\",\"fugiat\",\"laborum\"],\"friends\":[{\"id\":0,\"name\":\"Rebecca Hardy\"},{\"id\":1,\"name\":\"Sutton Briggs\"},{\"id\":2,\"name\":\"Dena Campos\"}],\"greeting\":\"Hello, Mercedes Brewer! You have 7 unread messages.\",\"favoriteFruit\":\"strawberry\"}".getBytes(encoding);
        mMessageWithSecondsTimestamp =
            new Message("test", 0, 0, null, messageWithSecondsTimestamp);
    }

    @Test
    public void testExtractPartitions () throws Exception {
        LinkedHashMap<String, String> prefixToJsonPathMap = new LinkedHashMap<String, String>();
        prefixToJsonPathMap.put("gender=", "$.gender");
        prefixToJsonPathMap.put("id=", "$.id");

        Mockito.when(mConfig.getMessagePartitionFieldPrefixToJsonPathMap()).thenReturn(prefixToJsonPathMap);

        TimestampJsonPathParser jsonPathParser = new TimestampJsonPathParser(mConfig);

        String resultSeconds[] = jsonPathParser.extractPartitions(mMessageWithSecondsTimestamp);
        assertEquals(6, resultSeconds.length);
        assertEquals("gender=female", resultSeconds[0]);
        assertEquals("id=0", resultSeconds[1]);
        assertEquals("year=2014", resultSeconds[2]);
        assertEquals("month=07", resultSeconds[3]);
        assertEquals("day=20", resultSeconds[4]);
        assertEquals("hour=19", resultSeconds[5]);
    }

    @Test
    public void testExtractPartitions_nestedJson () throws Exception {
        LinkedHashMap<String, String> prefixToJsonPathMap = new LinkedHashMap<String, String>();
        prefixToJsonPathMap.put("gender=", "$.gender");
        prefixToJsonPathMap.put("id=", "$.id");
        prefixToJsonPathMap.put("friends=", "$['friends'][0]['name']");

        Mockito.when(mConfig.getMessagePartitionFieldPrefixToJsonPathMap()).thenReturn(prefixToJsonPathMap);

        TimestampJsonPathParser jsonPathParser = new TimestampJsonPathParser(mConfig);

        String resultSeconds[] = jsonPathParser.extractPartitions(mMessageWithSecondsTimestamp);
        assertEquals(7, resultSeconds.length);
        assertEquals("gender=female", resultSeconds[0]);
        assertEquals("id=0", resultSeconds[1]);
        assertEquals("friends=Rebecca Hardy", resultSeconds[2]);
        assertEquals("year=2014", resultSeconds[3]);
        assertEquals("month=07", resultSeconds[4]);
        assertEquals("day=20", resultSeconds[5]);
        assertEquals("hour=19", resultSeconds[6]);
    }

    @Test
    public void testExtractPartitions_missingFields () throws Exception {
        LinkedHashMap<String, String> prefixToJsonPathMap = new LinkedHashMap<String, String>();
        prefixToJsonPathMap.put("gender=", "$.invalidField");

        Mockito.when(mConfig.getMessagePartitionFieldPrefixToJsonPathMap()).thenReturn(prefixToJsonPathMap);

        mExpectedException.expect(RuntimeException.class);
        mExpectedException.expectMessage("Failed to extract jsonPath: [$.invalidField] from the message");

        TimestampJsonPathParser jsonPathParser = new TimestampJsonPathParser(mConfig);
        jsonPathParser.extractPartitions(mMessageWithSecondsTimestamp);
    }
}
