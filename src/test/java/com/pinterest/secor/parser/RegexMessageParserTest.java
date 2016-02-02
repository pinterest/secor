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

import junit.framework.TestCase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
public class RegexMessageParserTest extends TestCase {

    private SecorConfig mConfig;
    private Message mMessageWithMillisTimestamp;
    private Message mMessageWithWrongFormatTimestamp;

    @Override
    public void setUp() throws Exception {
        mConfig = Mockito.mock(SecorConfig.class);
        Mockito.when(mConfig.getMessageTimestampInputPattern()).thenReturn("^[^ ]+ [^ ]+ ([^ ]+) .*$");

        byte messageWithMillisTimestamp[] =
            "?24.140.88.218 2015/09/22T22:19:00+0000 1442960340 GET http://api.com/test/?id=123 HTTP/1.1 s200 1017 0.384213448 pass - r685206763364 91ea566f - \"for iOS/5.4.2 (iPhone; 9.0)\"".getBytes("UTF-8");
        mMessageWithMillisTimestamp = new Message("test", 0, 0, null, messageWithMillisTimestamp);

      byte messageWithWrongFormatTimestamp[] =
          "?24.140.88.218 2015/09/22T22:19:00+0000 A1442960340 GET http://api.com/test/?id=123 HTTP/1.1 s200 1017 0.384213448 pass - r685206763364 91ea566f - \"for iOS/5.4.2 (iPhone; 9.0)\"".getBytes("UTF-8");
      mMessageWithWrongFormatTimestamp = new Message("test", 0, 0, null, messageWithWrongFormatTimestamp);

    }

    @Test
    public void testExtractTimestampMillis() throws Exception {
        RegexMessageParser regexMessageParser = new RegexMessageParser(mConfig);

        assertEquals(1442960340000l, regexMessageParser.extractTimestampMillis(mMessageWithMillisTimestamp));
    }

    @Test(expected=NumberFormatException.class)
    public void testExtractTimestampMillisEmpty() throws Exception {
        RegexMessageParser regexMessageParser = new RegexMessageParser(mConfig);
        byte emptyBytes2[] = "".getBytes();
        regexMessageParser.extractTimestampMillis(new Message("test", 0, 0, null, emptyBytes2));
    }

    @Test(expected=NumberFormatException.class)
    public void testExtractTimestampMillisException1() throws Exception {
        RegexMessageParser regexMessageParser = new RegexMessageParser(mConfig);
       regexMessageParser.extractTimestampMillis(mMessageWithWrongFormatTimestamp);
    }

}
