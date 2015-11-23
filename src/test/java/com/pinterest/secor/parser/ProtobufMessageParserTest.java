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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.modules.junit4.PowerMockRunner;
import com.google.protobuf.CodedOutputStream;

@RunWith(PowerMockRunner.class)
public class ProtobufMessageParserTest extends TestCase {
    private SecorConfig mConfig;

    private Message buildMessage(long timestamp) throws Exception {
        byte data[] = new byte[16];
        CodedOutputStream output = CodedOutputStream.newInstance(data);
        output.writeUInt64(1, timestamp);
        return new Message("test", 0, 0, null, data);
    }

    @Override
    public void setUp() throws Exception {
        mConfig = Mockito.mock(SecorConfig.class);
    }

    @Test
    public void testExtractTimestampMillis() throws Exception {
        ProtobufMessageParser parser = new ProtobufMessageParser(mConfig);

        assertEquals(1405970352000l, parser.extractTimestampMillis(buildMessage(1405970352l)));
        assertEquals(1405970352123l, parser.extractTimestampMillis(buildMessage(1405970352123l)));
    }
}
