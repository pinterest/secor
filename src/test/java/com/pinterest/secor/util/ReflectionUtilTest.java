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
package com.pinterest.secor.util;

import com.pinterest.secor.common.LogFilePath;
import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.parser.MessageParser;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.Test;

public class ReflectionUtilTest {

    private SecorConfig mSecorConfig;
    private LogFilePath mLogFilePath;

    public void setUp() throws Exception {
        PropertiesConfiguration properties = new PropertiesConfiguration();
        mSecorConfig = new SecorConfig(properties);
        mLogFilePath = new LogFilePath("/foo", "/foo/bar/baz/1_1_1");
    }

    @Test
    public void testCreateMessageParser() throws Exception {
        MessageParser messageParser = ReflectionUtil.createMessageParser("com.pinterest.secor.parser.OffsetMessageParser",
                mSecorConfig);
    }

    @Test(expected = ClassNotFoundException.class)
    public void testMessageParserClassNotFound() throws Exception {
        ReflectionUtil.createMessageParser("com.example.foo", mSecorConfig);
    }

    @Test(expected = ClassNotFoundException.class)
    public void testFileWriterClassNotFound() throws Exception {
        ReflectionUtil.createFileWriter("com.example.foo", mLogFilePath, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMessageParserConstructorMissing() throws Exception {
        // Try to create a message parser using an existent and available class, but one not
        // assignable to MessageParser
        ReflectionUtil.createMessageParser("java.lang.Object",
                mSecorConfig);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFileWriterConstructorMissing() throws Exception {
        // Try to create a message parser using an existent and available class, but one not
        // assignable to MessageParser
        ReflectionUtil.createFileWriter("java.lang.Object",
                mLogFilePath, null);
    }
}