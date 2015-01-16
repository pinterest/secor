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
package com.pinterest.secor.io.impl;

import com.google.common.io.Files;
import com.pinterest.secor.common.LogFilePath;
import com.pinterest.secor.io.FileReader;
import com.pinterest.secor.io.FileWriter;
import com.pinterest.secor.io.KeyValue;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class SequenceFileReaderWriterFactoryTest {
    private SequenceFileReaderWriterFactory mFactory;

    public void setUp() throws Exception {
        mFactory = new SequenceFileReaderWriterFactory();
    }

    @Test
    public void testSequenceReadWriteRoundTrip() throws Exception {
        SequenceFileReaderWriterFactory factory = new SequenceFileReaderWriterFactory();
        LogFilePath tempLogFilePath = new LogFilePath(Files.createTempDir().toString(),
                "test-topic",
                new String[]{"part-1"},
                0,
                1,
                0,
                ".log"
        );
        FileWriter fileWriter = factory.BuildFileWriter(tempLogFilePath, null);
        KeyValue kv1 = (new KeyValue(23232, new byte[]{23, 45, 40 ,10, 122}));
        KeyValue kv2 = (new KeyValue(23233, new byte[]{2, 3, 4, 5}));
        fileWriter.write(kv1);
        fileWriter.write(kv2);
        fileWriter.close();
        FileReader fileReader = factory.BuildFileReader(tempLogFilePath, null);

        KeyValue kvout = fileReader.next();
        assertEquals(kv1.getKey(), kvout.getKey());
        assertArrayEquals(kv1.getValue(), kvout.getValue());
        kvout = fileReader.next();
        assertEquals(kv2.getKey(), kvout.getKey());
        assertArrayEquals(kv2.getValue(), kvout.getValue());
    }


}