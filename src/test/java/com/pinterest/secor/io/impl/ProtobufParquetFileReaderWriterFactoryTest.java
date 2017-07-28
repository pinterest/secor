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

import static org.junit.Assert.assertArrayEquals;

import java.util.HashMap;
import java.util.Map;

import org.apache.parquet.hadoop.ParquetWriter;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.modules.junit4.PowerMockRunner;

import com.google.common.io.Files;
import com.pinterest.secor.common.LogFilePath;
import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.io.FileReader;
import com.pinterest.secor.io.FileWriter;
import com.pinterest.secor.io.KeyValue;
import com.pinterest.secor.protobuf.Messages.UnitTestMessage3;
import com.pinterest.secor.util.ParquetUtil;
import com.pinterest.secor.util.ReflectionUtil;


import junit.framework.TestCase;

@RunWith(PowerMockRunner.class)
public class ProtobufParquetFileReaderWriterFactoryTest extends TestCase {

    private SecorConfig config;

    @Override
    public void setUp() throws Exception {
        config = Mockito.mock(SecorConfig.class);
    }

    @Test
    public void testProtobufParquetReadWriteRoundTrip() throws Exception {
        Map<String, String> classPerTopic = new HashMap<String, String>();
        classPerTopic.put("test-pb-topic", UnitTestMessage3.class.getName());
        Mockito.when(config.getProtobufMessageClassPerTopic()).thenReturn(classPerTopic);
        Mockito.when(config.getFileReaderWriterFactory())
                .thenReturn(ProtobufParquetFileReaderWriterFactory.class.getName());
        Mockito.when(ParquetUtil.getParquetBlockSize(config))
                .thenReturn(ParquetWriter.DEFAULT_BLOCK_SIZE);
        Mockito.when(ParquetUtil.getParquetPageSize(config))
                .thenReturn(ParquetWriter.DEFAULT_PAGE_SIZE);
        Mockito.when(ParquetUtil.getParquetEnableDictionary(config))
                .thenReturn(ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED);
        Mockito.when(ParquetUtil.getParquetValidation(config))
                .thenReturn(ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED);


        LogFilePath tempLogFilePath = new LogFilePath(Files.createTempDir().toString(), "test-pb-topic",
                new String[] { "part-1" }, 0, 1, 23232, ".log");

        FileWriter fileWriter = ReflectionUtil.createFileWriter(config.getFileReaderWriterFactory(), tempLogFilePath,
                null, config);

        UnitTestMessage3 msg1 = UnitTestMessage3.newBuilder().setData("abc").setTimestamp(1467176315L).build();
        UnitTestMessage3 msg2 = UnitTestMessage3.newBuilder().setData("XYZ").setTimestamp(1467176344L).build();

        KeyValue kv1 = (new KeyValue(23232, msg1.toByteArray()));
        KeyValue kv2 = (new KeyValue(23233, msg2.toByteArray()));
        fileWriter.write(kv1);
        fileWriter.write(kv2);
        fileWriter.close();

        FileReader fileReader = ReflectionUtil.createFileReader(config.getFileReaderWriterFactory(), tempLogFilePath,
                null, config);

        KeyValue kvout = fileReader.next();
        assertEquals(kv1.getOffset(), kvout.getOffset());
        assertArrayEquals(kv1.getValue(), kvout.getValue());
        assertEquals(msg1.getData(), UnitTestMessage3.parseFrom(kvout.getValue()).getData());

        kvout = fileReader.next();
        assertEquals(kv2.getOffset(), kvout.getOffset());
        assertArrayEquals(kv2.getValue(), kvout.getValue());
        assertEquals(msg2.getData(), UnitTestMessage3.parseFrom(kvout.getValue()).getData());
    }
}