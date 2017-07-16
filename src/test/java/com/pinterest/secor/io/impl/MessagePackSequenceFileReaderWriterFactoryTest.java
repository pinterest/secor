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

public class MessagePackSequenceFileReaderWriterFactoryTest {

    @Test
    public void testMessagePackSequenceReadWriteRoundTrip() throws Exception {
        MessagePackSequenceFileReaderWriterFactory factory =
                new MessagePackSequenceFileReaderWriterFactory();
        LogFilePath tempLogFilePath = new LogFilePath(Files.createTempDir().toString(),
                "test-topic",
                new String[]{"part-1"},
                0,
                1,
                0,
                ".log"
        );
        FileWriter fileWriter = factory.BuildFileWriter(tempLogFilePath, null);
        KeyValue kv1 = (new KeyValue(23232, new byte[]{44, 55, 66, 77, 88}, new byte[]{23, 45, 40 ,10, 122}));
        KeyValue kv2 = (new KeyValue(23233, new byte[]{2, 3, 4, 5}));
        KeyValue kv3 =  (new KeyValue(23234, new byte[]{44, 55, 66, 77, 88}, new byte[]{23, 45, 40 ,10, 122}, 1496318250l));
        KeyValue kv4 =  (new KeyValue(23235, null, new byte[]{23, 45, 40 ,10, 122}, 1496318250l));

        fileWriter.write(kv1);
        fileWriter.write(kv2);
        fileWriter.write(kv3);
        fileWriter.write(kv4);

        fileWriter.close();
        FileReader fileReader = factory.BuildFileReader(tempLogFilePath, null);

        KeyValue kvout = fileReader.next();
        assertEquals(kv1.getOffset(), kvout.getOffset());
        assertArrayEquals(kv1.getKafkaKey(), kvout.getKafkaKey());
        assertArrayEquals(kv1.getValue(), kvout.getValue());
        kvout = fileReader.next();
        assertEquals(kv2.getOffset(), kvout.getOffset());
        assertArrayEquals(kv2.getKafkaKey(), kvout.getKafkaKey());
        assertArrayEquals(kv2.getValue(), kvout.getValue());
        kvout = fileReader.next();
        assertEquals(kv3.getOffset(), kvout.getOffset());
        assertArrayEquals(kv3.getKafkaKey(), kvout.getKafkaKey());
        assertArrayEquals(kv3.getValue(), kvout.getValue());
        assertEquals(kv3.getTimestamp(), kvout.getTimestamp());
        kvout = fileReader.next();
        assertEquals(kv4.getOffset(), kvout.getOffset());
        assertArrayEquals(new byte[0], kvout.getKafkaKey());
        assertArrayEquals(kv4.getValue(), kvout.getValue());
        assertEquals(kv4.getTimestamp(), kvout.getTimestamp());

    }

}