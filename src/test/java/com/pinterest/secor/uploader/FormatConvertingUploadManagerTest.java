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
package com.pinterest.secor.uploader;

import com.google.common.io.Files;

import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.common.LogFilePath;
import com.pinterest.secor.io.FileReader;
import com.pinterest.secor.io.FileWriter;
import com.pinterest.secor.io.impl.DelimitedTextFileReaderWriterFactory;
import com.pinterest.secor.io.impl.SequenceFileReaderWriterFactory;
import com.pinterest.secor.io.KeyValue;
import com.pinterest.secor.uploader.FormatConvertingUploadManager;
import com.pinterest.secor.uploader.S3UploadManager;
import com.pinterest.secor.uploader.S3UploadHandle;
import com.pinterest.secor.uploader.Handle;
import com.pinterest.secor.uploader.TempFileUploadHandle;
import com.pinterest.secor.uploader.TestHandle;
import com.pinterest.secor.uploader.TestUploadManager;
import com.pinterest.secor.uploader.UploadManager;
import com.pinterest.secor.util.FileUtil;

import java.io.IOException;
import java.io.File;
import java.util.HashSet;

import junit.framework.TestCase;

import org.apache.hadoop.io.compress.CompressionCodec;
import org.joda.time.DateTime;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Tests logic of converting files from
 * one format to another prior to upload
 *
 * @author Jason Butterfield (jason.butterfield@outreach.io)
 */
@RunWith(PowerMockRunner.class)
public class FormatConvertingUploadManagerTest extends TestCase {

    public void testUpload() throws Exception {
        SecorConfig mConfig = Mockito.mock(SecorConfig.class);
        Mockito.when(mConfig.getFileReaderWriterFactory()).thenReturn("com.pinterest.secor.io.impl.SequenceFileReaderWriterFactory");
        Mockito.when(mConfig.getDestinationFileReaderWriterFactory()).thenReturn("com.pinterest.secor.io.impl.DelimitedTextFileReaderWriterFactory");
        Mockito.when(mConfig.getInnerUploadManagerClass()).thenReturn("com.pinterest.secor.uploader.TestUploadManager");

        FormatConvertingUploadManager unspiedUploadManager = new FormatConvertingUploadManager(mConfig);
        FormatConvertingUploadManager uploadManager = Mockito.spy(unspiedUploadManager);

        PowerMockito.whenNew(FormatConvertingUploadManager.class).withArguments(mConfig).thenReturn(uploadManager);

        SequenceFileReaderWriterFactory sequenceFileFactory = new SequenceFileReaderWriterFactory();
        LogFilePath tempLogFilePath = new LogFilePath(Files.createTempDir().toString(),
                "test-topic",
                new String[]{"part-1"},
                0,
                1,
                23232,
                ".log"
        );

        FileWriter writer = sequenceFileFactory.BuildFileWriter(tempLogFilePath, null);

        KeyValue kv1 = new KeyValue(23232, "value1".getBytes());
        KeyValue kv2 = new KeyValue(23233, "value2".getBytes());
        writer.write(kv1);
        writer.write(kv2);
        writer.close();

        TempFileUploadHandle<?> tempFileUploadHandle = uploadManager.upload(tempLogFilePath);
        LogFilePath convertedFilePath = tempFileUploadHandle.getTempFilePath();

        // test that the converted file contains the correct format
        DelimitedTextFileReaderWriterFactory textFileFactory = new DelimitedTextFileReaderWriterFactory();
        FileReader reader = textFileFactory.BuildFileReader(convertedFilePath, null);
        KeyValue kvout = reader.next();
        assertEquals(kv1.getOffset(), kvout.getOffset());
        assertArrayEquals(kv1.getValue(), kvout.getValue());
        
        kvout = reader.next();
        assertEquals(kv2.getOffset(), kvout.getOffset());
        assertArrayEquals(kv2.getValue(), kvout.getValue());
        reader.close();

        // Test that it deletes the file after resolving the handle
        tempFileUploadHandle.get();
        assertEquals(false, (new File(convertedFilePath.getLogFilePath()).exists()));
        assertEquals(false, (new File(convertedFilePath.getLogFileCrcPath()).exists()));
    }
}
