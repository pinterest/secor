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
package com.pinterest.secor.io;

import java.io.*;
import java.net.URI;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.GzipCodec;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.pinterest.secor.common.LogFilePath;
import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.io.impl.DelimitedTextFileReaderWriterFactory;
import com.pinterest.secor.io.impl.SequenceFileReaderWriterFactory;
import com.pinterest.secor.util.ReflectionUtil;

import junit.framework.TestCase;

/**
 * Test the file readers and writers
 *
 * @author Praveen Murugesan (praveen@uber.com)
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({FileSystem.class, DelimitedTextFileReaderWriterFactory.class,
        SequenceFile.class, SequenceFileReaderWriterFactory.class, GzipCodec.class,
        FileInputStream.class, FileOutputStream.class})
public class FileReaderWriterFactoryTest extends TestCase {

    private static final String DIR = "/some_parent_dir/some_topic/some_partition/some_other_partition";
    private static final String BASENAME = "10_0_00000000000000000100";
    private static final String PATH = DIR + "/" + BASENAME;
    private static final String PATH_GZ = DIR + "/" + BASENAME + ".gz";

    private LogFilePath mLogFilePath;
    private LogFilePath mLogFilePathGz;
    private SecorConfig mConfig;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        mLogFilePath = new LogFilePath("/some_parent_dir", PATH);
        mLogFilePathGz = new LogFilePath("/some_parent_dir", PATH_GZ);
    }

    private void setupSequenceFileReaderConfig() {
        PropertiesConfiguration properties = new PropertiesConfiguration();
        properties.addProperty("secor.file.reader.writer.factory",
                "com.pinterest.secor.io.impl.SequenceFileReaderWriterFactory");
        mConfig = new SecorConfig(properties);
    }

    private void setupDelimitedTextFileWriterConfig() {
        PropertiesConfiguration properties = new PropertiesConfiguration();
        properties.addProperty("secor.file.reader.writer.factory",
                "com.pinterest.secor.io.impl.DelimitedTextFileReaderWriterFactory");
        mConfig = new SecorConfig(properties);
    }

    private void mockDelimitedTextFileWriter(boolean isCompressed) throws Exception {
        PowerMockito.mockStatic(FileSystem.class);
        FileSystem fs = Mockito.mock(FileSystem.class);
        Mockito.when(
                FileSystem.get(Mockito.any(URI.class),
                        Mockito.any(Configuration.class))).thenReturn(fs);

        Path fsPath = (!isCompressed) ? new Path(PATH) : new Path(PATH_GZ);

        GzipCodec codec = PowerMockito.mock(GzipCodec.class);
        PowerMockito.whenNew(GzipCodec.class).withNoArguments()
                .thenReturn(codec);

        FSDataInputStream fileInputStream = Mockito
                .mock(FSDataInputStream.class);
        FSDataOutputStream fileOutputStream = Mockito
                .mock(FSDataOutputStream.class);

        Mockito.when(fs.open(fsPath)).thenReturn(fileInputStream);
        Mockito.when(fs.create(fsPath)).thenReturn(fileOutputStream);

        CompressionInputStream inputStream = Mockito
                .mock(CompressionInputStream.class);
        CompressionOutputStream outputStream = Mockito
                .mock(CompressionOutputStream.class);
        Mockito.when(codec.createInputStream(Mockito.any(InputStream.class)))
                .thenReturn(inputStream);

        Mockito.when(codec.createOutputStream(Mockito.any(OutputStream.class)))
                .thenReturn(outputStream);
    }

    private void mockSequenceFileWriter(boolean isCompressed)
            throws Exception {
        PowerMockito.mockStatic(FileSystem.class);
        FileSystem fs = Mockito.mock(FileSystem.class);
        Mockito.when(
                FileSystem.get(Mockito.any(URI.class),
                        Mockito.any(Configuration.class))).thenReturn(fs);

        Path fsPath = (!isCompressed) ? new Path(PATH) : new Path(PATH_GZ);

        SequenceFile.Reader reader = PowerMockito
                .mock(SequenceFile.Reader.class);
        PowerMockito
                .whenNew(SequenceFile.Reader.class)
                .withParameterTypes(FileSystem.class, Path.class,
                        Configuration.class)
                .withArguments(Mockito.eq(fs), Mockito.eq(fsPath),
                        Mockito.any(Configuration.class)).thenReturn(reader);

        Mockito.<Class<?>>when(reader.getKeyClass()).thenReturn(
                (Class<?>) LongWritable.class);
        Mockito.<Class<?>>when(reader.getValueClass()).thenReturn(
                (Class<?>) BytesWritable.class);

        if (!isCompressed) {
            PowerMockito.mockStatic(SequenceFile.class);
            SequenceFile.Writer writer = Mockito
                    .mock(SequenceFile.Writer.class);
            Mockito.when(
                    SequenceFile.createWriter(Mockito.eq(fs),
                            Mockito.any(Configuration.class),
                            Mockito.eq(fsPath), Mockito.eq(LongWritable.class),
                            Mockito.eq(BytesWritable.class)))
                    .thenReturn(writer);

            Mockito.when(writer.getLength()).thenReturn(123L);
        } else {
            PowerMockito.mockStatic(SequenceFile.class);
            SequenceFile.Writer writer = Mockito
                    .mock(SequenceFile.Writer.class);
            Mockito.when(
                    SequenceFile.createWriter(Mockito.eq(fs),
                            Mockito.any(Configuration.class),
                            Mockito.eq(fsPath), Mockito.eq(LongWritable.class),
                            Mockito.eq(BytesWritable.class),
                            Mockito.eq(SequenceFile.CompressionType.BLOCK),
                            Mockito.any(GzipCodec.class))).thenReturn(writer);

            Mockito.when(writer.getLength()).thenReturn(12L);
        }
    }

    public void testSequenceFileReader() throws Exception {
        setupSequenceFileReaderConfig();
        mockSequenceFileWriter(false);
        ReflectionUtil.createFileReader(mConfig.getFileReaderWriterFactory(), mLogFilePath, null);

        // Verify that the method has been called exactly once (the default).
        PowerMockito.verifyStatic();
        FileSystem.get(Mockito.any(URI.class), Mockito.any(Configuration.class));

        mockSequenceFileWriter(true);
        ReflectionUtil.createFileWriter(mConfig.getFileReaderWriterFactory(), mLogFilePathGz, new GzipCodec());

        // Verify that the method has been called exactly once (the default).
        PowerMockito.verifyStatic();
        FileSystem
                .get(Mockito.any(URI.class), Mockito.any(Configuration.class));
    }

    public void testSequenceFileWriter() throws Exception {
        setupSequenceFileReaderConfig();
        mockSequenceFileWriter(false);

        FileWriter writer = ReflectionUtil.createFileWriter(mConfig.getFileReaderWriterFactory(),
                mLogFilePath, null);

        // Verify that the method has been called exactly once (the default).
        PowerMockito.verifyStatic();
        FileSystem
                .get(Mockito.any(URI.class), Mockito.any(Configuration.class));

        assert writer.getLength() == 123L;

        mockSequenceFileWriter(true);

        writer = ReflectionUtil.createFileWriter(mConfig.getFileReaderWriterFactory(),
                mLogFilePathGz, new GzipCodec());

        // Verify that the method has been called exactly once (the default).
        PowerMockito.verifyStatic();
        FileSystem
                .get(Mockito.any(URI.class), Mockito.any(Configuration.class));

        assert writer.getLength() == 12L;
    }


    public void testDelimitedTextFileWriter() throws Exception {
        setupDelimitedTextFileWriterConfig();
        mockDelimitedTextFileWriter(false);
        FileWriter writer = (FileWriter) ReflectionUtil
                .createFileWriter(mConfig.getFileReaderWriterFactory(),
                        mLogFilePath, null
                );
        assert writer.getLength() == 0L;

        mockDelimitedTextFileWriter(true);
        writer = (FileWriter) ReflectionUtil
                .createFileWriter(mConfig.getFileReaderWriterFactory(),
                        mLogFilePathGz, new GzipCodec()
                );
        assert writer.getLength() == 0L;
    }

    public void testDelimitedTextFileReader() throws Exception {
        setupDelimitedTextFileWriterConfig();

        mockDelimitedTextFileWriter(false);

        ReflectionUtil.createFileReader(mConfig.getFileReaderWriterFactory(), mLogFilePath, null);

        mockDelimitedTextFileWriter(true);
        ReflectionUtil.createFileReader(mConfig.getFileReaderWriterFactory(), mLogFilePathGz, new GzipCodec());
    }
}