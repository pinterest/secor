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
package com.pinterest.secor.common;

import com.pinterest.secor.io.FileWriter;
import com.pinterest.secor.util.FileUtil;
import com.pinterest.secor.util.ReflectionUtil;

import junit.framework.TestCase;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Collection;

/**
 * FileRegistryTest tests the file registry logic.
 *
 * @author Pawel Garbacki (pawel@pinterest.com)
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ FileRegistry.class, FileUtil.class, ReflectionUtil.class })
public class FileRegistryTest extends TestCase {
    private static final String PATH = "/some_parent_dir/some_topic/some_partition/some_other_partition/"
            + "10_0_00000000000000000100";
    private static final String PATH_GZ = "/some_parent_dir/some_topic/some_partition/some_other_partition/"
            + "10_0_00000000000000000100.gz";
    private static final String CRC_PATH = "/some_parent_dir/some_topic/some_partition/some_other_partition/"
            + ".10_0_00000000000000000100.crc";
    private LogFilePath mLogFilePath;
    private LogFilePath mLogFilePathGz;
    private TopicPartition mTopicPartition;
    private FileRegistry mRegistry;

    public void setUp() throws Exception {
        super.setUp();
        PropertiesConfiguration properties = new PropertiesConfiguration();
        properties.addProperty("secor.file.reader.writer.factory",
                "com.pinterest.secor.io.impl.SequenceFileReaderWriterFactory");
        SecorConfig secorConfig = new SecorConfig(properties);
        mRegistry = new FileRegistry(secorConfig);
        mLogFilePath = new LogFilePath("/some_parent_dir", PATH);
        mTopicPartition = new TopicPartition("some_topic", 0);
        mLogFilePathGz = new LogFilePath("/some_parent_dir", PATH_GZ);
    }

    private void createWriter() throws Exception {
        PowerMockito.mockStatic(FileUtil.class);

        PowerMockito.mockStatic(ReflectionUtil.class);
        FileWriter writer = Mockito.mock(FileWriter.class);
        Mockito.when(
                ReflectionUtil.createFileWriter(
                        Mockito.any(String.class),
                        Mockito.any(LogFilePath.class),
                        Mockito.any(CompressionCodec.class)
                ))
                .thenReturn(writer);

        Mockito.when(writer.getLength()).thenReturn(123L);

        FileWriter createdWriter = mRegistry.getOrCreateWriter(
                mLogFilePath, null);
        assertTrue(createdWriter == writer);
    }

    public void testGetOrCreateWriter() throws Exception {
        createWriter();

        // Call the method again. This time it should return an existing writer.
        mRegistry.getOrCreateWriter(mLogFilePath, null);

        // Verify that the method has been called exactly once (the default).
        PowerMockito.verifyStatic();
        ReflectionUtil.createFileWriter(Mockito.any(String.class),
                Mockito.any(LogFilePath.class),
                Mockito.any(CompressionCodec.class)
        );

        PowerMockito.verifyStatic();
        FileUtil.delete(PATH);
        PowerMockito.verifyStatic();
        FileUtil.delete(CRC_PATH);

        TopicPartition topicPartition = new TopicPartition("some_topic", 0);
        Collection<TopicPartition> topicPartitions = mRegistry
                .getTopicPartitions();
        assertEquals(1, topicPartitions.size());
        assertTrue(topicPartitions.contains(topicPartition));

        Collection<LogFilePath> logFilePaths = mRegistry
                .getPaths(topicPartition);
        assertEquals(1, logFilePaths.size());
        assertTrue(logFilePaths.contains(mLogFilePath));
    }

    private void createCompressedWriter() throws Exception {
        PowerMockito.mockStatic(FileUtil.class);

        PowerMockito.mockStatic(ReflectionUtil.class);
        FileWriter writer = Mockito.mock(FileWriter.class);
        Mockito.when(
                ReflectionUtil.createFileWriter(
                        Mockito.any(String.class),
                        Mockito.any(LogFilePath.class),
                        Mockito.any(CompressionCodec.class)
                ))
                .thenReturn(writer);

        Mockito.when(writer.getLength()).thenReturn(123L);

        FileWriter createdWriter = mRegistry.getOrCreateWriter(
                mLogFilePathGz, new GzipCodec());
        assertTrue(createdWriter == writer);
    }

    public void testGetOrCreateWriterCompressed() throws Exception {
        createCompressedWriter();

        mRegistry.getOrCreateWriter(mLogFilePathGz, new GzipCodec());

        // Verify that the method has been called exactly once (the default).
        PowerMockito.verifyStatic();
        FileUtil.delete(PATH_GZ);
        PowerMockito.verifyStatic();
        FileUtil.delete(CRC_PATH);

        PowerMockito.verifyStatic();
        ReflectionUtil.createFileWriter(Mockito.any(String.class),
                Mockito.any(LogFilePath.class),
                Mockito.any(CompressionCodec.class)
        );

        TopicPartition topicPartition = new TopicPartition("some_topic", 0);
        Collection<TopicPartition> topicPartitions = mRegistry
                .getTopicPartitions();
        assertEquals(1, topicPartitions.size());
        assertTrue(topicPartitions.contains(topicPartition));

        Collection<LogFilePath> logFilePaths = mRegistry
                .getPaths(topicPartition);
        assertEquals(1, logFilePaths.size());
        assertTrue(logFilePaths.contains(mLogFilePath));
    }

    public void testDeletePath() throws Exception {
        createWriter();

        PowerMockito.mockStatic(FileUtil.class);

        mRegistry.deletePath(mLogFilePath);
        PowerMockito.verifyStatic();
        FileUtil.delete(PATH);
        PowerMockito.verifyStatic();
        FileUtil.delete(CRC_PATH);

        assertTrue(mRegistry.getPaths(mTopicPartition).isEmpty());
        assertTrue(mRegistry.getTopicPartitions().isEmpty());
    }

    public void testDeleteTopicPartition() throws Exception {
        createWriter();

        PowerMockito.mockStatic(FileUtil.class);

        mRegistry.deleteTopicPartition(mTopicPartition);
        PowerMockito.verifyStatic();
        FileUtil.delete(PATH);
        PowerMockito.verifyStatic();
        FileUtil.delete(CRC_PATH);

        assertTrue(mRegistry.getTopicPartitions().isEmpty());
        assertTrue(mRegistry.getPaths(mTopicPartition).isEmpty());
    }

    public void testGetSize() throws Exception {
        createWriter();

        assertEquals(123L, mRegistry.getSize(mTopicPartition));
    }

    public void testGetModificationAgeSec() throws Exception {
        PowerMockito.mockStatic(System.class);
        PowerMockito.when(System.currentTimeMillis()).thenReturn(10000L)
                .thenReturn(100000L);
        createWriter();

        assertEquals(90, mRegistry.getModificationAgeSec(mTopicPartition));
    }
}
