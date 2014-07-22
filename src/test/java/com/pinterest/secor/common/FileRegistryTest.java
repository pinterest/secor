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

import java.io.IOException;
import java.util.Collection;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.pinterest.secor.storage.Writer;
import com.pinterest.secor.storage.seqfile.HadoopSequenceFileStorageFactory;
import com.pinterest.secor.util.FileUtil;

/**
 * FileRegistryTest tests the file registry logic.
 * 
 * @author Pawel Garbacki (pawel@pinterest.com)
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ FileRegistry.class, FileSystem.class, FileUtil.class,
		SequenceFile.class })
public class FileRegistryTest extends TestCase {
	private static final String PATH = "/some_parent_dir/some_topic/some_partition/some_other_partition/"
			+ "10_0_00000000000000000100";
	private static final String CRC_PATH = "/some_parent_dir/some_topic/some_partition/some_other_partition/"
			+ ".10_0_00000000000000000100.crc";
	private LogFilePath mLogFilePath;
	private TopicPartition mTopicPartition;
	private FileRegistry mRegistry;

	private HadoopSequenceFileStorageFactory mHadoopWriterFactory;

	public void setUp() throws Exception {
		super.setUp();
		mLogFilePath = new LogFilePath("/some_parent_dir", PATH);
		mTopicPartition = new TopicPartition("some_topic", 0);
		mRegistry = new FileRegistry();
		mHadoopWriterFactory = new HadoopSequenceFileStorageFactory();
	}

	private void sequenceFileCreateWriter() throws IOException {
		PowerMockito.mockStatic(FileUtil.class);

		PowerMockito.mockStatic(FileSystem.class);
		FileSystem fs = Mockito.mock(FileSystem.class);
		Mockito.when(FileSystem.get(Mockito.any(Configuration.class)))
				.thenReturn(fs);

		PowerMockito.mockStatic(SequenceFile.class);
		Path fsPath = new Path(PATH);
		SequenceFile.Writer writer = Mockito.mock(SequenceFile.Writer.class);
		Mockito.when(
				SequenceFile.createWriter(Mockito.eq(fs),
						Mockito.any(Configuration.class), Mockito.eq(fsPath),
						Mockito.eq(LongWritable.class),
						Mockito.eq(BytesWritable.class))).thenReturn(writer);

		Mockito.when(writer.getLength()).thenReturn(123L);

		Writer createdWriter = mRegistry.getOrCreateWriter(
				mHadoopWriterFactory, mLogFilePath);
		assertTrue(createdWriter.getImpl() == writer);
	}

	public void testGetOrCreateWriter() throws Exception {
		sequenceFileCreateWriter();

		// Call the method again. This time it should return an existing writer.
		mRegistry.getOrCreateWriter(mHadoopWriterFactory, mLogFilePath);

		// Verify that the method has been called exactly once (the default).
		PowerMockito.verifyStatic();
		FileSystem.get(Mockito.any(Configuration.class));

		PowerMockito.verifyStatic();
		FileUtil.delete(PATH);
		PowerMockito.verifyStatic();
		FileUtil.delete(CRC_PATH);

		Path fsPath = new Path(PATH);
		PowerMockito.verifyStatic();
		SequenceFile
				.createWriter(Mockito.any(FileSystem.class),
						Mockito.any(Configuration.class), Mockito.eq(fsPath),
						Mockito.eq(LongWritable.class),
						Mockito.eq(BytesWritable.class));

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
		sequenceFileCreateWriter();

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
		sequenceFileCreateWriter();

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
		sequenceFileCreateWriter();

		assertEquals(123L, mRegistry.getSize(mTopicPartition));
	}

	public void testGetModificationAgeSec() throws Exception {
		PowerMockito.mockStatic(System.class);
		PowerMockito.when(System.currentTimeMillis()).thenReturn(10000L)
				.thenReturn(100000L);
		sequenceFileCreateWriter();

		assertEquals(90, mRegistry.getModificationAgeSec(mTopicPartition));
	}
}
