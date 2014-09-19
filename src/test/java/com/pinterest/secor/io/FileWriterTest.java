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

import java.io.FileOutputStream;
import java.io.IOException;

import junit.framework.TestCase;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.GzipCodec;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.pinterest.secor.common.LogFilePath;
import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.io.impl.GzipFileWriter;

/**
 * 
 * Test the file writers
 * 
 * @author Praveen Murugesan (praveen@uber.com)
 *
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ FileSystem.class, SequenceFile.class, GzipFileWriter.class })
public class FileWriterTest extends TestCase {

	private static final String PATH = "/some_parent_dir/some_topic/some_partition/some_other_partition/"
			+ "10_0_00000000000000000100";
	private static final String PATH_GZ = "/some_parent_dir/some_topic/some_partition/some_other_partition/"
			+ "10_0_00000000000000000100.gz";

	private LogFilePath mLogFilePath;
	private LogFilePath mLogFilePathGz;
	private SecorConfig mConfig;

	@Override
	public void setUp() throws Exception {
		super.setUp();
		mLogFilePath = new LogFilePath("/some_parent_dir", PATH);
		mLogFilePathGz = new LogFilePath("/some_parent_dir", PATH_GZ);
	}

	private void setupSequenceFileWriterConfig() {
		PropertiesConfiguration properties = new PropertiesConfiguration();
		properties.addProperty("secor.file.writer",
				"com.pinterest.secor.io.impl.SequenceFileWriter");
		mConfig = new SecorConfig(properties);
	}

	private void setupGzipFileWriterConfig() {
		PropertiesConfiguration properties = new PropertiesConfiguration();
		properties.addProperty("secor.file.writer",
				"com.pinterest.secor.io.impl.GzipFileWriter");
		mConfig = new SecorConfig(properties);
	}

	public void testSequenceFileWriter() throws Exception {
		setupSequenceFileWriterConfig();
		mockSequenceFileWriter();

		FileWriter writer = FileWriterFactory.create(mLogFilePath, null,
				mConfig);

		// Verify that the method has been called exactly once (the default).
		PowerMockito.verifyStatic();
		FileSystem.get(Mockito.any(Configuration.class));

		assert writer.getLength() == 123L;

		mockSequenceCompressedFileWriter();

		FileWriterFactory.create(mLogFilePathGz, new GzipCodec(), mConfig);

		// Verify that the method has been called exactly once (the default).
		PowerMockito.verifyStatic();
		FileSystem.get(Mockito.any(Configuration.class));

		assert writer.getLength() == 12L;
	}

	public void testGzipFileWriter() throws Exception {
		setupGzipFileWriterConfig();
		mockGzipFileWriter();
		FileWriter writer = FileWriterFactory.create(mLogFilePathGz, null,
				mConfig);
		assert writer.getLength() == 0L;
	}

	private void mockGzipFileWriter() throws Exception {
		FileOutputStream fileOutputStreamMock = PowerMockito
				.mock(FileOutputStream.class);

		PowerMockito.whenNew(FileOutputStream.class)
				.withParameterTypes(String.class)
				.withArguments(Mockito.any(String.class))
				.thenReturn(fileOutputStreamMock);
	}

	private void mockSequenceFileWriter() throws IOException {
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
	}

	private void mockSequenceCompressedFileWriter() throws IOException {
		PowerMockito.mockStatic(FileSystem.class);
		FileSystem fs = Mockito.mock(FileSystem.class);
		Mockito.when(FileSystem.get(Mockito.any(Configuration.class)))
				.thenReturn(fs);

		PowerMockito.mockStatic(SequenceFile.class);
		Path fsPath = new Path(PATH_GZ);
		SequenceFile.Writer writer = Mockito.mock(SequenceFile.Writer.class);
		Mockito.when(
				SequenceFile.createWriter(Mockito.eq(fs),
						Mockito.any(Configuration.class), Mockito.eq(fsPath),
						Mockito.eq(LongWritable.class),
						Mockito.eq(BytesWritable.class),
						Mockito.eq(SequenceFile.CompressionType.BLOCK),
						Mockito.any(GzipCodec.class))).thenReturn(writer);

		Mockito.when(writer.getLength()).thenReturn(12L);
	}
}
