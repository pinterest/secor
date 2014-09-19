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

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

import org.apache.commons.configuration.PropertiesConfiguration;
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

import com.pinterest.secor.common.LogFilePath;
import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.io.impl.GzipFileReader;
import com.pinterest.secor.io.impl.SequenceFileReader;

import junit.framework.TestCase;

/**
 * Test the file readers
 * 
 * @author Praveen Murugesan (praveen@uber.com)
 *
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ FileSystem.class, GzipFileReader.class, SequenceFileReader.class })
public class FileReaderTest extends TestCase {
	
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

	private void setupSequenceFileReaderConfig() {
		PropertiesConfiguration properties = new PropertiesConfiguration();
		properties.addProperty("secor.file.reader",
				"com.pinterest.secor.io.impl.SequenceFileReader");
		mConfig = new SecorConfig(properties);
	}

	private void setupGzipFileReaderConfig() {
		PropertiesConfiguration properties = new PropertiesConfiguration();
		properties.addProperty("secor.file.reader",
				"com.pinterest.secor.io.impl.GzipFileReader");
		mConfig = new SecorConfig(properties);
	}

	public void testSequenceFileReader() throws Exception {
		setupSequenceFileReaderConfig();
		mockSequenceFileReader();

		FileReaderFactory.create(mLogFilePath, mConfig);

		// Verify that the method has been called exactly once (the default).
		PowerMockito.verifyStatic();
		FileSystem.get(Mockito.any(Configuration.class));
	}

	public void testGzipFileReader() throws Exception {
		setupGzipFileReaderConfig();
		mockGzipFileReader();
		FileReaderFactory.create(mLogFilePathGz, mConfig);
	}

	private void mockGzipFileReader() throws Exception {
		FileInputStream fileInputStreamMock = PowerMockito
				.mock(FileInputStream.class);
		
		GZIPInputStream gzipInputStreamMock = PowerMockito
				.mock(GZIPInputStream.class);

		PowerMockito.whenNew(FileInputStream.class)
				.withParameterTypes(String.class)
				.withArguments(Mockito.any(String.class))
				.thenReturn(fileInputStreamMock);
		
		PowerMockito.whenNew(GZIPInputStream.class)
		.withParameterTypes(InputStream.class)
		.withArguments(Mockito.any(FileInputStream.class))
		.thenReturn(gzipInputStreamMock);
	}

	private void mockSequenceFileReader() throws Exception {
		PowerMockito.mockStatic(FileSystem.class);
		FileSystem fs = Mockito.mock(FileSystem.class);
		Mockito.when(FileSystem.get(Mockito.any(Configuration.class)))
				.thenReturn(fs);
		
		Path fsPath = new Path(PATH);
		SequenceFile.Reader reader = PowerMockito.mock(SequenceFile.Reader.class);
		PowerMockito.whenNew(SequenceFile.Reader.class)
				.withParameterTypes(FileSystem.class, Path.class, Configuration.class)
				.withArguments(Mockito.eq(fs), Mockito.eq(fsPath), Mockito.any(Configuration.class))
				.thenReturn(reader);
		
		Mockito.<Class<?>>when(reader.getKeyClass()).thenReturn((Class<?>) LongWritable.class);
		Mockito.<Class<?>>when(reader.getValueClass()).thenReturn((Class<?>) BytesWritable.class);
	}
}
