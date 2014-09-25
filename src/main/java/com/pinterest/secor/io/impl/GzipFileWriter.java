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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.GZIPOutputStream;

import com.google.common.io.CountingOutputStream;
import com.pinterest.secor.common.LogFilePath;
import com.pinterest.secor.io.FileWriter;

/**
 * 
 * Gzip File writer implementation
 * 
 * @author Praveen Murugesan (praveen@uber.com)
 *
 */
public class GzipFileWriter implements FileWriter {
	
	//delimiter used between messages
	private static final byte DELIMITER = '\n';
	
	private final CountingOutputStream countingStream;
	private final BufferedOutputStream writer;

	// constructor
	public GzipFileWriter(LogFilePath path) throws FileNotFoundException,
			IOException {
		File logFile = new File(path.getLogFilePath());
		logFile.getParentFile().mkdirs();
		this.countingStream = new CountingOutputStream(new FileOutputStream(logFile));
		this.writer = new BufferedOutputStream(new GZIPOutputStream(this.countingStream));
	}

	@Override
	public void close() throws IOException {
		this.writer.close();
	}

	@Override
	public long getLength() throws IOException {
		return this.countingStream.getCount();
	}

	@Override
	public void write(long key, byte[] value) throws IOException {
		this.writer.write(value);
		this.writer.write(DELIMITER);
	}

}
