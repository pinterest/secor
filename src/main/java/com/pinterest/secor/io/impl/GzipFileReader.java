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

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;

import com.pinterest.secor.common.LogFilePath;
import com.pinterest.secor.io.FileReader;
import com.pinterest.secor.io.KeyValue;

/**
 * Gzip File reader implementation
 * 
 * @author Praveen Murugesan (praveen@uber.com)
 *
 */
public class GzipFileReader implements FileReader {
	
	private static final byte DELIMITER = '\n';
	
	private final BufferedInputStream reader;
	private long offset;
	
	// constructor
	public GzipFileReader(LogFilePath path) throws Exception {
		reader = new BufferedInputStream(new GZIPInputStream(new FileInputStream(new File(path.getLogFilePath()))));
		this.offset = path.getOffset();
	}
	
	@Override
	public KeyValue next() throws IOException {
		ByteArrayOutputStream messageBuffer = new ByteArrayOutputStream();
		int nextByte;
		while ((nextByte = reader.read()) != DELIMITER) {
		      if (nextByte == -1) { // end of stream?
		        if (messageBuffer.size() == 0) { // if no byte read
		          return null;
		        } else { // if bytes followed by end of stream: framing error
		          throw new EOFException("Non-empty message without delimiter");
		        }
		      }
		      messageBuffer.write(nextByte);
		}
		return new KeyValue(this.offset++, messageBuffer.toByteArray());
	}

	@Override
	public void close() throws IOException {
		this.reader.close();
	}

}
