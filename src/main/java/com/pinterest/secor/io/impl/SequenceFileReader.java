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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;

import com.pinterest.secor.common.LogFilePath;
import com.pinterest.secor.io.FileReader;
import com.pinterest.secor.io.KeyValue;

/**
 * 
 * Sequence file reader implementation
 * 
 * @author Praveen Murugesan (praveen@uber.com)
 *
 */
public class SequenceFileReader implements FileReader {

	private final SequenceFile.Reader reader;
	private final LongWritable key;
	private final BytesWritable value;
	
	// constructor
	public SequenceFileReader(LogFilePath path) throws Exception {
		Configuration config = new Configuration();
        FileSystem fs = FileSystem.get(config);
        String srcFilename = path.getLogFilePath();
        Path srcFsPath = new Path(srcFilename);
        this.reader = new SequenceFile.Reader(fs, srcFsPath, config);
        this.key = (LongWritable) reader.getKeyClass().newInstance();
        this.value = (BytesWritable) reader.getValueClass().newInstance();
	}
	
	@Override
	public KeyValue next() throws IOException {
		if(reader.next(key, value)) {
			return new KeyValue(key.get(), value.getBytes());
		} else {
			return null;
		}	
	}

	@Override
	public void close() throws IOException {
		this.reader.close();
	}

}
