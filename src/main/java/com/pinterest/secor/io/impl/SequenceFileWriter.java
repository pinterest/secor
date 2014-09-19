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
import org.apache.hadoop.io.compress.CompressionCodec;

import com.pinterest.secor.common.LogFilePath;
import com.pinterest.secor.io.FileWriter;

/**
 * 
 * Sequence file writer implementation
 * 
 * @author Praveen Murugesan (praveen@uber.com)
 *
 */
public class SequenceFileWriter implements FileWriter {
	
	private final SequenceFile.Writer writer;
	
	// constructor
	public SequenceFileWriter(LogFilePath path, CompressionCodec codec) throws IOException {
		Configuration config = new Configuration();
        FileSystem fs = FileSystem.get(config);
		if (codec != null) {
            Path fsPath = new Path(path.getLogFilePath());
            this.writer = SequenceFile.createWriter(fs, config, fsPath, LongWritable.class,
                    BytesWritable.class,
                    SequenceFile.CompressionType.BLOCK, codec);
        } else {
            Path fsPath = new Path(path.getLogFilePath());
            this.writer = SequenceFile.createWriter(fs, config, fsPath, LongWritable.class,
                    BytesWritable.class);
        }
	}
	
	@Override
	public void close() throws IOException {
		this.writer.close();
	}
	
	@Override
	public long getLength() throws IOException {
		return this.writer.getLength();
	}

	@Override
	public void write(long key, byte[] value) throws IOException {
		LongWritable writeableKey = new LongWritable(key);
        BytesWritable writeableValue = new BytesWritable(value);
		this.writer.append(writeableKey, writeableValue);
	}

}
