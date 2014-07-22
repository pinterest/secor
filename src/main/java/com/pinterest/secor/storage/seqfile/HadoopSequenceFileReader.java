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
package com.pinterest.secor.storage.seqfile;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;

import com.pinterest.secor.storage.Reader;

public class HadoopSequenceFileReader implements Reader {

	private final org.apache.hadoop.io.SequenceFile.Reader reader;
	private final LongWritable key;
	private final BytesWritable value;

	public HadoopSequenceFileReader(
			org.apache.hadoop.io.SequenceFile.Reader reader) throws Exception {
		this.reader = reader;
		key = (LongWritable) reader.getKeyClass().newInstance();
		value = (BytesWritable) reader.getValueClass().newInstance();
	}

	@Override
	public boolean next() throws IOException {
		return reader.next(key, value);
	}

	@Override
	public long getOffset() {
		return key.get();
	}

	@Override
	public byte[] getBytes() {
		return value.getBytes();
	}

	@Override
	public void close() throws IOException {
		reader.close();
	}
}
