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
import org.apache.hadoop.io.SequenceFile;

import com.pinterest.secor.message.ParsedMessage;
import com.pinterest.secor.storage.Writer;

public class HadoopSequenceFileWriter implements Writer {

	private final SequenceFile.Writer mBackedWriter;

	public HadoopSequenceFileWriter(SequenceFile.Writer writer) {
		this.mBackedWriter = writer;
	}

	@Override
	public void close() throws IOException {
		mBackedWriter.close();
	}

	@Override
	public long getLength() throws IOException {
		return mBackedWriter.getLength();
	}

	@Override
	public void append(final ParsedMessage message) throws IOException {
		LongWritable key = new LongWritable(message.getOffset());
		BytesWritable value = new BytesWritable(message.getPayload());
		mBackedWriter.append(key, value);
	}

	public Object getImpl() {
		return mBackedWriter;
	}
}
