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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;

import com.pinterest.secor.storage.Reader;
import com.pinterest.secor.storage.StorageFactory;
import com.pinterest.secor.storage.Writer;

/**
 * Storages messages to Hadoop Sequence File format.
 * 
 * @author Leonardo Noleto (noleto.leonardo@gmail.com)
 * 
 */
public class HadoopSequenceFileStorageFactory implements StorageFactory {

	@Override
	public Writer createWriter(Path fsPath) throws IOException {

		Configuration config = new Configuration();
		FileSystem fs = FileSystem.get(config);

		SequenceFile.Writer writer = SequenceFile.createWriter(fs, config,
				fsPath, LongWritable.class, BytesWritable.class);

		return new HadoopSequenceFileWriter(writer);
	}

	@Override
	public Reader createReader(Path path) throws Exception {
		Configuration config = new Configuration();
		FileSystem fs = FileSystem.get(config);
		return new HadoopSequenceFileReader(new SequenceFile.Reader(fs, path,
				config));
	}

	@Override
	public boolean supportsTrim() {
		return true;
	}
}
