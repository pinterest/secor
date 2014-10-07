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
import com.pinterest.secor.io.FileReaderWriter;
import com.pinterest.secor.io.KeyValue;

/**
 * 
 * Sequence file writer implementation
 * 
 * @author Praveen Murugesan (praveen@uber.com)
 *
 */
public class SequenceFileReaderWriter implements FileReaderWriter {

    private final SequenceFile.Writer writer;
    private final SequenceFile.Reader reader;
    private final LongWritable key;
    private final BytesWritable value;

    // constructor
    public SequenceFileReaderWriter(LogFilePath path, CompressionCodec codec,
            FileReaderWriter.Type type) throws Exception {
        Configuration config = new Configuration();
        FileSystem fs = FileSystem.get(config);
        Path fsPath = new Path(path.getLogFilePath());

        if (type == FileReaderWriter.Type.Reader) {
            this.reader = new SequenceFile.Reader(fs, fsPath, config);
            this.key = (LongWritable) reader.getKeyClass().newInstance();
            this.value = (BytesWritable) reader.getValueClass().newInstance();
            this.writer = null;
        } else if (type == FileReaderWriter.Type.Writer) {
            if (codec != null) {
                this.writer = SequenceFile.createWriter(fs, config, fsPath,
                        LongWritable.class, BytesWritable.class,
                        SequenceFile.CompressionType.BLOCK, codec);
            } else {
                this.writer = SequenceFile.createWriter(fs, config, fsPath,
                        LongWritable.class, BytesWritable.class);
            }
            this.reader = null;
            this.key = null;
            this.value = null;
        } else {
            throw new IllegalArgumentException("Undefined File Type: " + type);
        }

    }

    @Override
    public void close() throws IOException {
        if (this.writer == null) {
            this.writer.close();
        }
        if (this.reader == null) {
            this.reader.close();
        }
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

    @Override
    public KeyValue next() throws IOException {
        if (reader.next(key, value)) {
            return new KeyValue(key.get(), value.getBytes());
        } else {
            return null;
        }
    }

}
