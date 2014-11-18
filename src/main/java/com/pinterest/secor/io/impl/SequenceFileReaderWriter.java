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
import com.pinterest.secor.util.FileUtil;

/**
 * 
 * Sequence file reader writer implementation
 * 
 * @author Praveen Murugesan (praveen@uber.com)
 *
 */
public class SequenceFileReaderWriter implements FileReaderWriter {

    private final SequenceFile.Writer mWriter;
    private final SequenceFile.Reader mReader;
    private final LongWritable mKey;
    private final BytesWritable mValue;

    // constructor
    public SequenceFileReaderWriter(LogFilePath path, CompressionCodec codec,
            FileReaderWriter.Type type) throws Exception {
        Configuration config = new Configuration();
        Path fsPath = new Path(path.getLogFilePath());
        FileSystem fs = FileUtil.getFileSystem(path.getLogFilePath());

        if (type == FileReaderWriter.Type.Reader) {
            this.mReader = new SequenceFile.Reader(fs, fsPath, config);
            this.mKey = (LongWritable) mReader.getKeyClass().newInstance();
            this.mValue = (BytesWritable) mReader.getValueClass().newInstance();
            this.mWriter = null;
        } else if (type == FileReaderWriter.Type.Writer) {
            if (codec != null) {
                this.mWriter = SequenceFile.createWriter(fs, config, fsPath,
                        LongWritable.class, BytesWritable.class,
                        SequenceFile.CompressionType.BLOCK, codec);
            } else {
                this.mWriter = SequenceFile.createWriter(fs, config, fsPath,
                        LongWritable.class, BytesWritable.class);
            }
            this.mReader = null;
            this.mKey = null;
            this.mValue = null;
        } else {
            throw new IllegalArgumentException("Undefined File Type: " + type);
        }

    }

    @Override
    public void close() throws IOException {
        if (this.mWriter != null) {
            this.mWriter.close();
        }
        if (this.mReader != null) {
            this.mReader.close();
        }
    }

    @Override
    public long getLength() throws IOException {
        return this.mWriter.getLength();
    }

    @Override
    public void write(KeyValue keyValue) throws IOException {
        LongWritable writeableKey = new LongWritable(keyValue.getKey());
        BytesWritable writeableValue = new BytesWritable(keyValue.getValue());
        this.mWriter.append(writeableKey, writeableValue);
    }

    @Override
    public KeyValue next() throws IOException {
        if (mReader.next(mKey, mValue)) {
            return new KeyValue(mKey.get(), mValue.getBytes());
        } else {
            return null;
        }
    }

}