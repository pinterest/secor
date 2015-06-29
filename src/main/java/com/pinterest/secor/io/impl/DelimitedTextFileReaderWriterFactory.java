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

import java.io.DataInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;

import com.pinterest.secor.io.FileReader;
import com.pinterest.secor.io.FileReaderWriterFactory;
import com.pinterest.secor.io.FileWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.Decompressor;

import com.google.common.io.CountingOutputStream;
import com.pinterest.secor.common.LogFilePath;
import com.pinterest.secor.io.KeyValue;
import com.pinterest.secor.util.FileUtil;

/**
 * Delimited Text File Reader Writer with Compression
 *
 * @author Praveen Murugesan (praveen@uber.com)
 */
public class DelimitedTextFileReaderWriterFactory implements FileReaderWriterFactory {
    private static final byte DELIMITER = '\n';

    @Override
    public FileReader BuildFileReader(LogFilePath logFilePath, CompressionCodec codec)
            throws Exception {
        return new DelimitedTextFileReader(logFilePath, codec);
    }

    @Override
    public FileWriter BuildFileWriter(LogFilePath logFilePath, CompressionCodec codec) throws IOException {
        return new DelimitedTextFileWriter(logFilePath, codec);
    }

    protected class DelimitedTextFileReader implements FileReader {
        private final SequenceFile.Reader mOffsetReader;
        private final LongWritable mKey;
        private final IntWritable mValueLength;
        private final DataInputStream mReader;
        private Decompressor mDecompressor = null;
        private byte[] mDelimiter;

        public DelimitedTextFileReader(LogFilePath path, CompressionCodec codec) throws Exception {
            Configuration config = new Configuration();
            Path fsPath = new Path(path.getLogFilePath());
            FileSystem fs = FileUtil.getFileSystem(path.getLogFilePath());
            DataInputStream inputStream = fs.open(fsPath);
            Path offsetPath = new Path(path.getLogFileOffsetPath());
            this.mOffsetReader = new SequenceFile.Reader(fs, offsetPath, config);
            this.mReader = (codec == null) ? inputStream
                    : new DataInputStream(
                    codec.createInputStream(inputStream,
                                            mDecompressor = CodecPool.getDecompressor(codec)));

            this.mKey = (LongWritable) mOffsetReader.getKeyClass().newInstance();
            this.mValueLength = (IntWritable) mOffsetReader.getValueClass().newInstance();
            this.mDelimiter = new byte[1];
        }

        @Override
        public KeyValue next() throws IOException {
            if (mOffsetReader.next(mKey, mValueLength)) {
                byte[] buffer = new byte[mValueLength.get()];
                mReader.readFully(buffer);
                mReader.readFully(mDelimiter);
                assert mDelimiter[0] == DELIMITER;
                return new KeyValue(mKey.get(), buffer);
            } else {
                return null;
            }
        }

        @Override
        public void close() throws IOException {
            this.mReader.close();
            this.mOffsetReader.close();
            CodecPool.returnDecompressor(mDecompressor);
            mDecompressor = null;
        }
    }

    protected class DelimitedTextFileWriter implements FileWriter {
        private final CountingOutputStream mCountingStream;
        private final SequenceFile.Writer mOffsetWriter;
        private final LongWritable mKey;
        private final IntWritable mValueLength;
        private final BufferedOutputStream mWriter;
        private Compressor mCompressor = null;

        public DelimitedTextFileWriter(LogFilePath path, CompressionCodec codec) throws IOException {
            Configuration config = new Configuration();
            Path fsPath = new Path(path.getLogFilePath());
            FileSystem fs = FileUtil.getFileSystem(path.getLogFilePath());
            this.mCountingStream = new CountingOutputStream(fs.create(fsPath));
            Path offsetPath = new Path(path.getLogFileOffsetPath());
            this.mOffsetWriter = SequenceFile.createWriter(fs, config, offsetPath,
                    LongWritable.class, IntWritable.class);
            this.mWriter = (codec == null) ? new BufferedOutputStream(
                    this.mCountingStream) : new BufferedOutputStream(
                    codec.createOutputStream(this.mCountingStream,
                                             mCompressor = CodecPool.getCompressor(codec)));
            this.mKey = new LongWritable();
            this.mValueLength = new IntWritable();
        }

        @Override
        public long getLength() throws IOException {
            assert this.mCountingStream != null;
            return this.mCountingStream.getCount();
        }

        @Override
        public void write(KeyValue keyValue) throws IOException {
            this.mWriter.write(keyValue.getValue());
            this.mWriter.write(DELIMITER);
            this.mKey.set(keyValue.getOffset());
            this.mValueLength.set(keyValue.getValue().length);
            this.mOffsetWriter.append(this.mKey, this.mValueLength);
        }

        @Override
        public void close() throws IOException {
            this.mOffsetWriter.close();
            this.mWriter.close();
            CodecPool.returnCompressor(mCompressor);
            mCompressor = null;
        }
    }
}
