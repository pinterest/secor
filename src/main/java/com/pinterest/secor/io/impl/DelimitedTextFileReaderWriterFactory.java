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
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import com.pinterest.secor.io.FileReader;
import com.pinterest.secor.io.FileReaderWriterFactory;
import com.pinterest.secor.io.FileWriter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
            throws IllegalAccessException, IOException, InstantiationException {
        return new DelimitedTextFileReader(logFilePath, codec);
    }

    @Override
    public FileWriter BuildFileWriter(LogFilePath logFilePath, CompressionCodec codec) throws IOException {
        return new DelimitedTextFileWriter(logFilePath, codec);
    }

    protected class DelimitedTextFileReader implements FileReader {
        private final BufferedInputStream mReader;
        private long mOffset;
        private Decompressor mDecompressor = null;

        public DelimitedTextFileReader(LogFilePath path, CompressionCodec codec) throws IOException {
            Path fsPath = new Path(path.getLogFilePath());
            FileSystem fs = FileUtil.getFileSystem(path.getLogFilePath());
            InputStream inputStream = fs.open(fsPath);
            this.mReader = (codec == null) ? new BufferedInputStream(inputStream)
                    : new BufferedInputStream(
                    codec.createInputStream(inputStream,
                                            mDecompressor = CodecPool.getDecompressor(codec)));
            this.mOffset = path.getOffset();
        }

        @Override
        public KeyValue next() throws IOException {
            ByteArrayOutputStream messageBuffer = new ByteArrayOutputStream();
            int nextByte;
            while ((nextByte = mReader.read()) != DELIMITER) {
                if (nextByte == -1) { // end of stream?
                    if (messageBuffer.size() == 0) { // if no byte read
                        return null;
                    } else { // if bytes followed by end of stream: framing error
                        throw new EOFException(
                                "Non-empty message without delimiter");
                    }
                }
                messageBuffer.write(nextByte);
            }
            return new KeyValue(this.mOffset++, messageBuffer.toByteArray());
        }

        @Override
        public void close() throws IOException {
            this.mReader.close();
            CodecPool.returnDecompressor(mDecompressor);
            mDecompressor = null;
        }
    }

    protected class DelimitedTextFileWriter implements FileWriter {
        private final CountingOutputStream mCountingStream;
        private final BufferedOutputStream mWriter;
        private Compressor mCompressor = null;

        public DelimitedTextFileWriter(LogFilePath path, CompressionCodec codec) throws IOException {
            Path fsPath = new Path(path.getLogFilePath());
            FileSystem fs = FileUtil.getFileSystem(path.getLogFilePath());
            this.mCountingStream = new CountingOutputStream(fs.create(fsPath));
            this.mWriter = (codec == null) ? new BufferedOutputStream(
                    this.mCountingStream) : new BufferedOutputStream(
                    codec.createOutputStream(this.mCountingStream,
                                             mCompressor = CodecPool.getCompressor(codec)));
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
        }

        @Override
        public void close() throws IOException {
            this.mWriter.close();
            CodecPool.returnCompressor(mCompressor);
            mCompressor = null;
        }
    }
}
