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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;

import com.google.common.io.CountingOutputStream;
import com.pinterest.secor.common.LogFilePath;
import com.pinterest.secor.io.FileReaderWriter;
import com.pinterest.secor.io.KeyValue;
import com.pinterest.secor.util.FileUtil;

/**
 * 
 * Delimited Text File Reader Writer with Compression
 * 
 * @author Praveen Murugesan (praveen@uber.com)
 *
 */
public class DelimitedTextFileReaderWriter extends FileReaderWriter {

    // delimiter used between messages
    private static final byte DELIMITER = '\n';

    private final CountingOutputStream mCountingStream;
    private final BufferedOutputStream mWriter;

    private final BufferedInputStream mReader;
    private long mOffset;

    // constructor
    public DelimitedTextFileReaderWriter(LogFilePath path,
            CompressionCodec codec, FileReaderWriter.Type type)
            throws FileNotFoundException, IOException {

        Path fsPath = new Path(path.getLogFilePath());
        FileSystem fs = FileUtil.getFileSystem(path.getLogFilePath());
        if (type == FileReaderWriter.Type.Reader) {
            InputStream inputStream = fs.open(fsPath);
            this.mReader = (codec == null) ? new BufferedInputStream(inputStream)
                    : new BufferedInputStream(
                            codec.createInputStream(inputStream));
            this.mOffset = path.getOffset();
            this.mCountingStream = null;
            this.mWriter = null;
        } else if (type == FileReaderWriter.Type.Writer) {
            this.mCountingStream = new CountingOutputStream(fs.create(fsPath));
            this.mWriter = (codec == null) ? new BufferedOutputStream(
                    this.mCountingStream) : new BufferedOutputStream(
                    codec.createOutputStream(this.mCountingStream));
            this.mReader = null;
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
        assert this.mCountingStream != null;
        return this.mCountingStream.getCount();
    }

    @Override
    public void write(KeyValue keyValue) throws IOException {
        assert this.mWriter != null;
        this.mWriter.write(keyValue.getValue());
        this.mWriter.write(DELIMITER);
    }

    @Override
    public KeyValue next() throws IOException {
        assert this.mReader != null;
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

}