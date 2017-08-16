////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/*
 * Copyright © 2017 Unified Social, Inc.
 * 180 Madison Avenue, 23rd Floor, New York, NY 10016, U.S.A.
 * All rights reserved.
 *
 * This software (the "Software") is provided pursuant to the license agreement you entered into with Unified Social,
 * Inc. (the "License Agreement").  The Software is the confidential and proprietary information of Unified Social,
 * Inc., and you shall use it only in accordance with the terms and conditions of the License Agreement.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND "AS AVAILABLE."  UNIFIED SOCIAL, INC. MAKES NO WARRANTIES OF ANY KIND, WHETHER
 * EXPRESS OR IMPLIED, INCLUDING, BUT NOT LIMITED TO THE IMPLIED WARRANTIES AND CONDITIONS OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NON-INFRINGEMENT.
 */

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

package com.unified.secor.io.impl;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;

import com.google.common.base.Charsets;
import com.google.common.io.CountingOutputStream;
import com.pinterest.secor.common.LogFilePath;
import com.pinterest.secor.io.FileReader;
import com.pinterest.secor.io.FileReaderWriterFactory;
import com.pinterest.secor.io.FileWriter;
import com.pinterest.secor.io.KeyValue;
import com.pinterest.secor.util.FileUtil;
import com.unified.utils.csv.CSV;
import com.unified.utils.csv.CSVWriter;

public class DelimitedJsonToCsvFileReaderWriterFactory implements FileReaderWriterFactory {
    private static final byte   DELIMITER = '\n';
    private static final String LINE_END  = Byte.toString(DELIMITER);

    private static TagToColumns TAG_TO_COLUMNS;

    @Override
    public FileReader BuildFileReader(LogFilePath logFilePath, CompressionCodec codec)
            throws IllegalAccessException, IOException, InstantiationException {
        return new DelimitedTextFileReader(logFilePath, codec);
    }

    @Override
    public FileWriter BuildFileWriter(LogFilePath logFilePath, CompressionCodec codec) throws IOException {
        if (TAG_TO_COLUMNS == null ) {
            TAG_TO_COLUMNS = TagToColumns.getInstance();
        }
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
        private static final char   SEPARATOR = '\t';
        private static final char   QUOTE     = '\u0000';
        private static final char   ESCAPE    = '\\';

        private final CountingOutputStream mCountingStream;
        private final BufferedOutputStream mWriter;
        private Compressor mCompressor = null;
        private final CSVWriter mCsvWriter;

        public DelimitedTextFileWriter(LogFilePath path, CompressionCodec codec) throws IOException {
            Path fsPath = new Path(path.getLogFilePath());
            FileSystem fs = FileUtil.getFileSystem(path.getLogFilePath());
            this.mCountingStream = new CountingOutputStream(fs.create(fsPath));
            this.mWriter = (codec == null) ? new BufferedOutputStream(
                    this.mCountingStream) : new BufferedOutputStream(
                    codec.createOutputStream(this.mCountingStream,
                                             mCompressor = CodecPool.getCompressor(codec)));
            this.mCsvWriter = CSV.createCSVWriter(mWriter, Charsets.UTF_8.name(), SEPARATOR, QUOTE, ESCAPE, LINE_END);
        }

        @Override
        public long getLength() throws IOException {
            assert this.mCountingStream != null;
            this.mWriter.flush();
            return this.mCountingStream.getCount();
        }

        @Override
        public void write(KeyValue keyValue) throws IOException {
            String message = new String(keyValue.getValue(), Charsets.UTF_8);
            JsonToCsvUtil.convertToTsv(message, mCsvWriter, TAG_TO_COLUMNS.tagToColumnMap);
            this.mCsvWriter.flush();
        }

        @Override
        public void close() throws IOException {
            this.mWriter.close();
            CodecPool.returnCompressor(mCompressor);
            mCompressor = null;
            this.mCsvWriter.flush();
            this.mCsvWriter.close();
        }
    }
}
