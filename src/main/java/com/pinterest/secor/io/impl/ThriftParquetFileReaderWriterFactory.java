/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.pinterest.secor.io.impl;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.thrift.ThriftParquetReader;
import org.apache.parquet.thrift.ThriftParquetWriter;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;

import com.pinterest.secor.common.files.LogFilePath;
import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.io.FileReader;
import com.pinterest.secor.io.FileReaderWriterFactory;
import com.pinterest.secor.io.FileWriter;
import com.pinterest.secor.io.KeyValue;
import com.pinterest.secor.util.ParquetUtil;
import com.pinterest.secor.util.ThriftUtil;

/**
 * Adapted from
 * com.pinterest.secor.io.impl.ProtobufParquetFileReaderWriterFactory
 * Implementation for reading/writing thrift messages to/from Parquet files.
 * 
 
 */
public class ThriftParquetFileReaderWriterFactory implements FileReaderWriterFactory {

    private ThriftUtil thriftUtil;

    protected final int blockSize;
    protected final int pageSize;
    protected final boolean enableDictionary;
    protected final boolean validating;

    public ThriftParquetFileReaderWriterFactory(SecorConfig config) {
        thriftUtil = new ThriftUtil(config);

        blockSize = ParquetUtil.getParquetBlockSize(config);
        pageSize = ParquetUtil.getParquetPageSize(config);
        enableDictionary = ParquetUtil.getParquetEnableDictionary(config);
        validating = ParquetUtil.getParquetValidation(config);
    }

    @Override
    public FileReader BuildFileReader(LogFilePath logFilePath, CompressionCodec codec) throws Exception {
        return new ThriftParquetFileReader(logFilePath, codec);
    }

    @Override
    public FileWriter BuildFileWriter(LogFilePath logFilePath, CompressionCodec codec) throws Exception {
        return new ThriftParquetFileWriter(logFilePath, codec);
    }

    protected class ThriftParquetFileReader implements FileReader {

        private ParquetReader<TBase<?, ?>> reader;
        private long offset;

        public ThriftParquetFileReader(LogFilePath logFilePath, CompressionCodec codec) throws IOException {
            Path path = new Path(logFilePath.getLogFilePath());
            Class<? extends TBase> messageClass = thriftUtil.getMessageClass(logFilePath.getTopic());
            reader = ThriftParquetReader.build(path).withThriftClass((Class<TBase<?, ?>>) messageClass).build();
            offset = logFilePath.getOffset();
        }

        @SuppressWarnings("rawtypes")
        @Override
        public KeyValue next() throws IOException {
            TBase msg = reader.read();

            if (msg != null) {
                try {
                    return new KeyValue(offset++, thriftUtil.encodeMessage(msg));
                } catch (TException e) {
                    throw new IOException("cannot write message", e);
                } catch (InstantiationException e) {
                    throw new IOException("cannot write message", e);
                } catch (IllegalAccessException e) {
                    throw new IOException("cannot write message", e);
                }
            }
            return null;
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }
    }

    protected class ThriftParquetFileWriter implements FileWriter {

        @SuppressWarnings("rawtypes")
        private ThriftParquetWriter writer;
        private String topic;

        @SuppressWarnings({ "rawtypes", "unchecked" })
        public ThriftParquetFileWriter(LogFilePath logFilePath, CompressionCodec codec) throws IOException {
            Path path = new Path(logFilePath.getLogFilePath());
            CompressionCodecName codecName = CompressionCodecName.fromCompressionCodec(codec != null ? codec.getClass() : null);
            topic = logFilePath.getTopic();
            writer = new ThriftParquetWriter(path, thriftUtil.getMessageClass(topic), codecName,
                    blockSize, pageSize, enableDictionary, validating);
        }

        @Override
        public long getLength() throws IOException {
            return writer.getDataSize();
        }

        @SuppressWarnings("unchecked")
        @Override
        public void write(KeyValue keyValue) throws IOException {
            Object message;
            try {
                message = thriftUtil.decodeMessage(topic, keyValue.getValue());
                writer.write(message);
            } catch (Exception e) {
                throw new IOException("cannot write message", e);
            }
        }

        @Override
        public void close() throws IOException {
            writer.close();
        }
    }
}
