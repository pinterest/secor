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

import com.pinterest.secor.common.AvroSchemaRegistry;
import com.pinterest.secor.common.LogFilePath;
import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.io.FileReader;
import com.pinterest.secor.io.FileReaderWriterFactory;
import com.pinterest.secor.io.FileWriter;
import com.pinterest.secor.io.KeyValue;
import com.pinterest.secor.util.AvroSchemaRegistryFactory;
import com.pinterest.secor.util.CompressionUtil;
import com.pinterest.secor.util.ParquetUtil;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class AvroFileReaderWriterFactory implements FileReaderWriterFactory {

    private static final Logger LOG = LoggerFactory.getLogger(AvroFileReaderWriterFactory.class);
    protected final int blockSize;
    protected final int pageSize;
    protected final boolean enableDictionary;
    protected final boolean validating;
    protected AvroSchemaRegistry schemaRegistry;
    private String avroCodec;

    public AvroFileReaderWriterFactory(SecorConfig config) {
        blockSize = ParquetUtil.getParquetBlockSize(config);
        pageSize = ParquetUtil.getParquetPageSize(config);
        enableDictionary = ParquetUtil.getParquetEnableDictionary(config);
        validating = ParquetUtil.getParquetValidation(config);
        schemaRegistry = AvroSchemaRegistryFactory.getSchemaRegistry(config);
        avroCodec = CompressionUtil.getAvroCompressionCodec(config);
    }

    @Override
    public FileReader BuildFileReader(LogFilePath logFilePath, CompressionCodec codec) throws Exception {
        return new AvroFileReader(logFilePath, avroCodec);
    }

    @Override
    public FileWriter BuildFileWriter(LogFilePath logFilePath, CompressionCodec codec) throws Exception {
        return new AvroFileWriter(logFilePath, avroCodec);
    }

    protected class AvroFileReader implements FileReader {

        private DataFileReader<GenericRecord> reader;
        private SpecificDatumWriter<GenericRecord> writer;
        private long offset;
        private File file;
        private String topic;


        public AvroFileReader(LogFilePath logFilePath,String avroCodec) throws IOException {
            file = new File(logFilePath.getLogFilePath());
            file.getParentFile().mkdirs();
            topic = logFilePath.getTopic();
            Schema schema = schemaRegistry.getSchema(topic);


            DatumReader datumReader = new SpecificDatumReader(schema);
            try {
                reader = new DataFileReader(file, datumReader);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            offset = logFilePath.getOffset();
        }

        @Override
        public KeyValue next() throws IOException {
            GenericRecord record = reader.next();
            if (record != null) {
                return new KeyValue(offset++, schemaRegistry.serialize(topic, record));
            }
            return null;
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }

    }

    protected class AvroFileWriter implements FileWriter {

        private DataFileWriter<GenericRecord> writer;
        private File file;

        private String topic;

        public AvroFileWriter(LogFilePath logFilePath, String avroCodec) throws IOException {
            file = new File(logFilePath.getLogFilePath());
            file.getParentFile().mkdirs();
            LOG.debug("Creating Brand new Writer for path {}", logFilePath.getLogFilePath());
            topic = logFilePath.getTopic();
            Schema schema = schemaRegistry.getSchema(topic);
            SpecificDatumWriter specificDatumWriter= new SpecificDatumWriter(schema);
            writer = new DataFileWriter(specificDatumWriter);
            writer.setCodec(getCodecFactory(avroCodec));
            writer.create(schema, file);
        }

        private CodecFactory getCodecFactory(String avroCodec) {
            try {
                return CodecFactory.fromString(avroCodec);
            } catch (AvroRuntimeException e) {
                LOG.error("Error creating codec factory", e);
            }
            return CodecFactory.fromString("null");
        }

        @Override
        public long getLength() throws IOException {
            return file.length();
        }

        @Override
        public void write(KeyValue keyValue) throws IOException {
            GenericRecord record = schemaRegistry.deserialize(topic, keyValue.getValue());
            LOG.trace("Writing record {}", record);
            if (record != null){
                writer.append(record);
            }
        }

        @Override
        public void close() throws IOException {
            writer.close();
        }
    }
}

