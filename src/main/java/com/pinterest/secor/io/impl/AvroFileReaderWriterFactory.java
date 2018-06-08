package com.pinterest.secor.io.impl;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import com.google.protobuf.Message;
import com.pinterest.secor.common.SecorSchemaRegistryClient;
import com.pinterest.secor.util.FileUtil;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import com.pinterest.secor.common.LogFilePath;
import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.io.FileReader;
import com.pinterest.secor.io.FileReaderWriterFactory;
import com.pinterest.secor.io.FileWriter;
import com.pinterest.secor.io.KeyValue;
import com.pinterest.secor.util.ParquetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AvroFileReaderWriterFactory implements FileReaderWriterFactory {

    private static final Logger LOG = LoggerFactory.getLogger(AvroFileReaderWriterFactory.class);
    protected final int blockSize;
    protected final int pageSize;
    protected final boolean enableDictionary;
    protected final boolean validating;
    protected SecorSchemaRegistryClient schemaRegistryClient;

    public AvroFileReaderWriterFactory(SecorConfig config) {
        blockSize = ParquetUtil.getParquetBlockSize(config);
        pageSize = ParquetUtil.getParquetPageSize(config);
        enableDictionary = ParquetUtil.getParquetEnableDictionary(config);
        validating = ParquetUtil.getParquetValidation(config);
        schemaRegistryClient = new SecorSchemaRegistryClient(config);
    }

    @Override
    public FileReader BuildFileReader(LogFilePath logFilePath, CompressionCodec codec) throws Exception {
        return new AvroFileReader(logFilePath, codec);
    }

    @Override
    public FileWriter BuildFileWriter(LogFilePath logFilePath, CompressionCodec codec) throws Exception {
        return new AvroFileWriter(logFilePath, codec);
    }

    protected static byte[] serializeAvroRecord(SpecificDatumWriter<GenericRecord> writer, GenericRecord record) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Encoder encoder = EncoderFactory.get().directBinaryEncoder(out, null);
        writer.write(record, encoder);
        encoder.flush();
        ByteBuffer serialized = ByteBuffer.allocate(out.toByteArray().length);
        serialized.put(out.toByteArray());
        return serialized.array();
    }

    protected class AvroFileReader implements FileReader {

        private DataFileReader<GenericRecord> reader;
        private SpecificDatumWriter<GenericRecord> writer;
        private long offset;
        private File file;

        public AvroFileReader(LogFilePath logFilePath, CompressionCodec codec) throws IOException {
            file = new File(logFilePath.getLogFilePath() + ".avro");
            file.getParentFile().mkdirs();
            String topic = logFilePath.getTopic();
            Schema schema = schemaRegistryClient.getSchema(topic);

            DatumReader datumReader = new SpecificDatumReader(schema);
            try {
                reader = new DataFileReader(file, datumReader);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            writer = new SpecificDatumWriter(schema);
            offset = logFilePath.getOffset();
        }

        @Override
        public KeyValue next() throws IOException {
            GenericRecord record = reader.next();
            if (record != null) {
                return new KeyValue(offset++, serializeAvroRecord(writer, record));
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

        public AvroFileWriter(LogFilePath logFilePath, CompressionCodec codec) throws IOException {
            file = new File(logFilePath.getLogFilePath());
            file.getParentFile().mkdirs();
            LOG.debug("Creating Brand new Writer for path {}", logFilePath.getLogFilePath());
            topic = logFilePath.getTopic();
            Schema schema = schemaRegistryClient.getSchema(topic);
            SpecificDatumWriter specificDatumWriter= new SpecificDatumWriter(schema);
            writer = new DataFileWriter(specificDatumWriter);
            writer.setCodec(getCodecFactory(codec));
            writer.create(schema, file);
        }

        private CodecFactory getCodecFactory(CompressionCodec codec) {
            CompressionCodecName codecName = CompressionCodecName
                    .fromCompressionCodec(codec != null ? codec.getClass() : null);
            try {
                return CodecFactory.fromString(codecName.name().toLowerCase());
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
            GenericRecord record = schemaRegistryClient.decodeMessage(topic, keyValue.getValue());
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

