package com.pinterest.secor.io.impl;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import com.google.protobuf.Message;
import com.pinterest.secor.common.SecorSchemaRegistryClient;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
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
import com.pinterest.secor.util.AvroSchemaUtil;
import com.pinterest.secor.util.ParquetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AvroParquetFileReaderWriterFactory implements FileReaderWriterFactory {

    private static final Logger LOG = LoggerFactory.getLogger(AvroParquetFileReaderWriterFactory.class);
    protected final int blockSize;
    protected final int pageSize;
    protected final boolean enableDictionary;
    protected final boolean validating;
    protected final String schemaSubjectSuffix;
    protected final String schemaSubjectOverride;
    protected SecorSchemaRegistryClient schemaRegistryClient;
    protected DecoderFactory decoderFactory;

    public AvroParquetFileReaderWriterFactory(SecorConfig config) {
        blockSize = ParquetUtil.getParquetBlockSize(config);
        pageSize = ParquetUtil.getParquetPageSize(config);
        enableDictionary = ParquetUtil.getParquetEnableDictionary(config);
        validating = ParquetUtil.getParquetValidation(config);
        schemaSubjectSuffix = AvroSchemaUtil.getAvroSubjectSuffix(config);
        schemaSubjectOverride = AvroSchemaUtil.getAvroSubjectOverride(config);
        schemaRegistryClient = new SecorSchemaRegistryClient(config);
        decoderFactory = new DecoderFactory();
    }

    @Override
    public FileReader BuildFileReader(LogFilePath logFilePath, CompressionCodec codec) throws Exception {
        return new AvroParquetFileReader(logFilePath, codec);
    }

    @Override
    public FileWriter BuildFileWriter(LogFilePath logFilePath, CompressionCodec codec) throws Exception {
        return new AvroParquetFileWriter(logFilePath, codec);
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

    protected static GenericRecord deserializeAvroRecord(DecoderFactory decoderFactory, SpecificDatumReader<GenericRecord> reader,
                                                         byte[] value) throws IOException {
        BinaryDecoder binaryDecoder = decoderFactory.binaryDecoder(value, null);
        return reader.read(null, binaryDecoder);
    }

    protected GenericRecord decodeMessage(byte[] value, String topic, SpecificDatumReader<GenericRecord> reader) throws IOException {
        // Avro schema registry header format is a "Magic Byte" that equals 0 followed by a 4-byte int
        // https://docs.confluent.io/current/schema-registry/docs/serializer-formatter.html#wire-format
        if (value.length > 5 && value[0] == 0) {
            return schemaRegistryClient.decodeMessage(topic, value);
        } else {
            return deserializeAvroRecord(decoderFactory, reader, value);
        }
    }

    protected Schema getSchema(String topic) {
        if (!schemaSubjectOverride.isEmpty()) {
            topic = schemaSubjectOverride;
        } else if (!schemaSubjectSuffix.isEmpty()) {
            topic += schemaSubjectSuffix;
        }
        return schemaRegistryClient.getSchema(topic);
    }

    protected class AvroParquetFileReader implements FileReader {

        private ParquetReader<GenericRecord> reader;
        private SpecificDatumWriter<GenericRecord> writer;
        private long offset;

        public AvroParquetFileReader(LogFilePath logFilePath, CompressionCodec codec) throws IOException {
            Path path = new Path(logFilePath.getLogFilePath());
            String topic = logFilePath.getTopic();
            reader = AvroParquetReader.<GenericRecord>builder(path).build();
            writer = new SpecificDatumWriter<>(getSchema(topic));
            offset = logFilePath.getOffset();
        }

        @Override
        public KeyValue next() throws IOException {
            GenericRecord record = reader.read();
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

    protected class AvroParquetFileWriter implements FileWriter {

        private ParquetWriter writer;
        private String topic;
        private Schema schema;
        private SpecificDatumReader<GenericRecord> datumReader;

        public AvroParquetFileWriter(LogFilePath logFilePath, CompressionCodec codec) throws IOException {
            Path path = new Path(logFilePath.getLogFilePath());
            LOG.debug("Creating Brand new Writer for path {}", path);
            CompressionCodecName codecName = CompressionCodecName
                    .fromCompressionCodec(codec != null ? codec.getClass() : null);
            topic = logFilePath.getTopic();
            schema = getSchema(topic);
            datumReader = new SpecificDatumReader<>(schema);

            // Not setting blockSize, pageSize, enableDictionary, and validating
            writer = AvroParquetWriter.builder(path)
                    .withSchema(schema)
                    .withCompressionCodec(codecName)
                    .build();
        }

        @Override
        public long getLength() throws IOException {
            return writer.getDataSize();
        }

        @Override
        public void write(KeyValue keyValue) throws IOException {
            GenericRecord record = decodeMessage(keyValue.getValue(), topic, datumReader);
            LOG.trace("Writing record {}", record);
            if (record != null){
                writer.write(record);
            }
        }

        @Override
        public void close() throws IOException {
            writer.close();
        }
    }
}

