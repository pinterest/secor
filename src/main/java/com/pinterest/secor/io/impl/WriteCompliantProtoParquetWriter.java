package com.pinterest.secor.io.impl;

import com.google.protobuf.Message;
import com.google.protobuf.MessageOrBuilder;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.proto.ProtoWriteSupport;

import java.io.IOException;

public class WriteCompliantProtoParquetWriter<T extends MessageOrBuilder> extends ParquetWriter<T> {
    public WriteCompliantProtoParquetWriter(Path file, Class<? extends Message> protoMessage, CompressionCodecName compressionCodecName, int blockSize, int pageSize) throws IOException {
        super(file, new WriteCompliantProtoWriteSupport(protoMessage), compressionCodecName, blockSize, pageSize);
    }

    public WriteCompliantProtoParquetWriter(Path file, Class<? extends Message> protoMessage, CompressionCodecName compressionCodecName, int blockSize, int pageSize, boolean enableDictionary, boolean validating) throws IOException {
        super(file, new WriteCompliantProtoWriteSupport(protoMessage), compressionCodecName, blockSize, pageSize, enableDictionary, validating);
    }

    public WriteCompliantProtoParquetWriter(Path file, Class<? extends Message> protoMessage) throws IOException {
        this(file, protoMessage, CompressionCodecName.UNCOMPRESSED, 134217728, 1048576);
    }
}
