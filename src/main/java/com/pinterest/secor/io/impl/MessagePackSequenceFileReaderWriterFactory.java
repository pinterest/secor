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

import com.pinterest.secor.common.LogFilePath;
import com.pinterest.secor.io.FileReader;
import com.pinterest.secor.io.FileReaderWriterFactory;
import com.pinterest.secor.io.FileWriter;
import com.pinterest.secor.io.KeyValue;
import com.pinterest.secor.util.FileUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePacker;
import org.msgpack.core.MessageUnpacker;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;

/**
 * Sequence file reader writer implementation
 *
 * @author Praveen Murugesan (praveen@uber.com)
 */
public class MessagePackSequenceFileReaderWriterFactory implements FileReaderWriterFactory {
    private static final int KAFKA_MESSAGE_OFFSET = 1;
    private static final int KAFKA_HASH_KEY = 2;
    private static final int KAFKA_MESSAGE_TIMESTAMP = 3;
    private static final byte[] EMPTY_BYTES = new byte[0];

    @Override
    public FileReader BuildFileReader(LogFilePath logFilePath, CompressionCodec codec) throws Exception {
        return new MessagePackSequenceFileReader(logFilePath);
    }

    @Override
    public FileWriter BuildFileWriter(LogFilePath logFilePath, CompressionCodec codec) throws IOException {
        return new MessagePackSequenceFileWriter(logFilePath, codec);
    }

    protected class MessagePackSequenceFileReader implements FileReader {
        private final SequenceFile.Reader mReader;
        private final BytesWritable mKey;
        private final BytesWritable mValue;

        public MessagePackSequenceFileReader(LogFilePath path) throws Exception {
            Configuration config = new Configuration();
            Path fsPath = new Path(path.getLogFilePath());
            FileSystem fs = FileUtil.getFileSystem(path.getLogFilePath());
            this.mReader = new SequenceFile.Reader(fs, fsPath, config);
            this.mKey = (BytesWritable) mReader.getKeyClass().newInstance();
            this.mValue = (BytesWritable) mReader.getValueClass().newInstance();
        }

        @Override
        public KeyValue next() throws IOException {
            if (mReader.next(mKey, mValue)) {
                MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(mKey.getBytes());
                int mapSize = unpacker.unpackMapHeader();
                long offset = 0;
                long timestamp = -1;
                byte[] keyBytes = EMPTY_BYTES;
                for (int i = 0; i < mapSize; i++) {
                    int key = unpacker.unpackInt();
                    switch (key) {
                        case KAFKA_MESSAGE_OFFSET:
                            offset = unpacker.unpackLong();
                            break;
                        case KAFKA_MESSAGE_TIMESTAMP:
                            timestamp = unpacker.unpackLong();
                            break;
                        case KAFKA_HASH_KEY:
                            int keySize = unpacker.unpackBinaryHeader();
                            keyBytes = new byte[keySize];
                            unpacker.readPayload(keyBytes);
                            break;
                    }
                }
                unpacker.close();
                return new KeyValue(offset, keyBytes, Arrays.copyOfRange(mValue.getBytes(), 0, mValue.getLength()), timestamp);
            } else {
                return null;
            }
        }

        @Override
        public void close() throws IOException {
            this.mReader.close();
        }
    }

    protected class MessagePackSequenceFileWriter implements FileWriter {
        private final SequenceFile.Writer mWriter;
        private final BytesWritable mKey;
        private final BytesWritable mValue;

        public MessagePackSequenceFileWriter(LogFilePath path, CompressionCodec codec) throws IOException {
            Configuration config = new Configuration();
            Path fsPath = new Path(path.getLogFilePath());
            FileSystem fs = FileUtil.getFileSystem(path.getLogFilePath());
            if (codec != null) {
                this.mWriter = SequenceFile.createWriter(fs, config, fsPath,
                        BytesWritable.class, BytesWritable.class,
                        SequenceFile.CompressionType.BLOCK, codec);
            } else {
                this.mWriter = SequenceFile.createWriter(fs, config, fsPath,
                        BytesWritable.class, BytesWritable.class);
            }
            this.mKey = new BytesWritable();
            this.mValue = new BytesWritable();
        }

        @Override
        public long getLength() throws IOException {
            return this.mWriter.getLength();
        }

        @Override
        public void write(KeyValue keyValue) throws IOException {
            byte[] kafkaKey = keyValue.hasKafkaKey() ? keyValue.getKafkaKey() : new byte[0];
            long timestamp = keyValue.getTimestamp();
            final int timestampLength = (keyValue.hasTimestamp()) ? 10 : 0;
            // output size estimate
            // 1 - map header
            // 1 - message pack key
            // 9 - max kafka offset
            // 1 - message pack key
            // 9 - kafka timestamp
            // 1 - message pack key
            // 5 - max (sane) kafka key size
            // N - size of kafka key
            // = 27 + N
            ByteArrayOutputStream out = new ByteArrayOutputStream(17 + timestampLength + kafkaKey.length);
            MessagePacker packer = MessagePack.newDefaultPacker(out)
                    .packMapHeader(numberOfFieldsMappedInHeader(keyValue))
                    .packInt(KAFKA_MESSAGE_OFFSET)
                    .packLong(keyValue.getOffset());

            if (keyValue.hasTimestamp())
                packer.packInt(KAFKA_MESSAGE_TIMESTAMP)
                        .packLong(timestamp);

            if (keyValue.hasKafkaKey())
                packer.packInt(KAFKA_HASH_KEY)
                        .packBinaryHeader(kafkaKey.length)
                        .writePayload(kafkaKey);

            packer.close();
            byte[] outBytes = out.toByteArray();
            this.mKey.set(outBytes, 0, outBytes.length);
            this.mValue.set(keyValue.getValue(), 0, keyValue.getValue().length);
            this.mWriter.append(this.mKey, this.mValue);
        }

        @Override
        public void close() throws IOException {
            this.mWriter.close();
        }

        private int numberOfFieldsMappedInHeader(KeyValue keyValue) {
            int fields = 1;

            if (keyValue.hasKafkaKey())
                fields++;

            if (keyValue.hasTimestamp())
                fields++;

            return fields;
        }
    }
}
