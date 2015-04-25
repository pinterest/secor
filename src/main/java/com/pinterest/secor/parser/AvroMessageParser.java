package com.pinterest.secor.parser;

import com.google.common.primitives.Longs;
import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.message.Message;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;

import java.io.File;
import java.io.IOException;

/**
 * Avro message parser extracts date partitions from avro messages.
 * Uses: 'message.timestamp.name' to identify the field name.
 */
public class AvroMessageParser extends TimestampedMessageParser {
    final DatumReader<GenericRecord> mDatumReader;
    final String mFieldName;

    public AvroMessageParser(SecorConfig config) throws IOException {
        super(config);
        Schema schema = new Schema.Parser().parse(new File(mConfig.getMessageInputAvroSchema()));
        mFieldName = config.getMessageTimestampName();
        mDatumReader = new GenericDatumReader<GenericRecord>(schema);
    }

    @Override
    public long extractTimestampMillis(Message message) throws Exception {
        byte[] payload = message.getPayload();
        if (payload == null || payload.length == 0) {
            return 0L;
        }

        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(payload, null);
        GenericRecord record = mDatumReader.read(null, decoder);
        Object val = record.get(mFieldName);
        Long longVal = Longs.tryParse(String.valueOf(val));
        if (longVal == null) {
            longVal = 0L;
        }
        return toMillis(longVal);
    }
}
