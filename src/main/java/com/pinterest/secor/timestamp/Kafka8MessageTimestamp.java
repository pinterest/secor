package com.pinterest.secor.timestamp;

import kafka.message.MessageAndMetadata;
import kafka.message.MessageAndOffset;

public class Kafka8MessageTimestamp implements KafkaMessageTimestamp {

    @Override
    public long getTimestamp(MessageAndMetadata<byte[], byte[]> kafkaMessage) {
        return 0l;
    }

    @Override
    public long getTimestamp(MessageAndOffset messageAndOffset) {
        return 0l;
    }
}
