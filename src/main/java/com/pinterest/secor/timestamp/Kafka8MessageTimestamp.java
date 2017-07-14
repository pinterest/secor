package com.pinterest.secor.timestamp;

import kafka.message.MessageAndMetadata;
import kafka.message.MessageAndOffset;

public class Kafka8MessageTimestamp implements KafkaMessageTimestamp {

    @Override
    public Long getTimestamp(MessageAndMetadata<byte[], byte[]> kafkaMessage) {
        return 0l;
    }

    @Override
    public Long getTimestamp(MessageAndOffset messageAndOffset) {
        return 0l;
    }
}
