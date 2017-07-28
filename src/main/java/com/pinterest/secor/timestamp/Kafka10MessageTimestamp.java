package com.pinterest.secor.timestamp;

import kafka.message.MessageAndMetadata;
import kafka.message.MessageAndOffset;

public class Kafka10MessageTimestamp implements KafkaMessageTimestamp {

    @Override
    public long getTimestamp(MessageAndMetadata<byte[], byte[]> kafkaMessage) {
        return kafkaMessage.timestamp();
    }

    @Override
    public long getTimestamp(MessageAndOffset messageAndOffset) {
        return messageAndOffset.message().timestamp();
    }
}
