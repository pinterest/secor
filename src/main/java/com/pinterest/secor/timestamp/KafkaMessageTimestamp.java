package com.pinterest.secor.timestamp;

import kafka.message.MessageAndMetadata;
import kafka.message.MessageAndOffset;

public interface KafkaMessageTimestamp {

    long getTimestamp(MessageAndMetadata<byte[], byte[]> kafkaMessage);

    long getTimestamp(MessageAndOffset messageAndOffset);
}
