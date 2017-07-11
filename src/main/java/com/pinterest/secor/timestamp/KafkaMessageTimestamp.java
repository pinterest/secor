package com.pinterest.secor.timestamp;

import kafka.message.MessageAndMetadata;
import kafka.message.MessageAndOffset;

public interface KafkaMessageTimestamp {

    Long getTimestamp(MessageAndMetadata<byte[], byte[]> kafkaMessage);

    Long getTimestamp(MessageAndOffset messageAndOffset);
}
