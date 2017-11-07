package com.pinterest.secor.timestamp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaMessageTimestampFactory {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaMessageTimestampFactory.class);

    private KafkaMessageTimestamp kafkaMessageTimestamp;

    public KafkaMessageTimestampFactory(String kafkaTimestampClassName) {
        try {
            Class timestampClass = Class.forName(kafkaTimestampClassName);
            this.kafkaMessageTimestamp = KafkaMessageTimestamp.class.cast(timestampClass.newInstance());
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public KafkaMessageTimestamp getKafkaMessageTimestamp() {
        return this.kafkaMessageTimestamp;
    }
}
