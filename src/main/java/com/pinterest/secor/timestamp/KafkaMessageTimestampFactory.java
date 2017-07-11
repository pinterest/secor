package com.pinterest.secor.timestamp;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaMessageTimestampFactory {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaMessageTimestampFactory.class);

    public static KafkaMessageTimestamp create(String kafkaTimestampClassName) {
        KafkaMessageTimestamp kafkaMessageTimestamp = null;
        try {
            Class timestampClass = Class.forName(kafkaTimestampClassName);
            kafkaMessageTimestamp = KafkaMessageTimestamp.class.cast(timestampClass.newInstance());
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            LOG.error("Unable to create KafkaTimestampFactory", e);
        }
        return kafkaMessageTimestamp;
    }
}
