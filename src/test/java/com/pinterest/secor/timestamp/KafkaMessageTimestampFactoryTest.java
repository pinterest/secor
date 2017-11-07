package com.pinterest.secor.timestamp;

import org.junit.Test;

import static org.junit.Assert.*;

public class KafkaMessageTimestampFactoryTest {

    private KafkaMessageTimestampFactory factory;

    @Test
    public void shouldReturnKafka8TimestampClassObject() {
        factory = new KafkaMessageTimestampFactory("com.pinterest.secor.timestamp.Kafka8MessageTimestamp");
        Object timestamp = factory.getKafkaMessageTimestamp();
        assertNotNull(timestamp);
        assertEquals(timestamp.getClass(), Kafka8MessageTimestamp.class);
    }

    @Test(expected = RuntimeException.class)
    public void shouldReturnNullForInvalidClass() {
        factory = new KafkaMessageTimestampFactory("com.pinterest.secor.timestamp.KafkaxxMessageTimestamp");
    }
}
