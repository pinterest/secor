package com.pinterest.secor.timestamp;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class KafkaMessageTimestampFactoryTest {

    private KafkaMessageTimestampFactory factory;

    @Before
    public void setup() {
        factory = new KafkaMessageTimestampFactory();
    }

    @Test
    public void shouldReturnKafka8TimestampClassObject() {
        Object timestamp = factory.create("com.pinterest.secor.timestamp.Kafka8MessageTimestamp");
        assertNotNull(timestamp);
        assertEquals(timestamp.getClass(), Kafka8MessageTimestamp.class);
    }

    @Test
    public void shouldReturnNullForInvalidClass() {
        Object timestamp = factory.create("com.pinterest.secor.timestamp.KafkaXXXMessageTimestamp");
        assertNull(timestamp);
    }
}
