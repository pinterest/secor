/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
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
