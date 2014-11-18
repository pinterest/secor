/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.pinterest.secor.tools;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.protocol.TSimpleJSONProtocol;

import com.pinterest.secor.thrift.TestMessage;
import com.pinterest.secor.thrift.TestEnum;

import java.util.Properties;

/**
 * Test log message producer generates test messages and submits them to kafka.
 *
 * @author Pawel Garbacki (pawel@pinterest.com)
 */
public class TestLogMessageProducer extends Thread {
    private final String mTopic;
    private final int mNumMessages;
    private final String mType;

    public TestLogMessageProducer(String topic, int numMessages, String type) {
        mTopic = topic;
        mNumMessages = numMessages;
        mType = type;
    }

    public void run() {
        Properties properties = new Properties();
        properties.put("metadata.broker.list", "localhost:9092");
        properties.put("partitioner.class", "com.pinterest.secor.tools.RandomPartitioner");
        properties.put("serializer.class", "kafka.serializer.DefaultEncoder");
        properties.put("key.serializer.class", "kafka.serializer.StringEncoder");
        properties.put("request.required.acks", "1");

        ProducerConfig config = new ProducerConfig(properties);
        Producer<String, byte[]> producer = new Producer<String, byte[]>(config);

        TProtocolFactory protocol = null;
        if(mType.equals("json")) {
            protocol = new TSimpleJSONProtocol.Factory();
        } else if (mType.equals("binary")) {
            protocol = new TBinaryProtocol.Factory();
        } else {
            throw new RuntimeException("Undefined message encoding type: " + mType);
        }

        TSerializer serializer = new TSerializer(protocol);
        for (int i = 0; i < mNumMessages; ++i) {
            TestMessage testMessage = new TestMessage(System.currentTimeMillis() * 1000000L + i,
                                                      "some_value_" + i);
            if (i % 2 == 0) {
                testMessage.setEnumField(TestEnum.SOME_VALUE);
            } else {
                testMessage.setEnumField(TestEnum.SOME_OTHER_VALUE);
            }
            byte[] bytes;
            try {
                bytes = serializer.serialize(testMessage);
            } catch(TException e) {
                throw new RuntimeException("Failed to serialize message " + testMessage, e);
            }
            KeyedMessage<String, byte[]> data = new KeyedMessage<String, byte[]>(
                mTopic, Integer.toString(i), bytes);
            producer.send(data);
        }
        producer.close();
    }
}