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
package com.pinterest.secor.message;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.String;

/**
 * Message represents a raw Kafka log message.
 *
 * @author Pawel Garbacki (pawel@pinterest.com)
 */
public class Message {
    private String mTopic;
    private int mKafkaPartition;
    private long mOffset;
    private byte[] mPayload;

    protected String fieldsToString() {
        return "topic='" + mTopic + '\'' +
               ", kafkaPartition=" + mKafkaPartition +
               ", offset=" + mOffset +
               ", payload=" + new String(mPayload);

    }

    @Override
    public String toString() {
        return "Message{" + fieldsToString() + '}';
    }

    public Message(String topic, int kafkaPartition, long offset, byte[] payload) {
        mTopic = topic;
        mKafkaPartition = kafkaPartition;
        mOffset = offset;
        mPayload = payload;
    }

    public String getTopic() {

        return mTopic;
    }

    public int getKafkaPartition() {
        return mKafkaPartition;
    }

    public long getOffset() {
        return mOffset;
    }

    public byte[] getPayload() {
        return mPayload;
    }

    public void write(OutputStream output) throws IOException {
        output.write(mPayload);
    }
}
