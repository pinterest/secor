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
package com.pinterest.secor.message;

import java.io.IOException;
import java.io.OutputStream;
import java.lang.String;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.util.Collections;
import java.util.List;

/**
 * Message represents a raw Kafka log message.
 *
 * @author Pawel Garbacki (pawel@pinterest.com)
 */
public class Message {

    private static final byte[] EMPTY_BYTES = new byte[0];

    private String mTopic;
    private int mKafkaPartition;
    private long mOffset;
    private byte[] mKafkaKey;
    private byte[] mPayload;
    private long mTimestamp;
    private List<MessageHeader> mHeaders;

    private static final int TRUNCATED_STRING_MAX_LEN = 1024;
    /**
     * Message key and payload may be arbitrary binary strings, so we should make sure we don't throw
     * when logging them by using a CharsetDecoder which replaces bad data. (While in practice `new String(bytes)`
     * does the same thing, the documentation for that method leaves that behavior unspecified.)
     * Additionally, in contexts where Message.toString() will be logged at a high level (including exception
     * messages), we truncate long keys and payloads, which may be very long binary data.
     */
    private String bytesToString(byte[] bytes, boolean truncate) {
        CharsetDecoder decoder = Charset.defaultCharset()
                .newDecoder()
                .onMalformedInput(CodingErrorAction.REPLACE)
                .onUnmappableCharacter(CodingErrorAction.REPLACE);
        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
        CharBuffer charBuffer;
        try {
            charBuffer = decoder.decode(byteBuffer);
        } catch (CharacterCodingException e) {
            // Shouldn't happen due to choosing REPLACE above, but Java makes us catch it anyway.
            throw new RuntimeException(e);
        }
        String s = charBuffer.toString();
        if (truncate && s.length() > TRUNCATED_STRING_MAX_LEN) {
            return new StringBuilder().append(s, 0, TRUNCATED_STRING_MAX_LEN).append("[...]").toString();
        } else {
            return s;
        }
    }

    protected String fieldsToString(boolean truncate) {
        return "topic='" + mTopic + '\'' +
               ", kafkaPartition=" + mKafkaPartition +
               ", offset=" + mOffset +
               ", kafkaKey=" + bytesToString(mKafkaKey, truncate) +
               ", payload=" + bytesToString(mPayload, truncate) +
               ", timestamp=" + mTimestamp +
               ", headers=" + mHeaders;
    }

    @Override
    public String toString() {
        return "Message{" + fieldsToString(false) + '}';
    }

    public Message(String topic, int kafkaPartition, long offset, byte[] kafkaKey, byte[] payload, long timestamp, List<MessageHeader> headers) {
        mTopic = topic;
        mKafkaPartition = kafkaPartition;
        mOffset = offset;
        mKafkaKey = kafkaKey;
        if (mKafkaKey == null) {
            mKafkaKey = EMPTY_BYTES;
        }
        mPayload = payload;
        if (mPayload == null) {
            mPayload = EMPTY_BYTES;
        }
        mTimestamp = timestamp;
        mHeaders = headers;
        if(mHeaders == null){
            mHeaders = Collections.emptyList();
        }
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

    public byte[] getKafkaKey() {
        return mKafkaKey;
    }

    public byte[] getPayload() {
        return mPayload;
    }

    public long getTimestamp() {
        return mTimestamp;
    }

    public List<MessageHeader> getHeaders(){
        return mHeaders;
    }

    public void write(OutputStream output) throws IOException {
        output.write(mPayload);
    }
}
