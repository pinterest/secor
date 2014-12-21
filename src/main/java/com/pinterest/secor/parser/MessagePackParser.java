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
package com.pinterest.secor.parser;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.message.Message;
import org.msgpack.jackson.dataformat.MessagePackFactory;

import java.util.HashMap;

/**
 * MessagePack timestamped message parser.
 * Requires a second or ms timestamp.
 * Does not support message.timestamp.input.pattern.
 *
 * @author Zack Dever (zack@rd.io)
 */
public class MessagePackParser extends TimestampedMessageParser {
    private ObjectMapper mMessagePackObjectMapper;
    private TypeReference mTypeReference;

    public MessagePackParser(SecorConfig config) {
        super(config);
        mMessagePackObjectMapper = new ObjectMapper(new MessagePackFactory());
        mTypeReference = new TypeReference<HashMap<String, Object>>(){};
    }

    @Override
    public long extractTimestampMillis(Message message) throws Exception {
        HashMap<String, Object> msgHash = mMessagePackObjectMapper.readValue(message.getPayload(),
                mTypeReference);
        Object timestampValue = msgHash.get(mConfig.getMessageTimestampName());

        if (timestampValue instanceof Number) {
            return toMillis(((Number) timestampValue).longValue());
        } else if (timestampValue instanceof String) {
            return toMillis(Long.parseLong((String) timestampValue));
        } else {
            return toMillis((Long) timestampValue);
        }
    }
}
