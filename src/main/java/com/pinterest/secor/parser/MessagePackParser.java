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

import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.message.Message;
import org.msgpack.MessagePack;
import org.msgpack.MessageTypeException;
import org.msgpack.type.MapValue;
import org.msgpack.type.RawValue;
import org.msgpack.type.Value;
import org.msgpack.type.ValueFactory;

/**
 * MessagePack timestamped message parser.
 * Requires a second or ms timestamp.
 * Does not support message.timestamp.input.pattern.
 *
 * @author Zack Dever (zack@rd.io)
 */
public class MessagePackParser extends TimestampedMessageParser {
    private MessagePack mMessagePack = new MessagePack();
    private RawValue mTimestampField;

    public MessagePackParser(SecorConfig config) {
        super(config);
        String timestampName = mConfig.getMessageTimestampName();
        mTimestampField = ValueFactory.createRawValue(timestampName);
    }

    @Override
    public long extractTimestampMillis(Message message) throws Exception {
        MapValue msgMap = mMessagePack.read(message.getPayload()).asMapValue();
        Value timestampValue = msgMap.get(mTimestampField);

        if (timestampValue.isIntegerValue()) {
            return toMillis(timestampValue.asIntegerValue().getLong());
        } else if (timestampValue.isFloatValue()) {
            return toMillis(timestampValue.asFloatValue().longValue());
        } else {
            String timestampString = timestampValue.asRawValue().getString();
            return toMillis(Long.parseLong(timestampString));
        }
    }
}
