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
import com.google.protobuf.CodedInputStream;

import java.io.IOException;

/**
 * Basic protocol buffer parser.
 *
 * Assumes that the timestamp field is the first field, is required,
 * and is a uint64. A more advanced parser might support an arbitrary
 * field number (non-nested to keep things simple) and perhaps
 * different data types.
 *
 * @author Liam Stewart (liam.stewart@gmail.com)
 */
public class ProtobufMessageParser extends TimestampedMessageParser {
    public ProtobufMessageParser(SecorConfig config) {
        super(config);
    }

    @Override
    public long extractTimestampMillis(final Message message) throws IOException {
        CodedInputStream input = CodedInputStream.newInstance(message.getPayload());

        // Don't really care about the tag, but need to read it to get
        // to the payload.
        int tag = input.readTag();
        return toMillis(input.readUInt64());
    }
}
