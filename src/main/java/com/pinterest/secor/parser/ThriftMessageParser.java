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
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TFieldIdEnum;

/**
 * Thrift message parser extracts date partitions from thrift messages.
 *
 * @author Pawel Garbacki (pawel@pinterest.com)
 */
public class ThriftMessageParser extends TimestampedMessageParser {
    private TDeserializer mDeserializer;

    public ThriftMessageParser(SecorConfig config) {
        super(config);
        mDeserializer = new TDeserializer();
    }

    @Override
    public long extractTimestampMillis(final Message message) throws TException {
        class ThriftTemplate implements TFieldIdEnum {
            private final String mFieldName;
            public ThriftTemplate(final String fieldName) {
                this.mFieldName = fieldName;
            }

            @Override
            public short getThriftFieldId() {
                return 1;
            }

            @Override
            public String getFieldName() {
                return mFieldName;
            }
        }
        long timestamp = mDeserializer.partialDeserializeI64(message.getPayload(),
                new ThriftTemplate(mConfig.getMessageTimestampName()));
        return toMillis(timestamp);
    }
}
