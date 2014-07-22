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

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * Thrift message parser extracts date partitions from thrift messages.
 *
 * @author Pawel Garbacki (pawel@pinterest.com)
 */
public class ThriftMessageParser extends MessageParser {
    private TDeserializer mDeserializer;

    public ThriftMessageParser(SecorConfig config) {
        super(config);
        mDeserializer = new TDeserializer();
    }

    public long extractTimestampMillis(Message message) throws TException {
        class ThriftTemplate implements TFieldIdEnum {
            public ThriftTemplate() {
            }

            @Override
            public short getThriftFieldId() {
                return 1;
            }

            @Override
            public String getFieldName() {
                return "timestamp";
            }
        }
        long timestamp = mDeserializer.partialDeserializeI64(message.getPayload(),
                                                             new ThriftTemplate());
        final long nanosecondDivider = (long) Math.pow(10, 9 + 9);
        final long millisecondDivider = (long) Math.pow(10, 9 + 3);
        long timestampMillis;
        if (timestamp / nanosecondDivider > 0L) {
            timestampMillis = timestamp / (long) Math.pow(10, 6);
        } else if (timestamp / millisecondDivider > 0L) {
            timestampMillis = timestamp;
        } else {  // assume seconds
            timestampMillis = timestamp * 1000L;
        }
        return timestampMillis;
    }

    @Override
    public String[] extractPartitions(Message message) throws Exception {
        // Date constructor takes milliseconds since epoch.
        long timestampMillis = extractTimestampMillis(message);
        Date date = new Date(timestampMillis);
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        format.setTimeZone(TimeZone.getTimeZone("UTC"));
        String[] result = {"dt=" + format.format(date)};
        return result;
    }
}
