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

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public abstract class TimestampedMessageParser extends MessageParser {

    private SimpleDateFormat mFormatter;

    public TimestampedMessageParser(SecorConfig config) {
        super(config);
        mFormatter = new SimpleDateFormat("yyyy-MM-dd");
        mFormatter.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    public abstract long extractTimestampMillis(final Message message) throws Exception;

    protected static long toMillis(final long timestamp) {
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
        String result[] = {"dt=" + mFormatter.format(date)};
        return result;
    }
}
