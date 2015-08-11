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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public abstract class TimestampedMessageParser extends MessageParser implements Partitioner {

    private static final Logger LOG = LoggerFactory.getLogger(TimestampedMessageParser.class);

    private static final long HOUR_IN_MILLIS = 3600L * 1000L;
    private static final long DAY_IN_MILLIS = 3600L * 24 * 1000L;

    private static final SimpleDateFormat mDtFormatter = new SimpleDateFormat("yyyy-MM-dd");
    private static final SimpleDateFormat mHrFormatter = new SimpleDateFormat("HH");
    private static final SimpleDateFormat mDtHrFormatter = new SimpleDateFormat("yyyy-MM-dd-HH");

    static {
        mDtFormatter.setTimeZone(TimeZone.getTimeZone("UTC"));
        mHrFormatter.setTimeZone(TimeZone.getTimeZone("UTC"));
        mDtHrFormatter.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    private final boolean mUsingHourly;

    public TimestampedMessageParser(SecorConfig config) {
        super(config);
        mUsingHourly = usingHourly(config);
        LOG.info("UsingHourly: {}", mUsingHourly);
    }

    public abstract long extractTimestampMillis(final Message message) throws Exception;

    static boolean usingHourly(SecorConfig config) {
        return config.getBoolean("partitioner.granularity.hour", false);
    }

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

    protected String[] generatePartitions(long timestampMillis, boolean usingHourly)
        throws Exception {
        Date date = new Date(timestampMillis);
        String dt = "dt=" + mDtFormatter.format(date);
        String hr = "hr=" + mHrFormatter.format(date);
        if (usingHourly) {
            return new String[]{dt, hr};
        } else {
            return new String[]{dt};
        }
    }

    protected long parsePartitions(String[] partitions) throws Exception {
        String dtValue = partitions[0].split("=")[1];
        String hrValue = partitions.length > 1 ? partitions[1].split("=")[1] : "00";
        String value = dtValue + "-" + hrValue;
        Date date = mDtHrFormatter.parse(value);
        return date.getTime();
    }

    @Override
    public String[] extractPartitions(Message message) throws Exception {
        // Date constructor takes milliseconds since epoch.
        long timestampMillis = extractTimestampMillis(message);
        return generatePartitions(timestampMillis, mUsingHourly);
    }

    private long getFinalizedTimestampMillis(Message lastMessage,
                                             Message committedMessage) throws Exception {
        long lastTimestamp = extractTimestampMillis(lastMessage);
        long committedTimestamp = extractTimestampMillis(committedMessage);
        long now = System.currentTimeMillis();
        if (lastTimestamp == committedTimestamp && (now - lastTimestamp) > 3600 * 1000) {
            LOG.info("No new message coming, use the current time: " + now);
            return now;
        }
        return committedTimestamp;
    }

    @Override
    public String[] getFinalizedUptoPartitions(List<Message> lastMessages,
                                               List<Message> committedMessages) throws Exception {
        if (lastMessages == null || committedMessages == null) {
            LOG.error("Either: {} and {} is null", lastMessages,
                committedMessages);
            return null;
        }
        assert lastMessages.size() == committedMessages.size();

        long minMillis = Long.MAX_VALUE;
        for (int i = 0; i < lastMessages.size(); i++) {
            long millis = getFinalizedTimestampMillis(lastMessages.get(i),
                committedMessages.get(i));
            if (millis < minMillis) {
                minMillis = millis;
            }
        }
        if (minMillis == Long.MAX_VALUE) {
            LOG.error("No valid timestamps among messages: {} and {}", lastMessages,
                committedMessages);
            return null;
        }

        // add the safety lag for late-arrival messages
        minMillis -= 3600L * 1000L;
        LOG.info("adjusted millis {}", minMillis);
        return generatePartitions(minMillis, mUsingHourly);
    }

    @Override
    public String[] getPreviousPartitions(String[] partitions) throws Exception {
        long millis = parsePartitions(partitions);
        boolean usingHourly = mUsingHourly;
        if (mUsingHourly && millis % DAY_IN_MILLIS == 0) {
            // On the day boundary, if the currrent partition is [dt=07-07, hr=00], the previous
            // one is dt=07-06;  If the current one is [dt=07-06], the previous one is
            // [dt=07-06, hr-23]
            // So we would return in the order of:
            // dt=07-07, hr=01
            // dt=07-07, hr=00
            // dt=07-06
            // dt=07-06, hr=23
            if (partitions.length == 2 ) {
                usingHourly = false;
                millis -= DAY_IN_MILLIS;
            } else {
                usingHourly = true;
                millis += DAY_IN_MILLIS;
                millis -= HOUR_IN_MILLIS;
            }
        } else {
            long delta = mUsingHourly ? HOUR_IN_MILLIS : DAY_IN_MILLIS;
            millis -= delta;
        }
        return generatePartitions(millis, usingHourly);
    }

}
