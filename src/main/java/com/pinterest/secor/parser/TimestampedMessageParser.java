/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.pinterest.secor.parser;

import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.message.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public abstract class TimestampedMessageParser extends MessageParser implements Partitioner {

    private static final Logger LOG = LoggerFactory.getLogger(TimestampedMessageParser.class);

    private static final long HOUR_IN_MILLIS = 3600L * 1000L;
    private static final long DAY_IN_MILLIS = 3600L * 24 * 1000L;
    private static final long MINUTE_IN_MILLIS = 60L * 1000L;

    /*
     * IMPORTANT
     * SimpleDateFormat are NOT thread-safe.
     * Each parser needs to have their own local SimpleDateFormat or it'll cause race condition.
     */
    private final SimpleDateFormat mDtFormatter;
    private final SimpleDateFormat mHrFormatter;
    private final SimpleDateFormat mDtHrFormatter;
    private final int mFinalizerDelaySeconds;
    private final SimpleDateFormat mDtHrMinFormatter;
    private final SimpleDateFormat mMinFormatter;

    private final boolean mUsingHourly;
    private final boolean mUsingMinutely;


    public TimestampedMessageParser(SecorConfig config) {
        super(config);
        mUsingHourly = usingHourly(config);
        mUsingMinutely = usingMinutely(config);
        LOG.info("UsingHourly: {}", mUsingHourly);
        LOG.info("UsingMin: {}", mUsingMinutely);
        mFinalizerDelaySeconds = config.getFinalizerDelaySeconds();
        LOG.info("FinalizerDelaySeconds: {}", mFinalizerDelaySeconds);

        mDtFormatter = new SimpleDateFormat("yyyy-MM-dd");
        mDtFormatter.setTimeZone(config.getTimeZone());

        mHrFormatter = new SimpleDateFormat("HH");
        mHrFormatter.setTimeZone(config.getTimeZone());

        mDtHrFormatter = new SimpleDateFormat("yyyy-MM-dd-HH");
        mDtHrFormatter.setTimeZone(config.getTimeZone());

        mDtHrMinFormatter = new SimpleDateFormat("yyyy-MM-dd-HH-mm");
        mDtHrMinFormatter.setTimeZone(config.getTimeZone());
        mMinFormatter = new SimpleDateFormat("mm");
        mMinFormatter.setTimeZone(config.getTimeZone());
    }

    static boolean usingHourly(SecorConfig config) {
        return config.getBoolean("partitioner.granularity.hour", false);
    }

    static boolean usingMinutely(SecorConfig config) {
        return config.getBoolean("partitioner.granularity.minute", false);
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

    protected static long toMillis(final Timestamp timestamp) {
       return Timestamps.toMillis(timestamp);
    }

    public abstract long extractTimestampMillis(final Message message) throws Exception;

    protected String[] generatePartitions(long timestampMillis, boolean usingHourly, boolean usingMinutely)
            throws Exception {
        Date date = new Date(timestampMillis);
        String dt = "dt=" + mDtFormatter.format(date);
        String hr = "hr=" + mHrFormatter.format(date);
        String min = "min=" + mMinFormatter.format(date);
        if (usingMinutely) {
            return new String[]{dt, hr, min};
        } else if (usingHourly) {
            return new String[]{dt, hr};
        } else {
            return new String[]{dt};
        }
    }

    protected long parsePartitions(String[] partitions) throws Exception {
        String dtValue = partitions[0].split("=")[1];
        String hrValue = partitions.length > 1 ? partitions[1].split("=")[1] : "00";
        String minValue = partitions.length > 2 ? partitions[2].split("=")[1] : "00";
        String value = dtValue + "-" + hrValue + "-" + minValue;
        Date date = mDtHrMinFormatter.parse(value);
        return date.getTime();
    }

    @Override
    public String[] extractPartitions(Message message) throws Exception {
        // Date constructor takes milliseconds since epoch.
        long timestampMillis = extractTimestampMillis(message);
        return generatePartitions(timestampMillis, mUsingHourly, mUsingMinutely);
    }

    private long getFinalizedTimestampMillis(Message lastMessage,
                                             Message committedMessage) throws Exception {
        long lastTimestamp = extractTimestampMillis(lastMessage);
        long committedTimestamp = extractTimestampMillis(committedMessage);
        long now = System.currentTimeMillis();
        if (lastTimestamp == committedTimestamp &&
                (now - lastTimestamp) > mFinalizerDelaySeconds * 1000) {
            LOG.info("No new message coming, use the current time: " + now);
            return now;
        }
        return committedTimestamp;
    }

    @Override
    public String[] getFinalizedUptoPartitions(List<Message> lastMessages,
                                               List<Message> committedMessages) throws Exception {

        if (lastMessages == null || committedMessages == null) {
            LOG.error("Either: {} and {} is null", lastMessages, committedMessages);
            return null;
        }
        assert lastMessages.size() == committedMessages.size();

        long minMillis = Long.MAX_VALUE;
        for (int i = 0; i < lastMessages.size(); i++) {
            long millis = getFinalizedTimestampMillis(lastMessages.get(i), committedMessages.get(i));
            if (millis < minMillis) {
                LOG.info("partition {}, time {}", i, millis);
                minMillis = millis;
            }
        }
        if (minMillis == Long.MAX_VALUE) {
            LOG.error("No valid timestamps among messages: {} and {}", lastMessages, committedMessages);
            return null;
        }

        // add the safety lag for late-arrival messages
        minMillis -= mFinalizerDelaySeconds * 1000L;
        LOG.info("adjusted millis {}", minMillis);
        return generatePartitions(minMillis, mUsingHourly, mUsingMinutely);

    }
    @Override
    public String[] getPreviousPartitions(String[] partitions) throws Exception {
        long millis = parsePartitions(partitions);
        boolean usingHourly = mUsingHourly;
        boolean usingMinutely = mUsingMinutely;

        if (mUsingMinutely && millis % HOUR_IN_MILLIS == 0) {
            if (partitions.length == 3) {
                usingMinutely = false;
                if (millis % DAY_IN_MILLIS == 0) {
                    millis -= DAY_IN_MILLIS;
                } else {
                    millis -= HOUR_IN_MILLIS;
                    usingHourly = true;
                }
            } else if (partitions.length == 2) {
                millis += HOUR_IN_MILLIS;
                millis -= MINUTE_IN_MILLIS;
                usingMinutely = true;
            } else {
                millis += DAY_IN_MILLIS;
                millis -= HOUR_IN_MILLIS;
                usingMinutely = false;
                usingHourly = true;
            }
        } else if (mUsingHourly && millis % DAY_IN_MILLIS == 0) {
            // On the day boundary, if the current partition is [dt=07-07, hr=00], the previous
            // one is dt=07-06;  If the current one is [dt=07-06], the previous one is
            // [dt=07-06, hr-23]
            // So we would return in the order of:
            // dt=07-07, hr=01
            // dt=07-07, hr=00
            // dt=07-06
            // dt=07-06, hr=23
            if (partitions.length == 2) {
                usingHourly = false;
                millis -= DAY_IN_MILLIS;
            } else {
                usingHourly = true;
                millis += DAY_IN_MILLIS;
                millis -= HOUR_IN_MILLIS;
            }
        } else {
            long delta = mUsingHourly ? HOUR_IN_MILLIS : DAY_IN_MILLIS;
            if (mUsingMinutely) {
                delta = MINUTE_IN_MILLIS;
            }
            millis -= delta;
        }
        return generatePartitions(millis, usingHourly, usingMinutely);
    }
    }
