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
package com.pinterest.secor.parser;

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
    protected final SimpleDateFormat mDtFormatter;
    protected final SimpleDateFormat mHrFormatter;
    protected final SimpleDateFormat mDtHrFormatter;
    protected final int mFinalizerDelaySeconds;
    protected final SimpleDateFormat mDtHrMinFormatter;
    protected final SimpleDateFormat mMinFormatter;

    protected final String mDtFormat;
    protected final String mHrFormat;
    protected final String mMinFormat;

    protected final String mDtPrefix;
    protected final String mHrPrefix;
    protected final String mMinPrefix;

    protected final boolean mUsingHourly;
    protected final boolean mUsingMinutely;

    protected final boolean mUseKafkaTimestamp;


    public TimestampedMessageParser(SecorConfig config) {
        super(config);

        mUsingHourly = usingHourly(config);
        mUsingMinutely = usingMinutely(config);
        mUseKafkaTimestamp = useKafkaTimestamp(config);
        mDtFormat = usingDateFormat(config);
        mHrFormat = usingHourFormat(config);
        mMinFormat = usingMinuteFormat(config);

        mDtPrefix = usingDatePrefix(config);
        mHrPrefix = usingHourPrefix(config);
        mMinPrefix = usingMinutePrefix(config);

        LOG.info("UsingHourly: {}", mUsingHourly);
        LOG.info("UsingMin: {}", mUsingMinutely);
        mFinalizerDelaySeconds = config.getFinalizerDelaySeconds();
        LOG.info("FinalizerDelaySeconds: {}", mFinalizerDelaySeconds);

        mDtFormatter = new SimpleDateFormat(mDtFormat);
        mDtFormatter.setTimeZone(config.getTimeZone());

        mHrFormatter = new SimpleDateFormat(mHrFormat);
        mHrFormatter.setTimeZone(config.getTimeZone());

        mMinFormatter = new SimpleDateFormat(mMinFormat);
        mMinFormatter.setTimeZone(config.getTimeZone());

        mDtHrFormatter = new SimpleDateFormat(mDtFormat+ "-" + mHrFormat);
        mDtHrFormatter.setTimeZone(config.getTimeZone());

        mDtHrMinFormatter = new SimpleDateFormat(mDtFormat+ "-" + mHrFormat + "-" + mMinFormat);
        mDtHrMinFormatter.setTimeZone(config.getTimeZone());
    }

    static boolean usingHourly(SecorConfig config) {
        return config.getBoolean("partitioner.granularity.hour", false);
    }

    static boolean usingMinutely(SecorConfig config) {
        return config.getBoolean("partitioner.granularity.minute", false);
    }

    static String usingDateFormat(SecorConfig config) {
        return config.getString("partitioner.granularity.date.format", "yyyy-MM-dd");
    }

    static String usingHourFormat(SecorConfig config) {
        return config.getString("partitioner.granularity.hour.format", "HH");
    }

    static String usingMinuteFormat(SecorConfig config) {
        return config.getString("partitioner.granularity.min.format", "mm");
    }

    static String usingDatePrefix(SecorConfig config) {
        return config.getString("partitioner.granularity.date.prefix", "dt=");
    }

    static String usingHourPrefix(SecorConfig config) {
        return config.getString("partitioner.granularity.hour.prefix", "hr=");
    }

    static String usingMinutePrefix(SecorConfig config) {
        return config.getString("partitioner.granularity.minute.prefix", "min=");
    }

    static boolean useKafkaTimestamp(SecorConfig config) {
        return config.useKafkaTimestamp();
    }

    protected static long toMillis(final long timestamp) {
        final long nanosecondDivider = (long) Math.pow(10, 9 + 9);
        final long microsecondDivider = (long) Math.pow(10, 9 + 6);
        final long millisecondDivider = (long) Math.pow(10, 9 + 3);
        long timestampMillis;
        if (timestamp / nanosecondDivider > 0L) {
            timestampMillis = timestamp / (long) Math.pow(10, 6);
        } else if (timestamp / microsecondDivider > 0L) {
            timestampMillis = timestamp / (long) Math.pow(10, 3);
        } else if (timestamp / millisecondDivider > 0L) {
            timestampMillis = timestamp;
        } else {  // assume seconds
            timestampMillis = timestamp * 1000L;
        }
        return timestampMillis;
    }

    public abstract long extractTimestampMillis(final Message message) throws Exception;

    public long getTimestampMillis(Message message) throws Exception {
        return (mUseKafkaTimestamp) ? toMillis(message.getTimestamp()) : extractTimestampMillis(message);
    }

    protected String[] generatePartitions(long timestampMillis, boolean usingHourly, boolean usingMinutely)
            throws Exception {
        Date date = new Date(timestampMillis);
        String dt = mDtPrefix + mDtFormatter.format(date);
        String hr = mHrPrefix + mHrFormatter.format(date);
        String min = mMinPrefix + mMinFormatter.format(date);
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
        long timestampMillis = getTimestampMillis(message);
        return generatePartitions(timestampMillis, mUsingHourly, mUsingMinutely);
    }

    private long getFinalizedTimestampMillis(Message lastMessage,
                                             Message committedMessage) throws Exception {
        long lastTimestamp = getTimestampMillis(lastMessage);
        long committedTimestamp = getTimestampMillis(committedMessage);
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
