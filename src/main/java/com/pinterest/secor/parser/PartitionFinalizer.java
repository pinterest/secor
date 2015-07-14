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

import com.pinterest.secor.common.*;
import com.pinterest.secor.message.Message;
import com.pinterest.secor.util.CompressionUtil;
import com.pinterest.secor.util.FileUtil;
import com.pinterest.secor.util.ReflectionUtil;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Partition finalizer writes _SUCCESS files to date partitions that very likely won't be receiving
 * any new messages. It also adds those partitions to Hive.
 *
 * @author Pawel Garbacki (pawel@pinterest.com)
 */
public class PartitionFinalizer {
    private static final Logger LOG = LoggerFactory.getLogger(PartitionFinalizer.class);

    private SecorConfig mConfig;
    private ZookeeperConnector mZookeeperConnector;
    private TimestampedMessageParser mMessageParser;
    private KafkaClient mKafkaClient;
    private QuboleClient mQuboleClient;
    private String mFileExtension;
    private final boolean usingHourly;

    public PartitionFinalizer(SecorConfig config) throws Exception {
        mConfig = config;
        mKafkaClient = new KafkaClient(mConfig);
        mZookeeperConnector = new ZookeeperConnector(mConfig);
        mMessageParser = (TimestampedMessageParser) ReflectionUtil.createMessageParser(
          mConfig.getMessageParserClass(), mConfig);
        mQuboleClient = new QuboleClient(mConfig);
        if (mConfig.getCompressionCodec() != null && !mConfig.getCompressionCodec().isEmpty()) {
            CompressionCodec codec = CompressionUtil.createCompressionCodec(mConfig.getCompressionCodec());
            mFileExtension = codec.getDefaultExtension();
        } else {
            mFileExtension = "";
        }
        usingHourly = config.getMessageTimestampUsingHour();
    }

    private long getLastTimestampMillis(TopicPartition topicPartition) throws Exception {
        Message message = mKafkaClient.getLastMessage(topicPartition);
        if (message == null) {
            // This will happen if no messages have been posted to the given topic partition.
            LOG.error("No message found for topic {} partition {}" + topicPartition.getTopic(), topicPartition.getPartition());
            return -1;
        }
        return mMessageParser.extractTimestampMillis(message);
    }

    private long getLastTimestampMillis(String topic) throws Exception {
        final int numPartitions = mKafkaClient.getNumPartitions(topic);
        long max_timestamp = Long.MIN_VALUE;
        for (int partition = 0; partition < numPartitions; ++partition) {
            TopicPartition topicPartition = new TopicPartition(topic, partition);
            long timestamp = getLastTimestampMillis(topicPartition);
            if (timestamp > max_timestamp) {
                max_timestamp = timestamp;
            }
        }
        if (max_timestamp == Long.MIN_VALUE) {
            return -1;
        }
        return max_timestamp;
    }

    private long getCommittedTimestampMillis(TopicPartition topicPartition) throws Exception {
        Message message = mKafkaClient.getCommittedMessage(topicPartition);
        if (message == null) {
            LOG.error("No message found for topic {} partition {}", topicPartition.getTopic(), topicPartition.getPartition());
            return -1;
        }
        return mMessageParser.extractTimestampMillis(message);
    }

    private long getCommittedTimestampMillis(String topic) throws Exception {
        final int numPartitions = mKafkaClient.getNumPartitions(topic);
        long minTimestamp = Long.MAX_VALUE;
        for (int partition = 0; partition < numPartitions; ++partition) {
            TopicPartition topicPartition = new TopicPartition(topic, partition);
            long timestamp = getCommittedTimestampMillis(topicPartition);
            if (timestamp == -1) {
                return -1;
            } else {
                if (timestamp < minTimestamp) {
                    minTimestamp = timestamp;
                }
            }
        }
        if (minTimestamp == Long.MAX_VALUE) {
            return -1;
        }
        return minTimestamp;
    }

    private NavigableSet<Calendar> getPartitions(String topic) throws IOException, ParseException {
        final String s3Prefix = "s3n://" + mConfig.getS3Bucket() + "/" + mConfig.getS3Path();
        String[] partitions = usingHourly ? new String[]{"dt=", "hr="} : new String[]{"dt="};
        LogFilePath logFilePath = new LogFilePath(s3Prefix, topic, partitions,
            mConfig.getGeneration(), 0, 0, mFileExtension);
        String parentDir = logFilePath.getLogFileParentDir();
        String[] partitionDirs = FileUtil.list(parentDir);
        if (usingHourly) {
            List<String> dirs = new ArrayList<String>();
            for (String partionDir : partitionDirs) {
                dirs.addAll(Arrays.asList(FileUtil.list(partionDir)));
            }
            partitionDirs = dirs.toArray(new String[dirs.size()]);
        }
        String patternStr = ".*/dt=(\\d\\d\\d\\d-\\d\\d-\\d\\d)";
        if (usingHourly) {
            patternStr += "/hr=(\\d\\d)";
        }
        patternStr += "$";
        LOG.info("patternStr: " + patternStr);
        Pattern pattern = Pattern.compile(patternStr);
        TreeSet<Calendar> result = new TreeSet<Calendar>();
        for (String partitionDir : partitionDirs) {
            Matcher matcher = pattern.matcher(partitionDir);
            if (matcher.find()) {
                String date = matcher.group(1);
                String hour = usingHourly ? matcher.group(2) : "00";
                SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd-HH");
                format.setTimeZone(TimeZone.getTimeZone("UTC"));
                Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
                calendar.setTime(format.parse(date + "-" + hour));
                result.add(calendar);
            }
        }
        return result;
    }

    private void finalizePartitionsUpTo(String topic, Calendar calendar) throws IOException,
            ParseException, InterruptedException {
        NavigableSet<Calendar> partitionDates =
            getPartitions(topic).headSet(calendar, true).descendingSet();
        final String s3Prefix = "s3n://" + mConfig.getS3Bucket() + "/" + mConfig.getS3Path();
        SimpleDateFormat dtFormat = new SimpleDateFormat("yyyy-MM-dd");
        SimpleDateFormat hrFormat = new SimpleDateFormat("HH");
        dtFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        hrFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        NavigableSet<Calendar> finishedDates = new TreeSet<Calendar>();
        for (Calendar partition : partitionDates) {
            String dtPartitionStr = dtFormat.format(partition.getTime());
            String hrPartitionStr = hrFormat.format(partition.getTime());
            String[] partitions = usingHourly
                                  ? new String[]{"dt=" + dtPartitionStr, "hr=" + hrPartitionStr}
                                  : new String[]{"dt=" + dtPartitionStr};
            LogFilePath logFilePath = new LogFilePath(s3Prefix, topic, partitions,
                mConfig.getGeneration(), 0, 0, mFileExtension);
            String logFileDir = logFilePath.getLogFileDir();
            assert FileUtil.exists(logFileDir) : "FileUtil.exists(" + logFileDir + ")";
            String successFilePath = logFileDir + "/_SUCCESS";
            if (FileUtil.exists(successFilePath)) {
                LOG.info("File exist already, short circuit return. " + successFilePath);
                 break;
            }
            try {
                String parStr = "dt='" + dtPartitionStr + "'";
                if (usingHourly) {
                    parStr += ", hr='" + hrPartitionStr + "'";
                }
                String hivePrefix = null;
                try {
                    hivePrefix = mConfig.getHivePrefix();
                } catch (RuntimeException ex) {
                    LOG.warn("HivePrefix is not defined.  Skip hive registration");
                }
                if (hivePrefix != null) {
                    mQuboleClient.addPartition(hivePrefix + topic, parStr);
                }
            } catch (Exception e) {
                LOG.error("failed to finalize topic " + topic
                        + " partition dt=" + dtPartitionStr + " hr=" + hrPartitionStr,
                        e);
                continue;
            }
            LOG.info("touching file {}", successFilePath);
            FileUtil.touch(successFilePath);


            // We need to mark the successFile for the dt folder as well
            if (usingHourly) {
                Calendar yesterday = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
                yesterday.setTimeInMillis(partition.getTimeInMillis());
                yesterday.add(Calendar.DAY_OF_MONTH, -1);
                finishedDates.add(yesterday);
            }
        }

        // Reverse order to enable short circuit return
        finishedDates = finishedDates.descendingSet();
        for (Calendar partition : finishedDates) {
            String dtPartitionStr = dtFormat.format(partition.getTime());
            String[] partitions = new String[]{"dt=" + dtPartitionStr};
            LogFilePath logFilePath = new LogFilePath(s3Prefix, topic, partitions,
                mConfig.getGeneration(), 0, 0, mFileExtension);
            String logFileDir = logFilePath.getLogFileDir();
            String successFilePath = logFileDir + "/_SUCCESS";
            if (FileUtil.exists(successFilePath)) {
                LOG.info("File exist already, short circuit return. " + successFilePath);
                break;
            }
            LOG.info("touching file " + successFilePath);
            FileUtil.touch(successFilePath);
        }
    }

    /**
     * Get finalized timestamp for a given topic partition. Finalized timestamp is the current time
     * if the last offset for that topic partition has been committed earlier than an hour ago.
     * Otherwise, finalized timestamp is the committed timestamp.
     *
     * @param topicPartition The topic partition for which we want to compute the finalized
     *                       timestamp.
     * @return The finalized timestamp for the topic partition.
     * @throws Exception
     */
    private long getFinalizedTimestampMillis(TopicPartition topicPartition) throws Exception {
        long lastTimestamp = getLastTimestampMillis(topicPartition);
        long committedTimestamp = getCommittedTimestampMillis(topicPartition);
        long now = System.currentTimeMillis();
        if (lastTimestamp == committedTimestamp && (now - lastTimestamp) > 3600 * 1000) {
            return now;
        }
        return committedTimestamp;
    }

    private long getFinalizedTimestampMillis(String topic) throws Exception {
        final int numPartitions = mKafkaClient.getNumPartitions(topic);
        long minTimestamp = Long.MAX_VALUE;
        for (int partition = 0; partition < numPartitions; ++partition) {
            TopicPartition topicPartition = new TopicPartition(topic, partition);
            long timestamp = getFinalizedTimestampMillis(topicPartition);
            LOG.info("finalized timestamp for topic {} partition {} is {}", topic, partition, timestamp);
            if (timestamp == -1) {
                return -1;
            } else {
                if (timestamp < minTimestamp) {
                    minTimestamp = timestamp;
                }
            }
        }
        if (minTimestamp == Long.MAX_VALUE) {
            return -1;
        }
        return minTimestamp;
    }

    public void finalizePartitions() throws Exception {
        List<String> topics = mZookeeperConnector.getCommittedOffsetTopics();
        for (String topic : topics) {
            if (!topic.matches(mConfig.getKafkaTopicFilter())) {
                LOG.info("skipping topic {}", topic);
            } else {
                LOG.info("finalizing topic {}", topic);
                long finalizedTimestampMillis = getFinalizedTimestampMillis(topic);
                LOG.info("finalized timestamp for topic {} is {}", topic , finalizedTimestampMillis);
                if (finalizedTimestampMillis != -1) {
                    Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
                    calendar.setTimeInMillis(finalizedTimestampMillis);
                    if (!usingHourly) {
                        calendar.add(Calendar.DAY_OF_MONTH, -1);
                    }
                    // Introduce a lag of one hour.
                    calendar.add(Calendar.HOUR, -1);
                    finalizePartitionsUpTo(topic, calendar);
                }
            }
        }
    }
}
