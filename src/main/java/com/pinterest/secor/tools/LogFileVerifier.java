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
package com.pinterest.secor.tools;

import com.pinterest.secor.common.LogFilePath;
import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.common.TopicPartition;
import com.pinterest.secor.io.FileReader;
import com.pinterest.secor.io.KeyValue;
import com.pinterest.secor.util.CompressionUtil;
import com.pinterest.secor.util.FileUtil;
import com.pinterest.secor.util.ReflectionUtil;

import org.apache.hadoop.io.compress.CompressionCodec;

import java.io.IOException;
import java.util.*;

/**
 * Log file verifier checks the consistency of log files.
 *
 * @author Pawel Garbacki (pawel@pinterest.com)
 */
public class LogFileVerifier {
    private SecorConfig mConfig;
    private String mTopic;
    private HashMap<TopicPartition, SortedMap<Long, HashSet<LogFilePath>>>
        mTopicPartitionToOffsetToFiles;

    public LogFileVerifier(SecorConfig config, String topic) throws IOException {
        mConfig = config;
        mTopic = topic;
        mTopicPartitionToOffsetToFiles =
            new HashMap<TopicPartition, SortedMap<Long, HashSet<LogFilePath>>>();
    }

    private String getPrefix() {
        return "s3n://" + mConfig.getS3Bucket() + "/" + mConfig.getS3Path();
    }

    private String getTopicPrefix() {
        return getPrefix() + "/" + mTopic;
    }

    private void populateTopicPartitionToOffsetToFiles() throws IOException {
        String prefix = getPrefix();
        String topicPrefix = getTopicPrefix();
        String[] paths = FileUtil.listRecursively(topicPrefix);
        for (String path : paths) {
            if (!path.endsWith("/_SUCCESS")) {
                LogFilePath logFilePath = new LogFilePath(prefix, path);
                TopicPartition topicPartition = new TopicPartition(logFilePath.getTopic(),
                    logFilePath.getKafkaPartition());
                SortedMap<Long, HashSet<LogFilePath>> offsetToFiles =
                    mTopicPartitionToOffsetToFiles.get(topicPartition);
                if (offsetToFiles == null) {
                    offsetToFiles = new TreeMap<Long, HashSet<LogFilePath>>();
                    mTopicPartitionToOffsetToFiles.put(topicPartition, offsetToFiles);
                }
                long offset = logFilePath.getOffset();
                HashSet<LogFilePath> logFilePaths = offsetToFiles.get(offset);
                if (logFilePaths == null) {
                    logFilePaths = new HashSet<LogFilePath>();
                    offsetToFiles.put(offset, logFilePaths);
                }
                logFilePaths.add(logFilePath);
            }
        }
    }

    private void filterOffsets(long fromOffset, long toOffset) {
        Iterator iterator = mTopicPartitionToOffsetToFiles.entrySet().iterator();
        while (iterator.hasNext()) {
            long firstOffset = -2;
            long lastOffset = Long.MAX_VALUE;
            Map.Entry entry = (Map.Entry) iterator.next();
            SortedMap<Long, HashSet<LogFilePath>> offsetToFiles =
                (SortedMap<Long, HashSet<LogFilePath>>) entry.getValue();
            for (long offset : offsetToFiles.keySet()) {
                if (offset <= fromOffset || firstOffset == -2) {
                    firstOffset = offset;
                }
                if (offset >= toOffset && toOffset == Long.MAX_VALUE) {
                    lastOffset = offset;
                }
            }
            if (firstOffset != -2) {
                TopicPartition topicPartition = (TopicPartition) entry.getKey();
                offsetToFiles = offsetToFiles.subMap(firstOffset, lastOffset);
                mTopicPartitionToOffsetToFiles.put(topicPartition, offsetToFiles);
            }
        }
    }

    private int getMessageCount(LogFilePath logFilePath) throws Exception {
        FileReader reader = createFileReader(logFilePath);
        int result = 0;
        while (reader.next() != null) {
            result++;
        }
        reader.close();
        return result;
    }

    public void verifyCounts(long fromOffset, long toOffset, int numMessages) throws Exception {
        populateTopicPartitionToOffsetToFiles();
        filterOffsets(fromOffset, toOffset);
        Iterator iterator = mTopicPartitionToOffsetToFiles.entrySet().iterator();
        int aggregateMessageCount = 0;
        while (iterator.hasNext()) {
            long previousOffset = -2L;
            long previousMessageCount = -2L;
            Map.Entry entry = (Map.Entry) iterator.next();
            SortedMap<Long, HashSet<LogFilePath>> offsetToFiles =
                (SortedMap<Long, HashSet<LogFilePath>>) entry.getValue();
            for (HashSet<LogFilePath> logFilePaths : offsetToFiles.values()) {
                int messageCount = 0;
                long offset = -2;
                for (LogFilePath logFilePath : logFilePaths) {
                    assert offset == -2 || offset == logFilePath.getOffset():
                        Long.toString(offset) + " || " + offset + " == " + logFilePath.getOffset();
                    messageCount += getMessageCount(logFilePath);
                    offset = logFilePath.getOffset();
                }
                if (previousOffset != -2 && offset - previousOffset != previousMessageCount) {
                    TopicPartition topicPartition = (TopicPartition) entry.getKey();
                    throw new RuntimeException("Message count of " + previousMessageCount +
                                               " in topic " + topicPartition.getTopic() +
                                               " partition " + topicPartition.getPartition() +
                                               " does not agree with adjacent offsets " +
                                               previousOffset + " and " + offset);
                }
                previousOffset = offset;
                previousMessageCount = messageCount;
                aggregateMessageCount += messageCount;
            }
        }
        if (numMessages != -1 && aggregateMessageCount != numMessages) {
            throw new RuntimeException("Message count " + aggregateMessageCount +
                " does not agree with the expected count " + numMessages);
        }
    }

    private void getOffsets(LogFilePath logFilePath, Set<Long> offsets) throws Exception {
        FileReader reader = createFileReader(logFilePath);
        KeyValue record;
        while ((record = reader.next()) != null) {
            if (!offsets.add(record.getKey())) {
                throw new RuntimeException("duplicate key " + record.getKey() + " found in file " +
                    logFilePath.getLogFilePath());
            }
        }
        reader.close();
    }

    public void verifySequences(long fromOffset, long toOffset) throws Exception {
        populateTopicPartitionToOffsetToFiles();
        filterOffsets(fromOffset, toOffset);

        Iterator iterator = mTopicPartitionToOffsetToFiles.entrySet().iterator();
        while (iterator.hasNext()) {
            TreeSet<Long> offsets = new TreeSet<Long>();
            Map.Entry entry = (Map.Entry) iterator.next();
            TopicPartition topicPartition = (TopicPartition) entry.getKey();
            SortedMap<Long, HashSet<LogFilePath>> offsetToFiles =
                    (SortedMap<Long, HashSet<LogFilePath>>) entry.getValue();
            for (HashSet<LogFilePath> logFilePaths : offsetToFiles.values()) {
                for (LogFilePath logFilePath : logFilePaths) {
                    getOffsets(logFilePath, offsets);
                }
            }
            long lastOffset = -2;
            for (Long offset : offsets) {
                if (lastOffset != -2) {
                    assert lastOffset + 1 == offset: Long.toString(offset) + " + 1 == " + offset +
                        " for topic " + topicPartition.getTopic() + " partition " +
                        topicPartition.getPartition();
                }
                lastOffset = offset;
            }
        }
    }

    /**
     * Helper to create a file reader writer from config
     *
     * @param logFilePath
     * @return
     * @throws Exception
     */
    private FileReader createFileReader(LogFilePath logFilePath) throws Exception {
        CompressionCodec codec = null;
        if (mConfig.getCompressionCodec() != null && !mConfig.getCompressionCodec().isEmpty()) {
            codec = CompressionUtil.createCompressionCodec(mConfig.getCompressionCodec());
        }
        FileReader fileReader = ReflectionUtil.createFileReader(
                mConfig.getFileReaderWriterFactory(),
                logFilePath,
                codec
        );
        return fileReader;
    }
}