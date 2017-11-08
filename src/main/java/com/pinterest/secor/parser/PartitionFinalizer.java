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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Stack;

/**
 * Partition finalizer writes _SUCCESS files to date partitions that very likely won't be receiving
 * any new messages. It also adds those partitions to Hive.
 *
 * @author Pawel Garbacki (pawel@pinterest.com)
 */
public class PartitionFinalizer {
    private static final Logger LOG = LoggerFactory.getLogger(PartitionFinalizer.class);

    private final SecorConfig mConfig;
    private final ZookeeperConnector mZookeeperConnector;
    private final TimestampedMessageParser mMessageParser;
    private final KafkaClient mKafkaClient;
    private final QuboleClient mQuboleClient;
    private final String mFileExtension;
    private final int mLookbackPeriods;

    public PartitionFinalizer(SecorConfig config) throws Exception {
        mConfig = config;
        mKafkaClient = new KafkaClient(mConfig);
        mZookeeperConnector = new ZookeeperConnector(mConfig);
        mMessageParser = (TimestampedMessageParser) ReflectionUtil.createMessageParser(
          mConfig.getMessageParserClass(), mConfig);
        mQuboleClient = new QuboleClient(mConfig);
        if (mConfig.getFileExtension() != null && !mConfig.getFileExtension().isEmpty()) {
            mFileExtension = mConfig.getFileExtension();
        } else if (mConfig.getCompressionCodec() != null && !mConfig.getCompressionCodec().isEmpty()) {
            CompressionCodec codec = CompressionUtil.createCompressionCodec(mConfig.getCompressionCodec());
            mFileExtension = codec.getDefaultExtension();
        } else {
            mFileExtension = "";
        }
        mLookbackPeriods = config.getFinalizerLookbackPeriods();
        LOG.info("Lookback periods: " + mLookbackPeriods);
    }

    private String[] getFinalizedUptoPartitions(String topic) throws Exception {
        final int numPartitions = mKafkaClient.getNumPartitions(topic);
        List<Message> lastMessages = new ArrayList<Message>(numPartitions);
        List<Message> committedMessages = new ArrayList<Message>(numPartitions);
        for (int partition = 0; partition < numPartitions; ++partition) {
            TopicPartition topicPartition = new TopicPartition(topic, partition);
            Message lastMessage = mKafkaClient.getLastMessage(topicPartition);
            Message committedMessage = mKafkaClient.getCommittedMessage(topicPartition);
            if (lastMessage == null || committedMessage == null) {
                // This will happen if no messages have been posted to the given topic partition.
                LOG.error("For topic {} partition {}, lastMessage: {}, committed: {}",
                    topicPartition.getTopic(), topicPartition.getPartition(),
                    lastMessage, committedMessage);
                continue;
            }
            lastMessages.add(lastMessage);
            committedMessages.add(committedMessage);
        }
        return mMessageParser.getFinalizedUptoPartitions(lastMessages, committedMessages);
    }

    private void finalizePartitionsUpTo(String topic, String[] uptoPartitions) throws Exception {
        String prefix = FileUtil.getPrefix(topic, mConfig);
        LOG.info("Finalize up to (but not include) {}, dim: {}",
            uptoPartitions, uptoPartitions.length);

        String[] previous = mMessageParser.getPreviousPartitions(uptoPartitions);
        Stack<String[]> toBeFinalized = new Stack<String[]>();
        // Walk backwards to collect all partitions which are previous to the upTo partition
        // Do not include the upTo partition
        // Stop at the first partition which already have the SUCCESS file
        for (int i = 0; i < mLookbackPeriods; i++) {
            LOG.info("Looking for partition: " + Arrays.toString(previous));
            LogFilePath logFilePath = new LogFilePath(prefix, topic, previous,
                mConfig.getGeneration(), 0, 0, mFileExtension);

            if (FileUtil.s3PathPrefixIsAltered(logFilePath.getLogFilePath(), mConfig)) {
                logFilePath = logFilePath.withPrefix(FileUtil.getS3AlternativePrefix(mConfig));
            }

            String logFileDir = logFilePath.getLogFileDir();
            if (FileUtil.exists(logFileDir)) {
                String successFilePath = logFileDir + "/_SUCCESS";
                if (FileUtil.exists(successFilePath)) {
                    LOG.info(
                        "SuccessFile exist already, short circuit return. " + successFilePath);
                    break;
                }
                LOG.info("Folder {} exists and ready to be finalized.", logFileDir);
                toBeFinalized.push(previous);
            } else {
                LOG.info("Folder {} doesn't exist, skip", logFileDir);
            }
            previous = mMessageParser.getPreviousPartitions(previous);
        }

        LOG.info("To be finalized partitions: {}", toBeFinalized);
        if (toBeFinalized.isEmpty()) {
            LOG.warn("There is no partitions to be finalized.");
            return;
        }

        // Now walk forward the collected partitions to do the finalization
        // Note we are deliberately walking backwards and then forwards to make sure we don't
        // end up in a situation that a later date partition is finalized and then the system
        // crashes (which creates unfinalized partition folders in between)
        while (!toBeFinalized.isEmpty()) {
            String[] current = toBeFinalized.pop();
            LOG.info("Finalizing partition: " + Arrays.toString(current));
            // We only perform hive registration on the last dimension of the partition array
            // i.e. only do hive registration for the hourly folder, but not for the daily
            if (uptoPartitions.length == current.length) {
                try {
                    StringBuilder sb = new StringBuilder();
                    for (int i = 0; i < current.length; i++) {
                        String par = current[i];
                        // We expect the partition array in the form of key=value if
                        // they need to go through hive registration
                        String[] parts = par.split("=");
                        assert parts.length == 2 : "wrong partition format: " + par;
                        if (i > 0) {
                            sb.append(",");
                        }
                        sb.append(parts[0]);
                        sb.append("='");
                        sb.append(parts[1]);
                        sb.append("'");
                    }
                    LOG.info("Hive partition string: " + sb);

                    String hiveTableName = mConfig.getHiveTableName(topic);
                    LOG.info("Hive table name from config: {}", hiveTableName);
                    if (hiveTableName == null) {
                        String hivePrefix = null;
                        try {
                            hivePrefix = mConfig.getHivePrefix();
                            hiveTableName = hivePrefix + topic;
                            LOG.info("Hive table name from prefix: {}", hiveTableName);
                        } catch (RuntimeException ex) {
                            LOG.warn("HivePrefix is not defined.  Skip hive registration");
                        }
                    }
                    if (hiveTableName != null && mConfig.getQuboleEnabled()) {
                        mQuboleClient.addPartition(hiveTableName, sb.toString());
                    }
                } catch (Exception e) {
                    LOG.error("failed to finalize topic " + topic, e);
                    continue;
                }
            }

            // Generate the SUCCESS file at the end
            LogFilePath logFilePath = new LogFilePath(prefix, topic, current,
                mConfig.getGeneration(), 0, 0, mFileExtension);

            if (FileUtil.s3PathPrefixIsAltered(logFilePath.getLogFilePath(), mConfig)) {
                logFilePath = logFilePath.withPrefix(FileUtil.getS3AlternativePrefix(mConfig));
                LOG.info("Will finalize alternative s3 logFilePath {}", logFilePath);
            }

            String logFileDir = logFilePath.getLogFileDir();
            String successFilePath = logFileDir + "/_SUCCESS";

            LOG.info("touching file {}", successFilePath);
            FileUtil.touch(successFilePath);
        }
    }

    public void finalizePartitions() throws Exception {
        List<String> topics = mZookeeperConnector.getCommittedOffsetTopics();
        for (String topic : topics) {
            if (!topic.matches(mConfig.getKafkaTopicFilter())) {
                LOG.info("skipping topic {}", topic);
            } else {
                LOG.info("finalizing topic {}", topic);
                String[] partitions = getFinalizedUptoPartitions(topic);
                LOG.info("finalized timestamp for topic {} is {}", topic , partitions);
                if (partitions != null) {
                    finalizePartitionsUpTo(topic, partitions);
                }
            }
        }
    }
}
