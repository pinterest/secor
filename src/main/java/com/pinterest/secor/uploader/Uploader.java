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
package com.pinterest.secor.uploader;

import com.google.common.base.Joiner;
import com.pinterest.secor.common.*;
import com.pinterest.secor.io.FileReader;
import com.pinterest.secor.io.FileWriter;
import com.pinterest.secor.io.KeyValue;
import com.pinterest.secor.monitoring.MetricCollector;
import com.pinterest.secor.util.CompressionUtil;
import com.pinterest.secor.util.IdUtil;
import com.pinterest.secor.util.ReflectionUtil;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Uploader applies a set of policies to determine if any of the locally stored files should be
 * uploaded to the cloud.
 *
 * @author Pawel Garbacki (pawel@pinterest.com)
 */
public class Uploader {
    private static final Logger LOG = LoggerFactory.getLogger(Uploader.class);

    protected SecorConfig mConfig;
    protected MetricCollector mMetricCollector;
    protected OffsetTracker mOffsetTracker;
    protected FileRegistry mFileRegistry;
    protected ZookeeperConnector mZookeeperConnector;
    protected UploadManager mUploadManager;
    protected String mTopicFilter;


    /**
     * Init the Uploader with its dependent objects.
     *
     * @param config Secor configuration
     * @param offsetTracker Tracker of the current offset of topics partitions
     * @param fileRegistry Registry of log files on a per-topic and per-partition basis
     * @param uploadManager Manager of the physical upload of log files to the remote repository
     * @param metricCollector component that ingest metrics into monitoring system
     */
    public void init(SecorConfig config, OffsetTracker offsetTracker, FileRegistry fileRegistry,
                     UploadManager uploadManager, MetricCollector metricCollector) {
        init(config, offsetTracker, fileRegistry, uploadManager,
                new ZookeeperConnector(config), metricCollector);
    }

    // For testing use only.
    public void init(SecorConfig config, OffsetTracker offsetTracker, FileRegistry fileRegistry,
                     UploadManager uploadManager,
                     ZookeeperConnector zookeeperConnector, MetricCollector metricCollector) {
        mConfig = config;
        mOffsetTracker = offsetTracker;
        mFileRegistry = fileRegistry;
        mUploadManager = uploadManager;
        mZookeeperConnector = zookeeperConnector;
        mTopicFilter = mConfig.getKafkaTopicUploadAtMinuteMarkFilter();
        mMetricCollector = metricCollector;
    }

    protected void uploadFiles(TopicPartition topicPartition) throws Exception {
        long committedOffsetCount = mOffsetTracker.getTrueCommittedOffsetCount(topicPartition);
        long lastSeenOffset = mOffsetTracker.getLastSeenOffset(topicPartition);

        String stripped = StringUtils.strip(mConfig.getZookeeperPath(), "/");
        final String lockPath = Joiner.on("/").skipNulls().join(
            "",
            stripped.isEmpty() ? null : stripped,
            "secor",
            "locks",
            topicPartition.getTopic(),
            topicPartition.getPartition());

        mZookeeperConnector.lock(lockPath);
        try {
            // Check if the committed offset has changed.
            long zookeeperCommittedOffsetCount = mZookeeperConnector.getCommittedOffsetCount(
                    topicPartition);
            if (zookeeperCommittedOffsetCount == committedOffsetCount) {
                LOG.info("uploading topic {} partition {}", topicPartition.getTopic(), topicPartition.getPartition());
                // Deleting writers closes their streams flushing all pending data to the disk.
                mFileRegistry.deleteWriters(topicPartition);
                Collection<LogFilePath> paths = mFileRegistry.getPaths(topicPartition);
                List<Handle<?>> uploadHandles = new ArrayList<Handle<?>>();
                for (LogFilePath path : paths) {
                    uploadHandles.add(mUploadManager.upload(path));
                }
                for (Handle<?> uploadHandle : uploadHandles) {
                    uploadHandle.get();
                }
                mFileRegistry.deleteTopicPartition(topicPartition);
                mZookeeperConnector.setCommittedOffsetCount(topicPartition, lastSeenOffset + 1);
                mOffsetTracker.setCommittedOffsetCount(topicPartition, lastSeenOffset + 1);

                mMetricCollector.increment("uploader.file_uploads.count", paths.size(), topicPartition.getTopic());
            }
        } finally {
            mZookeeperConnector.unlock(lockPath);
        }
    }

    /**
     * This method is intended to be overwritten in tests.
     * @throws Exception
     */
    protected FileReader createReader(LogFilePath srcPath, CompressionCodec codec) throws Exception {
        return ReflectionUtil.createFileReader(
                mConfig.getFileReaderWriterFactory(),
                srcPath,
                codec,
                mConfig
        );
    }

    private void trim(LogFilePath srcPath, long startOffset) throws Exception {
        if (startOffset == srcPath.getOffset()) {
            return;
        }
        FileReader reader = null;
        FileWriter writer = null;
        LogFilePath dstPath = null;
        int copiedMessages = 0;
        // Deleting the writer closes its stream flushing all pending data to the disk.
        mFileRegistry.deleteWriter(srcPath);
        try {
            CompressionCodec codec = null;
            String extension = "";
            if (mConfig.getCompressionCodec() != null && !mConfig.getCompressionCodec().isEmpty()) {
                codec = CompressionUtil.createCompressionCodec(mConfig.getCompressionCodec());
                extension = codec.getDefaultExtension();
            }
            reader = createReader(srcPath, codec);
            KeyValue keyVal;
            while ((keyVal = reader.next()) != null) {
                if (keyVal.getOffset() >= startOffset) {
                    if (writer == null) {
                        String localPrefix = mConfig.getLocalPath() + '/' +
                            IdUtil.getLocalMessageDir();
                        dstPath = new LogFilePath(localPrefix, srcPath.getTopic(),
                                                  srcPath.getPartitions(), srcPath.getGeneration(),
                                                  srcPath.getKafkaPartition(), startOffset,
                                                  extension);
                        writer = mFileRegistry.getOrCreateWriter(dstPath,
                        		codec);
                    }
                    writer.write(keyVal);
                    copiedMessages++;
                }
            }
        } finally {
            if (reader != null) {
                reader.close();
            }
        }
        mFileRegistry.deletePath(srcPath);
        if (dstPath == null) {
            LOG.info("removed file {}", srcPath.getLogFilePath());
        } else {
            LOG.info("trimmed {} messages from {} to {} with start offset {}",
                    copiedMessages, srcPath.getLogFilePath(), dstPath.getLogFilePath(), startOffset);
        }
    }

    protected void trimFiles(TopicPartition topicPartition, long startOffset) throws Exception {
        Collection<LogFilePath> paths = mFileRegistry.getPaths(topicPartition);
        for (LogFilePath path : paths) {
            trim(path, startOffset);
        }
    }

    /***
     * If the topic is in the list of topics to upload at a specific time. For example at a minute mark.
     * @param topicPartition
     * @return
     * @throws Exception
     */
    private boolean isRequiredToUploadAtTime(TopicPartition topicPartition) throws Exception{
        final String topic = topicPartition.getTopic();
        if (mTopicFilter == null || mTopicFilter.isEmpty()){
            return false;
        }
        if (topic.matches(mTopicFilter)){
            if (DateTime.now().minuteOfHour().get() == mConfig.getUploadMinuteMark()){
               return true;
            }
        }
        return false;
    }

    protected void checkTopicPartition(TopicPartition topicPartition) throws Exception {
        final long size = mFileRegistry.getSize(topicPartition);
        final long modificationAgeSec = mFileRegistry.getModificationAgeSec(topicPartition);
        LOG.debug("size: " + size + " modificationAge: " + modificationAgeSec);
        if (size >= mConfig.getMaxFileSizeBytes() ||
                modificationAgeSec >= mConfig.getMaxFileAgeSeconds() ||
                isRequiredToUploadAtTime(topicPartition)) {
            long newOffsetCount = mZookeeperConnector.getCommittedOffsetCount(topicPartition);
            long oldOffsetCount = mOffsetTracker.setCommittedOffsetCount(topicPartition,
                    newOffsetCount);
            long lastSeenOffset = mOffsetTracker.getLastSeenOffset(topicPartition);
            if (oldOffsetCount == newOffsetCount) {
                LOG.debug("Uploading for: " + topicPartition);
                uploadFiles(topicPartition);
            } else if (newOffsetCount > lastSeenOffset) {  // && oldOffset < newOffset
                LOG.debug("last seen offset {} is lower than committed offset count {}. Deleting files in topic {} partition {}",
                        lastSeenOffset, newOffsetCount,topicPartition.getTopic(), topicPartition.getPartition());
                // There was a rebalancing event and someone committed an offset beyond that of the
                // current message.  We need to delete the local file.
                mFileRegistry.deleteTopicPartition(topicPartition);
            } else {  // oldOffsetCount < newOffsetCount <= lastSeenOffset
                LOG.debug("previous committed offset count {} is lower than committed offset {} is lower than or equal to last seen offset {}. " +
                                "Trimming files in topic {} partition {}",
                        oldOffsetCount, newOffsetCount, lastSeenOffset, topicPartition.getTopic(), topicPartition.getPartition());
                // There was a rebalancing event and someone committed an offset lower than that
                // of the current message.  We need to trim local files.
                trimFiles(topicPartition, newOffsetCount);
            }
        }
    }

    /**
     * Apply the Uploader policy for pushing partition files to the underlying storage.
     *
     * For each of the partitions of the file registry, apply the policy for flushing
     * them to the underlying storage.
     *
     * This method could be subclassed to provide an alternate policy. The custom uploader
     * class name would need to be specified in the secor.upload.class.
     *
     * @throws Exception if any error occurs while appying the policy
     */
    public void applyPolicy() throws Exception {
        Collection<TopicPartition> topicPartitions = mFileRegistry.getTopicPartitions();
        for (TopicPartition topicPartition : topicPartitions) {
            checkTopicPartition(topicPartition);
        }
    }
}
