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
package com.pinterest.secor.uploader;

import com.google.common.base.Joiner;
import com.pinterest.secor.common.DeterministicUploadPolicyTracker;
import com.pinterest.secor.common.FileRegistry;
import com.pinterest.secor.common.LogFilePath;
import com.pinterest.secor.common.OffsetTracker;
import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.common.SecorConstants;
import com.pinterest.secor.common.TopicPartition;
import com.pinterest.secor.common.ZookeeperConnector;
import com.pinterest.secor.io.FileReader;
import com.pinterest.secor.io.FileWriter;
import com.pinterest.secor.io.KeyValue;
import com.pinterest.secor.monitoring.MetricCollector;
import com.pinterest.secor.reader.MessageReader;
import com.pinterest.secor.util.CompressionUtil;
import com.pinterest.secor.util.IdUtil;
import com.pinterest.secor.util.ReflectionUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

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
    protected MessageReader mMessageReader;
    private DeterministicUploadPolicyTracker mDeterministicUploadPolicyTracker;
    private boolean mUploadLastSeenOffset;
    protected String mTopicFilter;

    private boolean isOffsetsStorageKafka = false;


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
                     UploadManager uploadManager, MessageReader messageReader, MetricCollector metricCollector,
                     DeterministicUploadPolicyTracker deterministicUploadPolicyTracker) {
        init(config, offsetTracker, fileRegistry, uploadManager, messageReader,
                new ZookeeperConnector(config), metricCollector, deterministicUploadPolicyTracker);
    }

    // For testing use only.
    public void init(SecorConfig config, OffsetTracker offsetTracker, FileRegistry fileRegistry,
                     UploadManager uploadManager, MessageReader messageReader,
                     ZookeeperConnector zookeeperConnector, MetricCollector metricCollector,
                     DeterministicUploadPolicyTracker deterministicUploadPolicyTracker) {
        mConfig = config;
        mOffsetTracker = offsetTracker;
        mFileRegistry = fileRegistry;
        mUploadManager = uploadManager;
        mMessageReader = messageReader;
        mZookeeperConnector = zookeeperConnector;
        mTopicFilter = mConfig.getKafkaTopicUploadAtMinuteMarkFilter();
        mMetricCollector = metricCollector;
        mDeterministicUploadPolicyTracker = deterministicUploadPolicyTracker;
        if (mConfig.getOffsetsStorage().equals(SecorConstants.KAFKA_OFFSETS_STORAGE_KAFKA)) {
            isOffsetsStorageKafka = true;
        }
        mUploadLastSeenOffset = mConfig.getUploadLastSeenOffset();
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
                if (mUploadLastSeenOffset) {
                    long zkLastSeenOffset = mZookeeperConnector.getLastSeenOffsetCount(topicPartition);
                    // If the in-memory lastSeenOffset is less than ZK's, this means there was a failed
                    // attempt uploading for this topic partition and we already have some files uploaded
                    // on S3, e.g.
                    //     s3n://topic/partition/day/hour=0/offset1
                    //     s3n://topic/partition/day/hour=1/offset1
                    // Since our in-memory accumulated files (offsets) is less than what's on ZK, we
                    // might have less files to upload, e.g.
                    //     localfs://topic/partition/day/hour=0/offset1
                    // If we continue uploading, we will upload this file:
                    //     s3n://topic/partition/day/hour=0/offset1
                    // But the next file to be uploaded will become:
                    //     s3n://topic/partition/day/hour=1/offset2
                    // So we will end up with 2 different files for hour=1/
                    // We should wait a bit longer to have at least getting to the same offset as ZK's
                    //
                    // Note We use offset + 1 throughout secor when we persist to ZK
                    if (lastSeenOffset + 1 < zkLastSeenOffset) {
                        LOG.warn("TP {}, ZK lastSeenOffset {}, in-memory lastSeenOffset {}, skip uploading this time",
                            topicPartition, zkLastSeenOffset, lastSeenOffset + 1);
                        mMetricCollector.increment("uploader.offset_delays", topicPartition.getTopic());
                    }
                    LOG.info("Setting lastSeenOffset for {} with {}", topicPartition, lastSeenOffset + 1);
                    mZookeeperConnector.setLastSeenOffsetCount(topicPartition, lastSeenOffset + 1);
                }
                LOG.info("uploading topic {} partition {}", topicPartition.getTopic(), topicPartition.getPartition());
                // Deleting writers closes their streams flushing all pending data to the disk.
                mFileRegistry.deleteWriters(topicPartition);
                Collection<LogFilePath> paths = mFileRegistry.getPaths(topicPartition);
                List<Handle<?>> uploadHandles = new ArrayList<Handle<?>>();
                for (LogFilePath path : paths) {
                    uploadHandles.add(mUploadManager.upload(path));
                }
                for (Handle<?> uploadHandle : uploadHandles) {
                    try {
                        uploadHandle.get();
                    } catch (Exception ex) {
                        mMetricCollector.increment("uploader.upload.failures", topicPartition.getTopic());
                        throw ex;
                    }
                }
                mFileRegistry.deleteTopicPartition(topicPartition);
                if (mDeterministicUploadPolicyTracker != null) {
                    mDeterministicUploadPolicyTracker.reset(topicPartition);
                }
                mZookeeperConnector.setCommittedOffsetCount(topicPartition, lastSeenOffset + 1);
                mOffsetTracker.setCommittedOffsetCount(topicPartition, lastSeenOffset + 1);
                if (isOffsetsStorageKafka) {
                    mMessageReader.commit(topicPartition, lastSeenOffset + 1);
                }
                mMetricCollector.increment("uploader.file_uploads.count", paths.size(), topicPartition.getTopic());
            } else {
                LOG.warn("Zookeeper committed offset didn't match for topic {} partition {}: {} vs {}",
                         topicPartition.getTopic(), topicPartition.getTopic(), zookeeperCommittedOffsetCount,
                         committedOffsetCount);
                mMetricCollector.increment("uploader.offset_mismatches", topicPartition.getTopic());
            }
        } finally {
            mZookeeperConnector.unlock(lockPath);
        }
    }

    /**
     * This method is intended to be overwritten in tests.
     * @param srcPath source Path
     * @param codec compression codec
     * @return FileReader created file reader
     * @throws Exception on error
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
        final TopicPartition topicPartition = new TopicPartition(srcPath.getTopic(), srcPath.getKafkaPartition());
        if (startOffset == srcPath.getOffset()) {
            // If *all* the files had the right offset already, trimFiles would have returned
            // before resetting the tracker. If just some do, we don't want to rewrite files in place
            // (it's probably safe but let's not stress it), but this shouldn't happen anyway.
            throw new RuntimeException("Some LogFilePath has unchanged offset, but they don't all? " + srcPath);
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
                    if (mDeterministicUploadPolicyTracker != null) {
                        mDeterministicUploadPolicyTracker.track(topicPartition, keyVal);
                    }
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
        if (paths.stream().allMatch(srcPath -> srcPath.getOffset() == startOffset)) {
            // We thought we needed to trim, but we were wrong: we already had started at the right offset.
            // (Probably because we don't initialize the offset from ZK on startup.)
            return;
        }
        if (mDeterministicUploadPolicyTracker != null) {
            mDeterministicUploadPolicyTracker.reset(topicPartition);
        }
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

    protected void checkTopicPartition(TopicPartition topicPartition, boolean forceUpload) throws Exception {
        boolean shouldUpload;
        if (mDeterministicUploadPolicyTracker != null) {
            shouldUpload = mDeterministicUploadPolicyTracker.shouldUpload(topicPartition);
        } else {
            final long size = mFileRegistry.getSize(topicPartition);
            final long modificationAgeSec = mFileRegistry.getModificationAgeSec(topicPartition);
            final int fileCount = mFileRegistry.getActiveFileCount();
            LOG.debug("size: " + size + " modificationAge: " + modificationAgeSec);
            shouldUpload = forceUpload ||
                           activeFileCountExceeded(fileCount) ||
                           size >= mConfig.getMaxFileSizeBytes() ||
                           modificationAgeSec >= mConfig.getMaxFileAgeSeconds() ||
                           isRequiredToUploadAtTime(topicPartition);
        }
        if (shouldUpload) {
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
                mMetricCollector.increment("uploader.partition_deletes", topicPartition.getTopic());
                // There was a rebalancing event and someone committed an offset beyond that of the
                // current message.  We need to delete the local file.
                mFileRegistry.deleteTopicPartition(topicPartition);
                if (mDeterministicUploadPolicyTracker != null) {
                    mDeterministicUploadPolicyTracker.reset(topicPartition);
                }
            } else {  // oldOffsetCount < newOffsetCount <= lastSeenOffset
                LOG.debug("previous committed offset count {} is lower than committed offset {} is lower than or equal to last seen offset {}. " +
                                "Trimming files in topic {} partition {}",
                        oldOffsetCount, newOffsetCount, lastSeenOffset, topicPartition.getTopic(), topicPartition.getPartition());
                mMetricCollector.increment("uploader.partition_trims", topicPartition.getTopic());
                // There was a rebalancing event and someone committed an offset lower than that
                // of the current message.  We need to trim local files.
                trimFiles(topicPartition, newOffsetCount);
                // We might still be at the right place to upload. (In fact, we always trim the first time
                // we hit the upload condition because oldOffsetCount starts at -1, but this is usually a no-op trim.)
                // Check again! This is especially important if this was an "upload in graceful shutdown".
                checkTopicPartition(topicPartition, forceUpload);
            }
        }
    }

    private boolean activeFileCountExceeded(int fileCount) {
        return mConfig.getMaxActiveFiles() > -1 && fileCount > mConfig.getMaxActiveFiles();
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
    public void applyPolicy(boolean forceUpload) throws Exception {
        Collection<TopicPartition> topicPartitions = mFileRegistry.getTopicPartitions();
        for (TopicPartition topicPartition : topicPartitions) {
            checkTopicPartition(topicPartition, forceUpload);
        }
    }
}
