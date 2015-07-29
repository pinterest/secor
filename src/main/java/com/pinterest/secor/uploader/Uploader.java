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
import com.pinterest.secor.util.CompressionUtil;
import com.pinterest.secor.util.FileUtil;
import com.pinterest.secor.util.IdUtil;
import com.pinterest.secor.util.ReflectionUtil;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Uploader applies a set of policies to determine if any of the locally stored files should be
 * uploaded to the cloud.
 *
 * @author Pawel Garbacki (pawel@pinterest.com)
 */
public class Uploader {
    private static final Logger LOG = LoggerFactory.getLogger(Uploader.class);

    private static final ExecutorService executor = Executors.newFixedThreadPool(256);

    private SecorConfig mConfig;
    private OffsetTracker mOffsetTracker;
    private FileRegistry mFileRegistry;
    private ZookeeperConnector mZookeeperConnector;

    public Uploader(SecorConfig config, OffsetTracker offsetTracker, FileRegistry fileRegistry) {
        this(config, offsetTracker, fileRegistry, new ZookeeperConnector(config));
    }

    // For testing use only.
    public Uploader(SecorConfig config, OffsetTracker offsetTracker, FileRegistry fileRegistry,
                    ZookeeperConnector zookeeperConnector) {
        mConfig = config;
        mOffsetTracker = offsetTracker;
        mFileRegistry = fileRegistry;
        mZookeeperConnector = zookeeperConnector;
    }

    private Future<?> upload(LogFilePath localPath) throws Exception {
    	String prefix = "";
        if (mConfig.getCloudService() != null && mConfig.getCloudService().equals("Swift")){ // the first condition 
        																					 // is for compile tests
        	String container = null;
        	if (mConfig.getSeperateContainersForTopics()) {
        		if (!FileUtil.exists("swift://" + localPath.getTopic() + ".GENERICPROJECT")){
        			try {
                    	FileUtil.CreateContainer(localPath.getTopic());
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
        		}
        		container = localPath.getTopic();
        	} else {
        		container = mConfig.getSwiftContainer();
        	}
        	prefix = "swift://" + container + ".GENERICPROJECT/" + mConfig.getSwiftPath();
    	} else {
	        prefix = "s3n://" + mConfig.getS3Bucket() + "/" + mConfig.getS3Path();
    	}
        LogFilePath path = new LogFilePath(prefix, localPath.getTopic(),
                                             localPath.getPartitions(),
                                             localPath.getGeneration(),
                                             localPath.getKafkaPartition(),
                                             localPath.getOffset(),
                                             localPath.getExtension());
        final String localLogFilename = localPath.getLogFilePath();
        final String logFilename = path.getLogFilePath();
        LOG.info("uploading file {} to {}", localLogFilename, logFilename);
        return executor.submit(new Runnable() {
            @Override
            public void run() {
                try {
               	    FileUtil.moveToCloud(localLogFilename, logFilename);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });
    }

    private void uploadFiles(TopicPartition topicPartition) throws Exception {
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
            long zookeeperComittedOffsetCount = mZookeeperConnector.getCommittedOffsetCount(
                    topicPartition);
            if (zookeeperComittedOffsetCount == committedOffsetCount) {
                LOG.info("uploading topic {} partition {}", topicPartition.getTopic(), topicPartition.getPartition());
                // Deleting writers closes their streams flushing all pending data to the disk.
                mFileRegistry.deleteWriters(topicPartition);
                Collection<LogFilePath> paths = mFileRegistry.getPaths(topicPartition);
                List<Future<?>> uploadFutures = new ArrayList<Future<?>>();
                for (LogFilePath path : paths) {
                    uploadFutures.add(upload(path));
                }
                for (Future<?> uploadFuture : uploadFutures) {
                    uploadFuture.get();
                }
                mFileRegistry.deleteTopicPartition(topicPartition);
                mZookeeperConnector.setCommittedOffsetCount(topicPartition, lastSeenOffset + 1);
                mOffsetTracker.setCommittedOffsetCount(topicPartition, lastSeenOffset + 1);
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
                codec
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
                if (keyVal.getKey() >= startOffset) {
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

    private void trimFiles(TopicPartition topicPartition, long startOffset) throws Exception {
        Collection<LogFilePath> paths = mFileRegistry.getPaths(topicPartition);
        for (LogFilePath path : paths) {
            trim(path, startOffset);
        }
    }

    private void checkTopicPartition(TopicPartition topicPartition) throws Exception {
        final long size = mFileRegistry.getSize(topicPartition);
        final long modificationAgeSec = mFileRegistry.getModificationAgeSec(topicPartition);
        LOG.debug("size: " + size + " modificationAge: " + modificationAgeSec);
        if (size >= mConfig.getMaxFileSizeBytes() ||
                modificationAgeSec >= mConfig.getMaxFileAgeSeconds()) {
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

    public void applyPolicy() throws Exception {
        Collection<TopicPartition> topicPartitions = mFileRegistry.getTopicPartitions();
        for (TopicPartition topicPartition : topicPartitions) {
            checkTopicPartition(topicPartition);
        }
    }
}
