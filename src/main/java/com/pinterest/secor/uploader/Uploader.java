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

import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pinterest.secor.common.FileRegistry;
import com.pinterest.secor.common.LogFilePath;
import com.pinterest.secor.common.OffsetTracker;
import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.common.TopicPartition;
import com.pinterest.secor.common.ZookeeperConnector;
import com.pinterest.secor.storage.StorageFactory;
import com.pinterest.secor.util.FileUtil;

/**
 * Uploader applies a set of policies to determine if any of the locally stored
 * files should be uploaded to s3.
 * 
 * @author Pawel Garbacki (pawel@pinterest.com)
 */
public class Uploader {
	private static final Logger LOG = LoggerFactory.getLogger(Uploader.class);

	private final SecorConfig mConfig;
	private final OffsetTracker mOffsetTracker;
	private final FileRegistry mFileRegistry;
	private final ZookeeperConnector mZookeeperConnector;

	private final StorageFactory mStorageFactory;

	public Uploader(SecorConfig config, OffsetTracker offsetTracker,
			FileRegistry fileRegistry, StorageFactory storageFactory) {
		this(config, offsetTracker, fileRegistry,
				new ZookeeperConnector(config), storageFactory);
	}

	// For testing use only.
	public Uploader(SecorConfig config, OffsetTracker offsetTracker,
			FileRegistry fileRegistry, ZookeeperConnector zookeeperConnector,
			StorageFactory storageFactory) {
		mConfig = config;
		mOffsetTracker = offsetTracker;
		mFileRegistry = fileRegistry;
		mZookeeperConnector = zookeeperConnector;
		this.mStorageFactory = storageFactory;
	}

	private void upload(LogFilePath localPath) throws Exception {
		String s3Prefix = "s3n://" + mConfig.getS3Bucket() + "/"
				+ mConfig.getS3Path();
		LogFilePath s3Path = new LogFilePath(s3Prefix, localPath.getTopic(),
				localPath.getPartitions(), localPath.getGeneration(),
				localPath.getKafkaPartition(), localPath.getOffset());

		String localLogFilename = localPath.getLogFilePath();
		LOG.info("uploading file " + localLogFilename + " to "
				+ s3Path.getLogFilePath());
		FileUtil.moveToS3(localLogFilename,
				mStorageFactory.addExtension(s3Path.getLogFilePath()));
	}

	private void uploadFiles(TopicPartition topicPartition) throws Exception {
		long committedOffsetCount = mOffsetTracker
				.getTrueCommittedOffsetCount(topicPartition);
		long lastSeenOffset = mOffsetTracker.getLastSeenOffset(topicPartition);
		final String lockPath = "/secor/locks/" + topicPartition.getTopic()
				+ "/" + topicPartition.getPartition();
		// Deleting writers closes their streams flushing all pending data to
		// the disk.
		mFileRegistry.deleteWriters(topicPartition);
		mZookeeperConnector.lock(lockPath);
		try {
			// Check if the committed offset has changed.
			long zookeeperComittedOffsetCount = mZookeeperConnector
					.getCommittedOffsetCount(topicPartition);
			if (zookeeperComittedOffsetCount == committedOffsetCount) {
				LOG.info("uploading topic " + topicPartition.getTopic()
						+ " partition " + topicPartition.getPartition());
				Collection<LogFilePath> paths = mFileRegistry
						.getPaths(topicPartition);
				for (LogFilePath path : paths) {
					upload(path);
				}
				mFileRegistry.deleteTopicPartition(topicPartition);
				mZookeeperConnector.setCommittedOffsetCount(topicPartition,
						lastSeenOffset + 1);
				mOffsetTracker.setCommittedOffsetCount(topicPartition,
						lastSeenOffset + 1);
			}
		} finally {
			mZookeeperConnector.unlock(lockPath);
		}
	}

	private void checkTopicPartition(TopicPartition topicPartition)
			throws Exception {
		final long size = mFileRegistry.getSize(topicPartition);
		final long modificationAgeSec = mFileRegistry
				.getModificationAgeSec(topicPartition);
		if (size >= mConfig.getMaxFileSizeBytes()
				|| modificationAgeSec >= mConfig.getMaxFileAgeSeconds()) {
			long newOffsetCount = mZookeeperConnector
					.getCommittedOffsetCount(topicPartition);
			long oldOffsetCount = mOffsetTracker.setCommittedOffsetCount(
					topicPartition, newOffsetCount);
			if (oldOffsetCount == newOffsetCount) {
				uploadFiles(topicPartition);
			} else {
				LOG.info(
						"There was a rebalancing event and someone committed a different offset that of the current consumer. "
								+ "Consumer base offset was: '{}'. The new one: '{}'. Deleting files in topic "
								+ topicPartition.getTopic()
								+ " partition "
								+ topicPartition.getPartition(),
						oldOffsetCount, newOffsetCount);
				mFileRegistry.deleteTopicPartition(topicPartition);
			}
		}
	}

	public void applyPolicy() throws Exception {
		Collection<TopicPartition> topicPartitions = mFileRegistry
				.getTopicPartitions();
		for (TopicPartition topicPartition : topicPartitions) {
			checkTopicPartition(topicPartition);
		}
	}
}
