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
package com.pinterest.secor.writer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pinterest.secor.common.FileRegistry;
import com.pinterest.secor.common.LogFilePath;
import com.pinterest.secor.common.OffsetTracker;
import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.common.TopicPartition;
import com.pinterest.secor.common.ZookeeperConnector;
import com.pinterest.secor.message.ParsedMessage;
import com.pinterest.secor.storage.StorageFactory;
import com.pinterest.secor.storage.Writer;
import com.pinterest.secor.util.IdUtil;
import com.pinterest.secor.util.StatsUtil;

/**
 * Message writer appends Kafka messages to local log files.
 * 
 * @author Pawel Garbacki (pawel@pinterest.com)
 */
public class MessageWriter {
	private static final Logger LOG = LoggerFactory
			.getLogger(MessageWriter.class);

	private final SecorConfig mConfig;
	private final OffsetTracker mOffsetTracker;
	private final FileRegistry mFileRegistry;
	private final ZookeeperConnector mZookeeperConnector;

	private final StorageFactory mStorageFactory;

	public MessageWriter(SecorConfig config, OffsetTracker offsetTracker,
			FileRegistry fileRegistry, StorageFactory storageFactory) {
		mConfig = config;
		mOffsetTracker = offsetTracker;
		mFileRegistry = fileRegistry;
		mStorageFactory = storageFactory;
		mZookeeperConnector = new ZookeeperConnector(config);
	}

	private void adjustOffset(ParsedMessage message) throws Exception {
		TopicPartition topicPartition = new TopicPartition(message.getTopic(),
				message.getKafkaPartition());
		long lastSeenOffset = mOffsetTracker.getLastSeenOffset(topicPartition);
		if (message.getOffset() != lastSeenOffset + 1) {
			// There was a rebalancing event since we read the last message.
			LOG.info("offset of message " + message
					+ " does not follow sequentially the last seen offset "
					+ lastSeenOffset + ".  Deleting files in topic "
					+ topicPartition.getTopic() + " partition "
					+ topicPartition.getPartition());

			mFileRegistry.deleteTopicPartition(topicPartition);

			// the committed offset has changed so we have to update tracker.
			long zookeeperComittedOffsetCount = mZookeeperConnector
					.getCommittedOffsetCount(topicPartition);
			mOffsetTracker.setCommittedOffsetCount(topicPartition,
					zookeeperComittedOffsetCount);
		}
		mOffsetTracker.setLastSeenOffset(topicPartition, message.getOffset());
	}

	public void write(ParsedMessage message) throws Exception {
		adjustOffset(message);
		TopicPartition topicPartition = new TopicPartition(message.getTopic(),
				message.getKafkaPartition());
		long offset = mOffsetTracker
				.getAdjustedCommittedOffsetCount(topicPartition);
		String localPrefix = mConfig.getLocalPath() + '/'
				+ IdUtil.getLocalMessageDir();
		LogFilePath path = new LogFilePath(localPrefix,
				mConfig.getGeneration(), offset, message);

		Writer writer = mFileRegistry.getOrCreateWriter(mStorageFactory, path);
		writer.append(message);

		StatsUtil.setLabel(topicPartition, "writer_last_offset",
				String.valueOf(message.getOffset()));

		LOG.trace("appended message " + message + " to file "
				+ path.getLogFilePath() + ".  File length "
				+ writer.getLength());
	}
}
