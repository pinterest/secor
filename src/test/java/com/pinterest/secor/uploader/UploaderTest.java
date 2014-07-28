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

import java.util.HashSet;

import junit.framework.TestCase;

import org.apache.hadoop.fs.FileSystem;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.pinterest.secor.common.FileRegistry;
import com.pinterest.secor.common.LogFilePath;
import com.pinterest.secor.common.OffsetTracker;
import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.common.TopicPartition;
import com.pinterest.secor.common.ZookeeperConnector;
import com.pinterest.secor.storage.StorageFactory;
import com.pinterest.secor.util.FileUtil;
import com.pinterest.secor.util.IdUtil;

/**
 * UploaderTest tests the log file uploader logic.
 * 
 * @author Pawel Garbacki (pawel@pinterest.com)
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ FileUtil.class, FileSystem.class, IdUtil.class })
public class UploaderTest extends TestCase {

	private TopicPartition mTopicPartition;

	private LogFilePath mLogFilePath;

	private SecorConfig mConfig;
	private OffsetTracker mOffsetTracker;
	private FileRegistry mFileRegistry;
	private ZookeeperConnector mZookeeperConnector;
	private StorageFactory storageFactory;

	private Uploader mUploader;

	@Override
	public void setUp() throws Exception {
		super.setUp();
		mTopicPartition = new TopicPartition("some_topic", 0);

		mLogFilePath = new LogFilePath("/some_parent_dir",
				"/some_parent_dir/some_topic/some_partition/some_other_partition/"
						+ "10_0_00000000000000000010");

		mConfig = Mockito.mock(SecorConfig.class);
		Mockito.when(mConfig.getLocalPath()).thenReturn("/some_parent_dir");
		Mockito.when(mConfig.getMaxFileSizeBytes()).thenReturn(10L);

		mOffsetTracker = Mockito.mock(OffsetTracker.class);

		mFileRegistry = Mockito.mock(FileRegistry.class);
		Mockito.when(mFileRegistry.getSize(mTopicPartition)).thenReturn(100L);
		HashSet<TopicPartition> topicPartitions = new HashSet<TopicPartition>();
		topicPartitions.add(mTopicPartition);
		Mockito.when(mFileRegistry.getTopicPartitions()).thenReturn(
				topicPartitions);

		mZookeeperConnector = Mockito.mock(ZookeeperConnector.class);

		storageFactory = Mockito.mock(StorageFactory.class);

		Mockito.when(storageFactory.addExtension(Mockito.any(String.class)))
				.thenAnswer(new Answer<String>() {

					@Override
					public String answer(InvocationOnMock invocation)
							throws Throwable {
						return (String) invocation.getArguments()[0];
					}
				});

		mUploader = new Uploader(mConfig, mOffsetTracker, mFileRegistry,
				mZookeeperConnector, storageFactory);
	}

	public void testUploadFiles() throws Exception {
		Mockito.when(
				mZookeeperConnector.getCommittedOffsetCount(mTopicPartition))
				.thenReturn(11L);
		Mockito.when(
				mOffsetTracker.setCommittedOffsetCount(mTopicPartition, 11L))
				.thenReturn(11L);
		Mockito.when(
				mOffsetTracker.setCommittedOffsetCount(mTopicPartition, 21L))
				.thenReturn(11L);
		Mockito.when(mOffsetTracker.getLastSeenOffset(mTopicPartition))
				.thenReturn(20L);
		Mockito.when(
				mOffsetTracker.getTrueCommittedOffsetCount(mTopicPartition))
				.thenReturn(11L);
		Mockito.when(mConfig.getS3Bucket()).thenReturn("some_bucket");
		Mockito.when(mConfig.getS3Path()).thenReturn("some_s3_parent_dir");

		HashSet<LogFilePath> logFilePaths = new HashSet<LogFilePath>();
		logFilePaths.add(mLogFilePath);
		Mockito.when(mFileRegistry.getPaths(mTopicPartition)).thenReturn(
				logFilePaths);

		PowerMockito.mockStatic(FileUtil.class);

		mUploader.applyPolicy();

		final String lockPath = "/secor/locks/some_topic/0";
		Mockito.verify(mZookeeperConnector).lock(lockPath);
		PowerMockito.verifyStatic();
		FileUtil.moveToS3(
				"/some_parent_dir/some_topic/some_partition/some_other_partition/"
						+ "10_0_00000000000000000010",
				"s3n://some_bucket/some_s3_parent_dir/some_topic/some_partition/"
						+ "some_other_partition/10_0_00000000000000000010");
		Mockito.verify(mFileRegistry).deleteTopicPartition(mTopicPartition);
		Mockito.verify(mZookeeperConnector).setCommittedOffsetCount(
				mTopicPartition, 21L);
		Mockito.verify(mOffsetTracker).setCommittedOffsetCount(mTopicPartition,
				21L);
		Mockito.verify(mZookeeperConnector).unlock(lockPath);
	}

	public void testDeleteTopicPartition() throws Exception {
		Mockito.when(
				mZookeeperConnector.getCommittedOffsetCount(mTopicPartition))
				.thenReturn(31L);
		Mockito.when(
				mOffsetTracker.setCommittedOffsetCount(mTopicPartition, 30L))
				.thenReturn(11L);
		Mockito.when(mOffsetTracker.getLastSeenOffset(mTopicPartition))
				.thenReturn(20L);

		mUploader.applyPolicy();

		Mockito.verify(mFileRegistry).deleteTopicPartition(mTopicPartition);
	}
}
