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

import com.pinterest.secor.common.*;
import com.pinterest.secor.io.FileReader;
import com.pinterest.secor.io.FileWriter;
import com.pinterest.secor.io.KeyValue;
import com.pinterest.secor.monitoring.MetricCollector;
import com.pinterest.secor.util.FileUtil;
import com.pinterest.secor.util.IdUtil;

import junit.framework.TestCase;

import org.apache.hadoop.io.compress.CompressionCodec;
import org.joda.time.DateTime;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.internal.exceptions.ExceptionIncludingMockitoWarnings;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.util.HashSet;

/**
 * UploaderTest tests the log file uploader logic.
 *
 * @author Pawel Garbacki (pawel@pinterest.com)
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ FileUtil.class, IdUtil.class , DateTime.class})
public class UploaderTest extends TestCase {
    private static class TestUploader extends Uploader {
        private FileReader mReader;

        public TestUploader(SecorConfig config, OffsetTracker offsetTracker,
                            FileRegistry fileRegistry,
                            UploadManager uploadManager,
                            ZookeeperConnector zookeeperConnector) {
            init(config, offsetTracker, fileRegistry, uploadManager, zookeeperConnector, Mockito.mock(MetricCollector.class));
            mReader = Mockito.mock(FileReader.class);
        }

        @Override
        protected FileReader createReader(LogFilePath srcPath,
                CompressionCodec codec) throws IOException {
            return mReader;
        }

        public FileReader getReader() {
            return mReader;
        }
    }

    private TopicPartition mTopicPartition;

    private LogFilePath mLogFilePath;

    private SecorConfig mConfig;
    private OffsetTracker mOffsetTracker;
    private FileRegistry mFileRegistry;
    private ZookeeperConnector mZookeeperConnector;
    private UploadManager mUploadManager;

    private TestUploader mUploader;

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
        Mockito.when(mConfig.getZookeeperPath()).thenReturn("/");

        mOffsetTracker = Mockito.mock(OffsetTracker.class);

        mFileRegistry = Mockito.mock(FileRegistry.class);
        Mockito.when(mFileRegistry.getSize(mTopicPartition)).thenReturn(100L);
        HashSet<TopicPartition> topicPartitions = new HashSet<TopicPartition>();
        topicPartitions.add(mTopicPartition);
        Mockito.when(mFileRegistry.getTopicPartitions()).thenReturn(
                topicPartitions);

        mUploadManager = new HadoopS3UploadManager(mConfig);

        mZookeeperConnector = Mockito.mock(ZookeeperConnector.class);
        mUploader = new TestUploader(mConfig, mOffsetTracker, mFileRegistry, mUploadManager,
                mZookeeperConnector);
    }

    public void testUploadAtTime() throws Exception {
        final int minuteUploadMark = 1;

        PowerMockito.mockStatic(DateTime.class);
        PowerMockito.when(DateTime.now()).thenReturn(new DateTime(2016,7,27,0,minuteUploadMark,0));
        Mockito.when(mConfig.getUploadMinuteMark()).thenReturn(minuteUploadMark);
        Mockito.when(mConfig.getKafkaTopicFilter()).thenReturn("some_topic");

        Mockito.when(mConfig.getCloudService()).thenReturn("S3");
        Mockito.when(mConfig.getS3Bucket()).thenReturn("some_bucket");
        Mockito.when(mConfig.getS3Path()).thenReturn("some_s3_parent_dir");

        HashSet<LogFilePath> logFilePaths = new HashSet<LogFilePath>();
        logFilePaths.add(mLogFilePath);
        Mockito.when(mFileRegistry.getPaths(mTopicPartition)).thenReturn(
                logFilePaths);

        PowerMockito.mockStatic(FileUtil.class);
        Mockito.when(FileUtil.getPrefix("some_topic", mConfig)).
                thenReturn("s3a://some_bucket/some_s3_parent_dir");
        mUploader.applyPolicy();

        final String lockPath = "/secor/locks/some_topic/0";
        Mockito.verify(mZookeeperConnector).lock(lockPath);
        PowerMockito.verifyStatic();
        FileUtil.moveToCloud(
                "/some_parent_dir/some_topic/some_partition/some_other_partition/"
                        + "10_0_00000000000000000010",
                "s3a://some_bucket/some_s3_parent_dir/some_topic/some_partition/"
                        + "some_other_partition/10_0_00000000000000000010");
        Mockito.verify(mFileRegistry).deleteTopicPartition(mTopicPartition);
        Mockito.verify(mZookeeperConnector).setCommittedOffsetCount(
                mTopicPartition, 1L);
        Mockito.verify(mOffsetTracker).setCommittedOffsetCount(mTopicPartition,
                1L);
        Mockito.verify(mZookeeperConnector).unlock(lockPath);
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


        Mockito.when(mConfig.getCloudService()).thenReturn("S3");
        Mockito.when(mConfig.getS3Bucket()).thenReturn("some_bucket");
        Mockito.when(mConfig.getS3Path()).thenReturn("some_s3_parent_dir");

        HashSet<LogFilePath> logFilePaths = new HashSet<LogFilePath>();
        logFilePaths.add(mLogFilePath);
        Mockito.when(mFileRegistry.getPaths(mTopicPartition)).thenReturn(
                logFilePaths);

        PowerMockito.mockStatic(FileUtil.class);
        Mockito.when(FileUtil.getPrefix("some_topic", mConfig)).
                thenReturn("s3a://some_bucket/some_s3_parent_dir");
        mUploader.applyPolicy();

        final String lockPath = "/secor/locks/some_topic/0";
        Mockito.verify(mZookeeperConnector).lock(lockPath);
        PowerMockito.verifyStatic();
        FileUtil.moveToCloud(
                "/some_parent_dir/some_topic/some_partition/some_other_partition/"
                        + "10_0_00000000000000000010",
                "s3a://some_bucket/some_s3_parent_dir/some_topic/some_partition/"
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

    public void testTrimFiles() throws Exception {
        Mockito.when(
                mZookeeperConnector.getCommittedOffsetCount(mTopicPartition))
                .thenReturn(21L);
        Mockito.when(
                mOffsetTracker.setCommittedOffsetCount(mTopicPartition, 21L))
                .thenReturn(20L);
        Mockito.when(mOffsetTracker.getLastSeenOffset(mTopicPartition))
                .thenReturn(21L);

        HashSet<LogFilePath> logFilePaths = new HashSet<LogFilePath>();
        logFilePaths.add(mLogFilePath);
        Mockito.when(mFileRegistry.getPaths(mTopicPartition)).thenReturn(
                logFilePaths);

        FileReader reader = mUploader.getReader();

        Mockito.when(reader.next()).thenAnswer(new Answer<KeyValue>() {
            private int mCallCount = 0;

            @Override
            public KeyValue answer(InvocationOnMock invocation)
                    throws Throwable {
                if (mCallCount == 2) {
                    return null;
                }
                return new KeyValue(20 + mCallCount++, null);
            }
        });

        PowerMockito.mockStatic(IdUtil.class);
        Mockito.when(IdUtil.getLocalMessageDir())
                .thenReturn("some_message_dir");

        FileWriter writer = Mockito.mock(FileWriter.class);
        LogFilePath dstLogFilePath = new LogFilePath(
                "/some_parent_dir/some_message_dir",
                "/some_parent_dir/some_message_dir/some_topic/some_partition/"
                        + "some_other_partition/10_0_00000000000000000021");
        Mockito.when(mFileRegistry.getOrCreateWriter(dstLogFilePath, null))
                .thenReturn(writer);

        mUploader.applyPolicy();

        Mockito.verify(writer).write(Mockito.any(KeyValue.class));
        Mockito.verify(mFileRegistry).deletePath(mLogFilePath);
    }
}
