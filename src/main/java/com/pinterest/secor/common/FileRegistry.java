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
package com.pinterest.secor.common;

import com.pinterest.secor.io.FileWriter;
import com.pinterest.secor.util.FileUtil;
import com.pinterest.secor.util.ReflectionUtil;
import com.pinterest.secor.util.StatsUtil;

import org.apache.hadoop.io.compress.CompressionCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * FileRegistry keeps track of local log files currently being appended to and the associated
 * writers.
 *
 * @author Pawel Garbacki (pawel@pinterest.com)
 */
public class FileRegistry {
    private static final Logger LOG = LoggerFactory.getLogger(FileRegistry.class);

    private final SecorConfig mConfig;
    private HashMap<TopicPartition, HashSet<LogFilePath>> mFiles;
    private HashMap<LogFilePath, FileWriter> mWriters;
    private HashMap<LogFilePath, Long> mCreationTimes;

    public FileRegistry(SecorConfig mConfig) {
        this.mConfig = mConfig;
        mFiles = new HashMap<TopicPartition, HashSet<LogFilePath>>();
        mWriters = new HashMap<LogFilePath, FileWriter>();
        mCreationTimes = new HashMap<LogFilePath, Long>();
    }

    /**
     * Get all topic partitions.
     * @return Collection of all registered topic partitions.
     */
    public Collection<TopicPartition> getTopicPartitions() {
        Set<TopicPartition> topicPartitions = mFiles.keySet();
        if (topicPartitions == null) {
            return new HashSet<TopicPartition>();
        }
        // Return a copy of the collection to prevent the caller from modifying internals.
        return new HashSet<TopicPartition>(topicPartitions);
    }

    /**
     * Get paths in a given topic partition.
     * @param topicPartition The topic partition to retrieve paths for.
     * @return Collection of file paths in the given topic partition.
     */
    public Collection<LogFilePath> getPaths(TopicPartition topicPartition) {
        HashSet<LogFilePath> logFilePaths = mFiles.get(topicPartition);
        if (logFilePaths == null) {
            return new HashSet<LogFilePath>();
        }
        return new HashSet<LogFilePath>(logFilePaths);
    }

    /**
     * Retrieve a writer for a given path or create a new one if it does not exist.
     * @param path The path to retrieve writer for.
     * @param codec Optional compression codec.
     * @return Writer for a given path.
     * @throws Exception 
     */
    public FileWriter getOrCreateWriter(LogFilePath path, CompressionCodec codec)
            throws Exception {
        FileWriter writer = mWriters.get(path);
        if (writer == null) {
            // Just in case.
            FileUtil.delete(path.getLogFilePath());
            FileUtil.delete(path.getLogFileCrcPath());
            TopicPartition topicPartition = new TopicPartition(path.getTopic(),
                    path.getKafkaPartition());
            HashSet<LogFilePath> files = mFiles.get(topicPartition);
            if (files == null) {
                files = new HashSet<LogFilePath>();
                mFiles.put(topicPartition, files);
            }
            if (!files.contains(path)) {
                files.add(path);
            }
            writer = ReflectionUtil.createFileWriter(mConfig.getFileReaderWriterFactory(), path, codec);
            mWriters.put(path, writer);
            mCreationTimes.put(path, System.currentTimeMillis() / 1000L);
            LOG.debug("created writer for path " + path.getLogFilePath());
        }
        return writer;
    }

    /**
     * Delete a given path, the underlying file, and the corresponding writer.
     * @param path The path to delete.
     * @throws IOException
     */
    public void deletePath(LogFilePath path) throws IOException {
        TopicPartition topicPartition = new TopicPartition(path.getTopic(),
                                                           path.getKafkaPartition());
        HashSet<LogFilePath> paths = mFiles.get(topicPartition);
        paths.remove(path);
        if (paths.isEmpty()) {
            mFiles.remove(topicPartition);
            StatsUtil.clearLabel("secor.size." + topicPartition.getTopic() + "." +
                                 topicPartition.getPartition());
            StatsUtil.clearLabel("secor.modification_age_sec." + topicPartition.getTopic() + "." +
                                 topicPartition.getPartition());
        }
        deleteWriter(path);
        FileUtil.delete(path.getLogFilePath());
        FileUtil.delete(path.getLogFileCrcPath());
    }

    /**
     * Delete all paths, files, and writers in a given topic partition.
     * @param topicPartition The topic partition to remove.
     * @throws IOException
     */
    public void deleteTopicPartition(TopicPartition topicPartition) throws IOException {
        HashSet<LogFilePath> paths = mFiles.get(topicPartition);
        if (paths == null) {
            return;
        }
        HashSet<LogFilePath> clonedPaths = (HashSet<LogFilePath>) paths.clone();
        for (LogFilePath path : clonedPaths) {
            deletePath(path);
        }
    }

    /**
     * Delete writer for a given topic partition.  Underlying file is not removed.
     * @param path The path to remove the writer for.
     */
    public void deleteWriter(LogFilePath path) throws IOException {
        FileWriter writer = mWriters.get(path);
        if (writer == null) {
            LOG.warn("No writer found for path " + path.getLogFilePath());
        } else {
            LOG.info("Deleting writer for path " + path.getLogFilePath());
            writer.close();
            mWriters.remove(path);
            mCreationTimes.remove(path);
        }
    }

    /**
     * Delete all writers in a given topic partition.  Underlying files are not removed.
     * @param topicPartition The topic partition to remove the writers for.
     */
    public void deleteWriters(TopicPartition topicPartition) throws IOException {
        HashSet<LogFilePath> paths = mFiles.get(topicPartition);
        if (paths == null) {
            LOG.warn("No paths found for topic " + topicPartition.getTopic() + " partition " +
                     topicPartition.getPartition());
        } else {
            for (LogFilePath path : paths) {
                deleteWriter(path);
            }
        }
    }

    /**
     * Get aggregated size of all files in a given topic partition.
     * @param topicPartition The topic partition to get the size for.
     * @return Aggregated size of files in the topic partition or 0 if the topic partition does
     *     not contain any files.
     * @throws IOException
     */
    public long getSize(TopicPartition topicPartition) throws IOException {
        Collection<LogFilePath> paths = getPaths(topicPartition);
        long result = 0;
        for (LogFilePath path : paths) {
            FileWriter writer = mWriters.get(path);
            if (writer != null) {
                result += writer.getLength();
            }
        }
        StatsUtil.setLabel("secor.size." + topicPartition.getTopic() + "." +
                           topicPartition.getPartition(), Long.toString(result));
        return result;
    }

    /**
     * Get the creation age of the most recently created file in a given topic partition.
     * @param topicPartition The topic partition to get the age of.
     * @return Age of the most recently created file in the topic partition or -1 if the partition
     *     does not contain any files.
     * @throws IOException
     */
    public long getModificationAgeSec(TopicPartition topicPartition) throws IOException {
        long now = System.currentTimeMillis() / 1000L;
        long result = Long.MAX_VALUE;
        Collection<LogFilePath> paths = getPaths(topicPartition);
        for (LogFilePath path : paths) {
            Long creationTime = mCreationTimes.get(path);
            if (creationTime == null) {
                LOG.warn("no creation time found for path " + path);
                creationTime = now;
            }
            long age = now - creationTime;
            if (age < result) {
                result = age;
            }
        }
        if (result == Long.MAX_VALUE) {
            result = -1;
        }
        StatsUtil.setLabel("secor.modification_age_sec." + topicPartition.getTopic() + "." +
            topicPartition.getPartition(), Long.toString(result));
        return result;
    }
}
