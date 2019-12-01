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
package com.pinterest.secor.common;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import org.apache.commons.lang.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

/**
 * ZookeeperConnector implements interactions with Zookeeper.
 *
 * @author Pawel Garbacki (pawel@pinterest.com)
 */
public class ZookeeperConnector implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(ZookeeperConnector.class);

    private SecorConfig mConfig;
    private CuratorFramework mCurator;
    private HashMap<String, InterProcessMutex> mLocks;
    private String mCommittedOffsetGroupPath;

    protected ZookeeperConnector() {
    }

    public ZookeeperConnector(SecorConfig config) {
        mConfig = config;
        mCurator = CuratorFrameworkFactory.newClient(mConfig.getZookeeperQuorum(),
            new ExponentialBackoffRetry(1000, 3));
        mCurator.start();
        try {
            mCurator.blockUntilConnected();
        } catch (InterruptedException ex) {
            throw new RuntimeException("Interrupted while waiting for ZK", ex);
        }

        mLocks = new HashMap<String, InterProcessMutex>();
    }

    @Override
    public void close() throws IOException  {
        if (mCurator != null) {
            mCurator.close();
        }
    }

    private Iterable<InetSocketAddress> getZookeeperAddresses() {
        String zookeeperQuorum = mConfig.getZookeeperQuorum();
        String[] hostports = zookeeperQuorum.split(",");
        LinkedList<InetSocketAddress> result = new LinkedList<InetSocketAddress>();
        for (String hostport : hostports) {
            String[] elements = hostport.split(":");
            assert elements.length == 2: Integer.toString(elements.length) + " == 2";
            String host = elements[0];
            int port = Integer.parseInt(elements[1]);
            result.add(InetSocketAddress.createUnresolved(host, port));
        }
        return result;
    }

    public void lock(String lockPath) {
        assert mLocks.get(lockPath) == null: "mLocks.get(" + lockPath + ") == null";
        InterProcessMutex distributedLock = new InterProcessMutex(mCurator, lockPath);
        mLocks.put(lockPath, distributedLock);
        try {
            distributedLock.acquire();
        } catch (Exception ex) {
            throw new RuntimeException("Unexpected ZK error", ex);
        }
    }

    public void unlock(String lockPath) {
        InterProcessMutex distributedLock = mLocks.get(lockPath);
        assert distributedLock != null: "mLocks.get(" + lockPath + ") != null";
        try {
            distributedLock.release();
        } catch (Exception ex) {
            throw new RuntimeException("Unexpected ZK error", ex);
        }
        mLocks.remove(lockPath);
    }

    protected String getCommittedOffsetGroupPath() {
        if (Strings.isNullOrEmpty(mCommittedOffsetGroupPath)) {
            String stripped = StringUtils.strip(mConfig.getKafkaZookeeperPath(), "/");
            mCommittedOffsetGroupPath = Joiner.on("/").skipNulls().join(
                    "",
                    stripped.equals("") ? null : stripped,
                    "consumers",
                    mConfig.getKafkaGroup(),
                    "offsets"
            );
        }
        return mCommittedOffsetGroupPath;
    }

    private String getCommittedOffsetTopicPath(String topic) {
        return getCommittedOffsetGroupPath() + "/" + topic;
    }

    private String getCommittedOffsetPartitionPath(TopicPartition topicPartition) {
        return getCommittedOffsetTopicPath(topicPartition.getTopic()) + "/" +
            topicPartition.getPartition();
    }

    public long getCommittedOffsetCount(TopicPartition topicPartition) throws Exception {
        String offsetPath = getCommittedOffsetPartitionPath(topicPartition);
        try {
            byte[] data = mCurator.getData().forPath(offsetPath);
            return Long.parseLong(new String(data));
        } catch (KeeperException.NoNodeException exception) {
            LOG.warn("path {} does not exist in zookeeper", offsetPath);
            return -1;
        }
    }

    public List<Integer> getCommittedOffsetPartitions(String topic) throws Exception {
        String topicPath = getCommittedOffsetTopicPath(topic);
        List<String> partitions = mCurator.getChildren().forPath(topicPath);
        LinkedList<Integer> result = new LinkedList<Integer>();
        for (String partitionPath : partitions) {
            String[] elements = partitionPath.split("/");
            String partition = elements[elements.length - 1];
            result.add(Integer.valueOf(partition));
        }
        return result;
    }

    public List<String> getCommittedOffsetTopics() throws Exception {
        String offsetPath = getCommittedOffsetGroupPath();
        List<String> topics = mCurator.getChildren().forPath(offsetPath);
        LinkedList<String> result = new LinkedList<String>();
        for (String topicPath : topics) {
            String[] elements = topicPath.split("/");
            String topic = elements[elements.length - 1];
            result.add(topic);
        }
        return result;
    }

    private void createMissingParents(String path) throws Exception {
      Stat stat = mCurator.checkExists().forPath(path);
      if (stat == null) {
        mCurator.create()
            .creatingParentsIfNeeded()
            .withMode(CreateMode.PERSISTENT)
            .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
            .forPath(path);
      }
    }

    public void setCommittedOffsetCount(TopicPartition topicPartition, long count)
            throws Exception {
        String offsetPath = getCommittedOffsetPartitionPath(topicPartition);
        LOG.info("creating missing parents for zookeeper path {}", offsetPath);
        createMissingParents(offsetPath);
        byte[] data = Long.toString(count).getBytes();
        try {
            LOG.info("setting zookeeper path {} value {}", offsetPath, count);
            // -1 matches any version
            mCurator.setData().forPath(offsetPath, data);
        } catch (KeeperException.NoNodeException exception) {
            LOG.warn("Failed to set value to path " + offsetPath, exception);
        }
    }

    public void deleteCommittedOffsetTopicCount(String topic) throws Exception {
        List<Integer> partitions = getCommittedOffsetPartitions(topic);
        for (Integer partition : partitions) {
            TopicPartition topicPartition = new TopicPartition(topic, partition);
            String offsetPath = getCommittedOffsetPartitionPath(topicPartition);
            LOG.info("deleting path {}", offsetPath);
            mCurator.delete().forPath(offsetPath);
        }
    }

    public void deleteCommittedOffsetPartitionCount(TopicPartition topicPartition)
            throws Exception {
        String offsetPath = getCommittedOffsetPartitionPath(topicPartition);
        LOG.info("deleting path {}", offsetPath);
        mCurator.delete().forPath(offsetPath);
    }

    protected void setConfig(SecorConfig config) {
        this.mConfig = config;
    }
}
