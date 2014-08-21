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

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.zookeeper.DistributedLock;
import com.twitter.common.zookeeper.DistributedLockImpl;
import com.twitter.common.zookeeper.ZooKeeperClient;
import org.apache.commons.lang.StringUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

/**
 * ZookeeperConnector implements interactions with Zookeeper.
 *
 * @author Pawel Garbacki (pawel@pinterest.com)
 */
public class ZookeeperConnector {
    private static final Logger LOG = LoggerFactory.getLogger(ZookeeperConnector.class);

    private SecorConfig mConfig;
    private ZooKeeperClient mZookeeperClient;
    private HashMap<String, DistributedLock> mLocks;
    private String mCommittedOffsetGroupPath;

    protected ZookeeperConnector() {
    }

    public ZookeeperConnector(SecorConfig config) {
        mConfig = config;
        mZookeeperClient = new ZooKeeperClient(Amount.of(1, Time.DAYS), getZookeeperAddresses());
        mLocks = new HashMap<String, DistributedLock>();
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
            result.add(new InetSocketAddress(host, port));
        }
        return result;
    }

    public void lock(String lockPath) {
        assert mLocks.get(lockPath) == null: "mLocks.get(" + lockPath + ") == null";
        DistributedLock distributedLock = new DistributedLockImpl(mZookeeperClient, lockPath);
        mLocks.put(lockPath, distributedLock);
        distributedLock.lock();
    }

    public void unlock(String lockPath) {
        DistributedLock distributedLock = mLocks.get(lockPath);
        assert distributedLock != null: "mLocks.get(" + lockPath + ") != null";
        distributedLock.unlock();
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
        ZooKeeper zookeeper = mZookeeperClient.get();
        String offsetPath = getCommittedOffsetPartitionPath(topicPartition);
        try {
            byte[] data = zookeeper.getData(offsetPath, false, null);
            return Long.parseLong(new String(data));
        } catch (KeeperException.NoNodeException exception) {
            LOG.warn("path " + offsetPath + " does not exist in zookeeper");
            return -1;
        }
    }

    public List<Integer> getCommittedOffsetPartitions(String topic) throws Exception {
        ZooKeeper zookeeper = mZookeeperClient.get();
        String topicPath = getCommittedOffsetTopicPath(topic);
        List<String> partitions = zookeeper.getChildren(topicPath, false);
        LinkedList<Integer> result = new LinkedList<Integer>();
        for (String partitionPath : partitions) {
            String[] elements = partitionPath.split("/");
            String partition = elements[elements.length - 1];
            result.add(Integer.valueOf(partition));
        }
        return result;
    }

    public List<String> getCommittedOffsetTopics() throws Exception {
        ZooKeeper zookeeper = mZookeeperClient.get();
        String offsetPath = getCommittedOffsetGroupPath();
        List<String> topics = zookeeper.getChildren(offsetPath, false);
        LinkedList<String> result = new LinkedList<String>();
        for (String topicPath : topics) {
            String[] elements = topicPath.split("/");
            String topic = elements[elements.length - 1];
            result.add(topic);
        }
        return result;
    }

    private void createMissingParents(String path) throws Exception {
        ZooKeeper zookeeper = mZookeeperClient.get();
        assert path.charAt(0) == '/': path + ".charAt(0) == '/'";
        String[] elements = path.split("/");
        String prefix = "";
        for (int i = 1; i < elements.length - 1; ++i) {
            prefix += "/" + elements[i];
            try {
                zookeeper.create(prefix, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                LOG.info("created path " + prefix);
            } catch (KeeperException.NodeExistsException exception) {
            }
        }
    }

    public void setCommittedOffsetCount(TopicPartition topicPartition, long count)
            throws Exception {
        ZooKeeper zookeeper = mZookeeperClient.get();
        String offsetPath = getCommittedOffsetPartitionPath(topicPartition);
        LOG.info("creating missing parents for zookeeper path " + offsetPath);
        createMissingParents(offsetPath);
        byte[] data = Long.toString(count).getBytes();
        try {
            LOG.info("setting zookeeper path " + offsetPath + " value " + count);
            // -1 matches any version
            zookeeper.setData(offsetPath, data, -1);
        } catch (KeeperException.NoNodeException exception) {
            zookeeper.create(offsetPath, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
    }

    public void deleteCommittedOffsetTopicCount(String topic) throws Exception {
        ZooKeeper zookeeper = mZookeeperClient.get();
        List<Integer> partitions = getCommittedOffsetPartitions(topic);
        for (Integer partition : partitions) {
            TopicPartition topicPartition = new TopicPartition(topic, partition);
            String offsetPath = getCommittedOffsetPartitionPath(topicPartition);
            LOG.info("deleting path " + offsetPath);
            zookeeper.delete(offsetPath, -1);
        }
    }

    public void deleteCommittedOffsetPartitionCount(TopicPartition topicPartition)
            throws Exception {
        String offsetPath = getCommittedOffsetPartitionPath(topicPartition);
        ZooKeeper zookeeper = mZookeeperClient.get();
        LOG.info("deleting path " + offsetPath);
        zookeeper.delete(offsetPath, -1);
    }

    protected void setConfig(SecorConfig config) {
        this.mConfig = config;
    }
}
