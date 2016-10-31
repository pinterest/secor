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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Topic partition group describes a kafka message topic-partitions pair.
 *
 * @author Henry Cai (hcai@pinterest.com)
 */
public class TopicPartitionGroup {
    private String mTopic;
    private int[] mPartitions;

    public TopicPartitionGroup(String topic, int[] partitions) {
        mTopic = topic;
        mPartitions = Arrays.copyOf(partitions, partitions.length);
    }

    public TopicPartitionGroup(TopicPartition tp) {
        this(tp.getTopic(), new int[]{tp.getPartition()});
    }

    public String getTopic() {
        return mTopic;
    }

    public int[] getPartitions() {
        return mPartitions;
    }

    public List<TopicPartition> getTopicPartitions() {
        List<TopicPartition> tps = new ArrayList<TopicPartition>();
        for (int p : mPartitions) {
            tps.add(new TopicPartition(mTopic, p));
        }
        return tps;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TopicPartitionGroup that = (TopicPartitionGroup) o;

        if (!Arrays.equals(mPartitions, that.mPartitions)) return false;
        if (mTopic != null ? !mTopic.equals(that.mTopic) : that.mTopic != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = mTopic != null ? mTopic.hashCode() : 0;
        result = 31 * result + Arrays.hashCode(mPartitions);
        return result;
    }

    @Override
    public String toString() {
        return "TopicPartitionGroup{" +
                "mTopic='" + mTopic + '\'' +
                ", mPartitions=" + Arrays.toString(mPartitions) +
                '}';
    }
}
