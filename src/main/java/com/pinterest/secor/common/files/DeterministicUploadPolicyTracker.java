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

package com.pinterest.secor.common.files;

import com.pinterest.secor.common.kafka.TopicPartition;
import com.pinterest.secor.io.KeyValue;
import com.pinterest.secor.message.Message;

import java.util.HashMap;
import java.util.Map;

/**
 * DeterministicUploadPolicyTracker stores the range of timestamps seen so far for a given TopicPartition.
 * It lets us implement a "time-based" upload policy that is still deterministic.
 */
public class DeterministicUploadPolicyTracker
{
  private final long mMaxFileTimestampRangeMillis;
  private final long mMaxInputPayloadSizeBytes;

  private static class TopicPartitionStats {
    long earliestTimestamp;
    long latestTimestamp;
    long totalInputPayloadSizeBytes;
    TopicPartitionStats(long earliestTimestamp, long latestTimestamp, long totalInputPayloadSizeBytes) {
      this.earliestTimestamp = earliestTimestamp;
      this.latestTimestamp = latestTimestamp;
      this.totalInputPayloadSizeBytes = totalInputPayloadSizeBytes;
    }
  }

  private Map<TopicPartition, TopicPartitionStats> mStats;

  public DeterministicUploadPolicyTracker(long maxFileTimestampRangeMillis, long maxInputPayloadSizeBytes)
  {
    if (maxFileTimestampRangeMillis > 0) {
      mMaxFileTimestampRangeMillis = maxFileTimestampRangeMillis;
    } else {
      mMaxFileTimestampRangeMillis = Long.MAX_VALUE;
    }
    if (maxInputPayloadSizeBytes > 0) {
      mMaxInputPayloadSizeBytes = maxInputPayloadSizeBytes;
    } else {
      mMaxInputPayloadSizeBytes = Long.MAX_VALUE;
    }
    if (mMaxFileTimestampRangeMillis == Long.MAX_VALUE && mMaxInputPayloadSizeBytes == Long.MAX_VALUE) {
      throw new RuntimeException("When secor.upload.deterministic is true, you must set either " +
                                 "secor.max.file.timestamp.range.millis or secor.max.input.payload.size.bytes");
    }
    mStats = new HashMap<>();
  }

  public void track(Message message) {
    track(new TopicPartition(message.getTopic(), message.getKafkaPartition()),
          message.getTimestamp(),
          message.getPayload().length);
  }

  public void track(TopicPartition topicPartition, KeyValue kv) {
    track(topicPartition, kv.getTimestamp(), kv.getValue().length);
  }

  private void track(TopicPartition topicPartition, long timestamp, long inputPayloadSizeBytes) {
    if (timestamp <= 0 && mMaxFileTimestampRangeMillis != Long.MAX_VALUE) {
      throw new RuntimeException("Message without timestamp incompatible with secor.max.file.timestamp.range.millis");
    }
    final TopicPartitionStats stats = mStats.get(topicPartition);
    if (stats == null) {
      mStats.put(topicPartition, new TopicPartitionStats(timestamp, timestamp, inputPayloadSizeBytes));
    } else {
      stats.earliestTimestamp = Math.min(stats.earliestTimestamp, timestamp);
      stats.latestTimestamp = Math.max(stats.latestTimestamp, timestamp);
      stats.totalInputPayloadSizeBytes += inputPayloadSizeBytes;
    }

  }

  public void reset(TopicPartition topicPartition)
  {
    mStats.remove(topicPartition);
  }

  public boolean shouldUpload(TopicPartition topicPartition) {
    final TopicPartitionStats stats = mStats.get(topicPartition);
    if (stats == null) {
      // No messages at all: definitely not ready for upload.
      return false;
    }
    return stats.totalInputPayloadSizeBytes >= mMaxInputPayloadSizeBytes
        || (stats.latestTimestamp - stats.earliestTimestamp) >= mMaxFileTimestampRangeMillis;
  }
}
