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

import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Offset tracker stores offset related metadata.
 * 
 * @author Pawel Garbacki (pawel@pinterest.com)
 */
public class OffsetTracker {
	public static final int LAST_SEEN_OFFSET_UNINITIALIZED = -2;

	public static final long COMMITTED_OFFSET_COUNT_UNINITIALIZED = -1L;

	private static final Logger LOG = LoggerFactory
			.getLogger(OffsetTracker.class);

	private HashMap<TopicPartition, Long> mLastSeenOffset;
	private HashMap<TopicPartition, Long> mFirstSeendOffset;
	private HashMap<TopicPartition, Long> mCommittedOffsetCount;

	public OffsetTracker() {
		mLastSeenOffset = new HashMap<TopicPartition, Long>();
		mCommittedOffsetCount = new HashMap<TopicPartition, Long>();
		mFirstSeendOffset = new HashMap<TopicPartition, Long>();
	}

	public long getLastSeenOffset(TopicPartition topicPartition) {
		Long offset = mLastSeenOffset.get(topicPartition);
		if (offset == null) {
			return LAST_SEEN_OFFSET_UNINITIALIZED;
		}
		return offset.longValue();
	}

	public long setLastSeenOffset(TopicPartition topicPartition, long offset) {
		long lastSeenOffset = getLastSeenOffset(topicPartition);
		mLastSeenOffset.put(topicPartition, offset);

		if (mFirstSeendOffset.get(topicPartition) == null) {
			mFirstSeendOffset.put(topicPartition, offset);
		}

		if (lastSeenOffset + 1 != offset) {
			LOG.info("offset for topic " + topicPartition.getTopic()
					+ " partition " + topicPartition.getPartition()
					+ " changed from " + lastSeenOffset + " to " + offset);

			mFirstSeendOffset.put(topicPartition, offset);
		}

		return lastSeenOffset;
	}

	public long getTrueCommittedOffsetCount(TopicPartition topicPartition) {
		Long committedOffsetCount = mCommittedOffsetCount.get(topicPartition);
		if (committedOffsetCount == null) {
			return COMMITTED_OFFSET_COUNT_UNINITIALIZED;
		}
		return committedOffsetCount.longValue();
	}

	public long getAdjustedCommittedOffsetCount(TopicPartition topicPartition) {
		long trueCommittedOffsetCount = getTrueCommittedOffsetCount(topicPartition);
		if (trueCommittedOffsetCount == COMMITTED_OFFSET_COUNT_UNINITIALIZED) {
			Long firstSeenOffset = mFirstSeendOffset.get(topicPartition);
			if (firstSeenOffset != null) {
				return firstSeenOffset.longValue();
			}
		}
		return trueCommittedOffsetCount;
	}

	public long setCommittedOffsetCount(TopicPartition topicPartition,
			long count) {
		long trueCommittedOffsetCount = getTrueCommittedOffsetCount(topicPartition);

		// Committed offsets should never go back.
		assert trueCommittedOffsetCount <= count : Long
				.toString(trueCommittedOffsetCount) + " <= " + count;

		mCommittedOffsetCount.put(topicPartition, count);
		return trueCommittedOffsetCount;
	}
}
