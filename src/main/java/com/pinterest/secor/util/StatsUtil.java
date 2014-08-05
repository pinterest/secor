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
package com.pinterest.secor.util;

import com.pinterest.secor.common.TopicPartition;
import com.twitter.ostrich.stats.Stats;

/**
 * Utilities to interact with Ostrich stats exporter.
 * 
 * @author Pawel Garbacki (pawel@pinterest.com)
 */
public class StatsUtil {
	public static void setLabel(String name, String value) {
		long threadId = Thread.currentThread().getId();
		String fqName = "secor_t" + String.valueOf(threadId) + "_" + name;
		Stats.setLabel(fqName, value);
	}

	public static void clearLabel(TopicPartition topicPartition, String name) {
		String underscoredName = prepareName(topicPartition, name);
		Stats.clearLabel(underscoredName);
	}

	private static String prepareName(TopicPartition topicPartition, String name) {
		long threadId = Thread.currentThread().getId();
		String fqName = "secor_t" + String.valueOf(threadId) + "_"
				+ topicPartition.getTopic() + "_"
				+ String.valueOf(topicPartition.getPartition()) + "_" + name;
		return fqName;
	}

	public static void incCounter(TopicPartition topicPartition, String name) {
		String fqName = prepareName(topicPartition, name);
		Stats.incr(fqName);
	}

	public static void clearCounter(TopicPartition topicPartition, String name) {
		String fqName = prepareName(topicPartition, name);
		Stats.removeCounter(fqName);
	}

	public static void setLabel(TopicPartition topicPartition, String name,
			String value) {
		String fqName = prepareName(topicPartition, name);
		Stats.setLabel(fqName, value);
	}

	public static void setRawLabel(String confName, String value) {
		Stats.setLabel(confName, value);
	}

	public static void clearStats(TopicPartition topicPartition) {
		StatsUtil.clearLabel(topicPartition, "most_recently_created_file_sec");
		StatsUtil.clearLabel(topicPartition, "aggregated_size_bytes");
		StatsUtil.clearLabel(topicPartition, "committed_offset");
		StatsUtil.clearLabel(topicPartition, "reader_last_offset");
		StatsUtil.clearLabel(topicPartition, "writer_last_offset");
		StatsUtil.clearCounter(topicPartition, "s3_uploads");
	}
}
