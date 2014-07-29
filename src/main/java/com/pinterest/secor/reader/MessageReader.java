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
package com.pinterest.secor.reader;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.consumer.TopicFilter;
import kafka.consumer.Whitelist;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pinterest.secor.common.OffsetTracker;
import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.common.TopicPartition;
import com.pinterest.secor.message.Message;
import com.pinterest.secor.util.IdUtil;
import com.pinterest.secor.util.RateLimitUtil;
import com.pinterest.secor.util.StatsUtil;

/**
 * Message reader consumer raw Kafka messages.
 * 
 * @author Pawel Garbacki (pawel@pinterest.com)
 */
public class MessageReader {
	private static final Logger LOG = LoggerFactory
			.getLogger(MessageReader.class);

	private SecorConfig mConfig;
	private ConsumerConnector mConsumerConnector;
	private ConsumerIterator mIterator;
	private HashMap<TopicPartition, Long> mLastAccessTime;

	public MessageReader(SecorConfig config, OffsetTracker offsetTracker)
			throws UnknownHostException {
		mConfig = config;

		mConsumerConnector = Consumer
				.createJavaConsumerConnector(createConsumerConfig());

		TopicFilter topicFilter = new Whitelist(mConfig.getKafkaTopicFilter());
		List<KafkaStream<byte[], byte[]>> streams = mConsumerConnector
				.createMessageStreamsByFilter(topicFilter);
		KafkaStream<byte[], byte[]> stream = streams.get(0);
		mIterator = stream.iterator();
		mLastAccessTime = new HashMap<TopicPartition, Long>();
		StatsUtil.setLabel("kafka_consumer_id", IdUtil.getConsumerId());
	}

	private void updateAccessTime(TopicPartition topicPartition) {
		long now = System.currentTimeMillis() / 1000L;
		mLastAccessTime.put(topicPartition, now);
		Iterator iterator = mLastAccessTime.entrySet().iterator();
		while (iterator.hasNext()) {
			Map.Entry pair = (Map.Entry) iterator.next();
			long lastAccessTime = (Long) pair.getValue();
			if (now - lastAccessTime > mConfig.getTopicPartitionForgetSeconds()) {
				iterator.remove();
			}
		}
	}

	private void exportStats(Message message) {
		StringBuffer topicPartitions = new StringBuffer();
		for (TopicPartition topicPartition : mLastAccessTime.keySet()) {
			if (topicPartitions.length() > 0) {
				topicPartitions.append(' ');
			}
			topicPartitions.append(topicPartition.getTopic() + '/'
					+ topicPartition.getPartition());
		}
		StatsUtil.setLabel("topic_partitions", topicPartitions.toString());
	}

	private ConsumerConfig createConsumerConfig() throws UnknownHostException {
		Properties props = new Properties();
		props.put("zookeeper.connect",
				mConfig.getZookeeperQuorum() + mConfig.getKafkaZookeeperPath());
		props.put("group.id", mConfig.getKafkaGroup());

		props.put("zookeeper.session.timeout.ms",
				Integer.toString(mConfig.getZookeeperSessionTimeoutMs()));
		props.put("zookeeper.sync.time.ms",
				Integer.toString(mConfig.getZookeeperSyncTimeMs()));
		props.put("auto.commit.enable", "false");
		// This option is required to make sure that messages are not lost for
		// new topics and
		// topics whose number of partitions has changed.
		props.put("auto.offset.reset", "smallest");
		props.put("consumer.timeout.ms",
				Integer.toString(mConfig.getConsumerTimeoutMs()));
		props.put("consumer.id", IdUtil.getConsumerId());

		return new ConsumerConfig(props);
	}

	public boolean hasNext() {
		return mIterator.hasNext();
	}

	public Message read() {
		assert hasNext();
		RateLimitUtil.acquire();
		MessageAndMetadata<byte[], byte[]> kafkaMessage = mIterator.next();
		Message message = new Message(kafkaMessage.topic(),
				kafkaMessage.partition(), kafkaMessage.offset(),
				kafkaMessage.message());
		TopicPartition topicPartition = new TopicPartition(message.getTopic(),
				message.getKafkaPartition());
		updateAccessTime(topicPartition);

		StatsUtil.setLabel(topicPartition, "reader_last_offset",
				String.valueOf(message.getOffset()));

		LOG.trace("read message [{}]", message);
		exportStats(message);
		return message;
	}
}
