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
package com.pinterest.secor.consumer;

import java.io.IOException;

import kafka.consumer.ConsumerTimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pinterest.secor.common.FileRegistry;
import com.pinterest.secor.common.OffsetTracker;
import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.message.Message;
import com.pinterest.secor.message.ParsedMessage;
import com.pinterest.secor.parser.MessageParser;
import com.pinterest.secor.reader.MessageReader;
import com.pinterest.secor.storage.StorageFactory;
import com.pinterest.secor.uploader.Uploader;
import com.pinterest.secor.util.ReflectionUtil;
import com.pinterest.secor.writer.MessageWriter;

/**
 * Consumer is a top-level component coordinating reading, writing, and
 * uploading Kafka log messages. It is implemented as a thread with the intent
 * of running multiple consumer concurrently.
 * 
 * Note that consumer is not fixed with a specific topic partition. Kafka
 * rebalancing mechanism allocates topic partitions to consumers dynamically to
 * accommodate consumer population changes.
 * 
 * @author Pawel Garbacki (pawel@pinterest.com)
 */
public class Consumer extends Thread {
	private static final Logger LOG = LoggerFactory.getLogger(Consumer.class);
	
	private final double DECAY = 0.999;

	private SecorConfig mConfig;

	private MessageReader mMessageReader;
	private MessageWriter mMessageWriter;
	private MessageParser mMessageParser;
	private Uploader mUploader;
	// TODO(pawel): we should keep a count per topic partition.
	private double mUnparsableMessages;

	public Consumer(SecorConfig config) {
		mConfig = config;
	}

	private void init() throws Exception {
		OffsetTracker offsetTracker = new OffsetTracker();
		mMessageReader = new MessageReader(mConfig, offsetTracker);
		
		FileRegistry fileRegistry = new FileRegistry();
		StorageFactory storageFactory = (StorageFactory) ReflectionUtil
				.createStorageFactory(mConfig.getStorageFactoryClassOrDefault(), mConfig);

		mMessageWriter = new MessageWriter(mConfig, offsetTracker,
				fileRegistry, storageFactory);
		mMessageParser = (MessageParser) ReflectionUtil.createMessageParser(
				mConfig.getMessageParserClass(), mConfig);
		mUploader = new Uploader(mConfig, offsetTracker, fileRegistry,
				storageFactory);
		mUnparsableMessages = 0.;
	}

	@Override
	public void run() {
		try {
			// init() cannot be called in the constructor since it contains
			// logic dependent on the
			// thread id.
			init();
		} catch (Exception e) {
			throw new RuntimeException("Failed to initialize the consumer", e);
		}
		while (true) {
			Message rawMessage = null;
			try {
				boolean hasNext = mMessageReader.hasNext();
				if (!hasNext) {
					return;
				}
				rawMessage = mMessageReader.read();
			} catch (ConsumerTimeoutException e) {
				// We wait for a new message with a timeout to periodically
				// apply the upload policy
				// even if no messages are delivered.
				LOG.trace("Consumer timed out", e);
			}
			if (rawMessage != null) {
				ParsedMessage parsedMessage = null;
				try {
					parsedMessage = mMessageParser.parse(rawMessage);
					mUnparsableMessages *= DECAY;
				} catch (Exception e) {
					mUnparsableMessages++;
					final double MAX_UNPARSABLE_MESSAGES = 1000.;
					if (mUnparsableMessages > MAX_UNPARSABLE_MESSAGES) {
						throw new RuntimeException("Failed to parse message "
								+ rawMessage, e);
					}
					LOG.warn("Failed to parse message " + rawMessage, e);
					continue;
				}
				if (parsedMessage != null) {
					try {
						mMessageWriter.write(parsedMessage);
					} catch (IOException e) {
						throw new RuntimeException("Failed to write message "
								+ parsedMessage, e);
					}
				}
			}
			// TODO(pawel): it may make sense to invoke the uploader less
			// frequently than after
			// each message.
			try {
				mUploader.applyPolicy();
			} catch (Exception e) {
				throw new RuntimeException("Failed to apply upload policy", e);
			}
		}
	}
}
