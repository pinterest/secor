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
package com.pinterest.secor.consumer;

import com.pinterest.secor.common.DeterministicUploadPolicyTracker;
import com.pinterest.secor.common.FileRegistry;
import com.pinterest.secor.common.OffsetTracker;
import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.common.ShutdownHookRegistry;
import com.pinterest.secor.message.Message;
import com.pinterest.secor.message.ParsedMessage;
import com.pinterest.secor.monitoring.MetricCollector;
import com.pinterest.secor.parser.MessageParser;
import com.pinterest.secor.reader.KafkaMessageIterator;
import com.pinterest.secor.reader.KafkaMessageIteratorFactory;
import com.pinterest.secor.reader.LegacyConsumerTimeoutException;
import com.pinterest.secor.reader.MessageReader;
import com.pinterest.secor.rebalance.RebalanceHandler;
import com.pinterest.secor.rebalance.RebalanceSubscriber;
import com.pinterest.secor.transformer.MessageTransformer;
import com.pinterest.secor.uploader.UploadManager;
import com.pinterest.secor.uploader.Uploader;
import com.pinterest.secor.util.ReflectionUtil;
import com.pinterest.secor.writer.MessageWriter;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Consumer is a top-level component coordinating reading, writing, and uploading Kafka log
 * messages.  It is implemented as a thread with the intent of running multiple consumer
 * concurrently.
 *
 * Note that consumer is not fixed with a specific topic partition.  Kafka rebalancing mechanism
 * allocates topic partitions to consumers dynamically to accommodate consumer population changes.
 *
 * @author Pawel Garbacki (pawel@pinterest.com)
 */
public class Consumer extends Thread {
    private static final Logger LOG = LoggerFactory.getLogger(Consumer.class);
    private static final double DECAY = 0.999;

    protected SecorConfig mConfig;
    protected MetricCollector mMetricCollector;

    protected MessageReader mMessageReader;
    protected MessageWriter mMessageWriter;
    protected MessageParser mMessageParser;
    protected OffsetTracker mOffsetTracker;
    private DeterministicUploadPolicyTracker mDeterministicUploadPolicyTracker;
    protected MessageTransformer mMessageTransformer;
    protected Uploader mUploader;
    protected KafkaMessageIterator mKafkaMessageIterator;

    // TODO(pawel): we should keep a count per topic partition.
    private boolean isLegacyConsumer;
    private final double mMaxBadMessages;
    protected double mBadMessages;
    // If we aren't configured to upload on shutdown, then don't bother to check
    // the volatile variable.
    private boolean mUploadOnShutdown;
    private volatile boolean mShuttingDown = false;
    private static volatile boolean mCallingSystemExit = false;

    public Consumer(SecorConfig config, MetricCollector metricCollector) {
        mConfig = config;
        mMetricCollector = metricCollector;
        isLegacyConsumer = true;
        mMaxBadMessages = config.getMaxBadMessages();
    }

    private void init() throws Exception {
        mOffsetTracker = new OffsetTracker();
        if (mConfig.getDeterministicUpload()) {
            mDeterministicUploadPolicyTracker = new DeterministicUploadPolicyTracker(
                mConfig.getMaxFileTimestampRangeMillis(), mConfig.getMaxInputPayloadSizeBytes()
            );
        } else {
            mDeterministicUploadPolicyTracker = null;
        }
        mKafkaMessageIterator = KafkaMessageIteratorFactory.getIterator(mConfig.getKafkaMessageIteratorClass(), mConfig);
        mMessageReader = new MessageReader(mConfig, mOffsetTracker, mKafkaMessageIterator);

        FileRegistry fileRegistry = new FileRegistry(mConfig);
        UploadManager uploadManager = ReflectionUtil.createUploadManager(mConfig.getUploadManagerClass(), mConfig);

        mUploader = ReflectionUtil.createUploader(mConfig.getUploaderClass());
        mUploader.init(mConfig, mOffsetTracker, fileRegistry, uploadManager, mMessageReader, mMetricCollector,
                       mDeterministicUploadPolicyTracker);

        if (mKafkaMessageIterator instanceof RebalanceSubscriber) {
            ((RebalanceSubscriber) mKafkaMessageIterator).subscribe(new RebalanceHandler(mUploader, fileRegistry, mOffsetTracker), mConfig);
            isLegacyConsumer = false;
        }
        mMessageWriter = new MessageWriter(mConfig, mOffsetTracker, fileRegistry, mDeterministicUploadPolicyTracker);
        mMessageParser = ReflectionUtil.createMessageParser(mConfig.getMessageParserClass(), mConfig);
        mMessageTransformer = ReflectionUtil.createMessageTransformer(mConfig.getMessageTransformerClass(), mConfig);
        mBadMessages = 0.;

        mUploadOnShutdown = mConfig.getUploadOnShutdown();
        if (mUploadOnShutdown) {
            if (mDeterministicUploadPolicyTracker != null) {
                throw new RuntimeException("Can't set secor.upload.on.shutdown with secor.upload.deterministic!");
            }
            ShutdownHookRegistry.registerHook(1, new FinalUploadShutdownHook());
        }
    }

    // When the JVM starts to shut down, tell the Consumer thread to upload once and wait for it to finish.
    private class FinalUploadShutdownHook extends Thread {
        @Override
        public void run() {
            if (mCallingSystemExit) {
                // We're shutting down because a consumer thread crashed. We don't want to do a final
                // upload: we just want to exit.  If this particular thread was the one that crashed,
                // we would deadlock if we didn't return here (the Consumer thread is blocked
                // in System.exit on shutdown handlers to run, and this thread would be blocked on the
                // Consumer thread to exit).  Even if it were a different thread that crashed, we
                // still want to exit the process as soon as possible: until we restart the process,
                // the partition being read by the other thread won't be consumed by anyone.
                return;
            }
            mShuttingDown = true;
            try {
                Consumer.this.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void run() {
        try {
            try {
                // init() cannot be called in the constructor since it contains logic dependent on the
                // thread id.
                init();
            } catch (Exception e) {
                throw new RuntimeException("Failed to initialize the consumer", e);
            }
            // check upload policy every N seconds or 10,000 messages/consumer timeouts
            long checkEveryNSeconds = Math.min(10 * 60, mConfig.getMaxFileAgeSeconds() / 2);
            long checkMessagesPerSecond = mConfig.getMessagesPerSecond();
            long nMessages = 0;
            long lastChecked = System.currentTimeMillis();
            while (true) {
                boolean hasMoreMessages = consumeNextMessage();
                if (!hasMoreMessages) {
                    break;
                }

                if (mUploadOnShutdown && mShuttingDown) {
                    LOG.info("Shutting down");
                    break;
                }

                long now = System.currentTimeMillis();
                if (mDeterministicUploadPolicyTracker != null ||
                    nMessages++ % checkMessagesPerSecond == 0 ||
                    (now - lastChecked) > checkEveryNSeconds * 1000) {
                    lastChecked = now;
                    checkUploadPolicy(false);
                }
            }
            if (mDeterministicUploadPolicyTracker == null) {
                LOG.info("Done reading messages; uploading what we have");
                checkUploadPolicy(true);
            }
            LOG.info("Consumer thread done");
        } catch (Throwable t) {
            LOG.error("Thread failed", t);
            mCallingSystemExit = true;
            System.exit(1);
        }
    }

    protected void checkUploadPolicy(boolean forceUpload) {
        try {
            mUploader.applyPolicy(forceUpload);
        } catch (Exception e) {
            throw new RuntimeException("Failed to apply upload policy", e);
        }
    }

    // @return whether there are more messages left to consume
    protected boolean consumeNextMessage() {
        Message rawMessage = null;
        try {
            boolean hasNext = mMessageReader.hasNext();
            if (!hasNext) {
                return false;
            }
            rawMessage = mMessageReader.read();
        } catch (LegacyConsumerTimeoutException e) {
            // We wait for a new message with a timeout to periodically apply the upload policy
            // even if no messages are delivered.
            LOG.trace("Consumer timed out", e);
        }
        if (rawMessage != null) {
            // Before parsing, update the offset and remove any redundant data
            adjustOffsets(rawMessage);
            ParsedMessage parsedMessage = null;
            try {
                Message transformedMessage = mMessageTransformer.transform(rawMessage);
                if (transformedMessage == null) {
                    return true;
                }

                parsedMessage = mMessageParser.parse(transformedMessage);

                if (parsedMessage != null) {
                    writeMessage(rawMessage, parsedMessage);
                }
            } catch (Exception e) {
                handleWriteError(rawMessage, parsedMessage, e);
            }
        }
        return true;
    }

    /**
     * Helper to get the offset tracker (used in tests)
     *
     * @return the offset tracker
     */
    public OffsetTracker getOffsetTracker() {
        return this.mOffsetTracker;
    }

    private void adjustOffsets(Message rawMessage) {
        try {
            mMessageWriter.adjustOffset(rawMessage, isLegacyConsumer);
        } catch (IOException e) {
            throw new RuntimeException("Failed to adjust offset.", e);
        }
    }

    private void writeMessage(Message rawMessage, ParsedMessage parsedMessage) throws Exception {
        mMessageWriter.write(parsedMessage);
        mBadMessages *= DECAY;
        mMetricCollector.metric("consumer.message_size_bytes", rawMessage.getPayload().length,
                rawMessage.getTopic());
        mMetricCollector.increment("consumer.throughput_bytes", rawMessage.getPayload().length,
                rawMessage.getTopic());

        if (mDeterministicUploadPolicyTracker != null) {
            mDeterministicUploadPolicyTracker.track(rawMessage);
        }
    }

    private void handleWriteError(Message rawMessage, ParsedMessage parsedMessage, Exception exception) {
        mMetricCollector.increment("consumer.message_errors.count", rawMessage.getTopic());
        mBadMessages++;
        if (mMaxBadMessages != -1 && mBadMessages > mMaxBadMessages) {
            throw new RuntimeException("Failed to write message " + rawMessage, exception);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("Failed to write message raw: {}; parsed: {}", rawMessage, parsedMessage, exception);
        }
    }
}
