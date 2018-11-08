package com.pinterest.secor.reader;

import com.google.common.collect.ImmutableMap;
import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.common.TopicPartition;
import com.pinterest.secor.message.Message;
import com.pinterest.secor.timestamp.KafkaMessageTimestampFactory;
import com.pinterest.secor.util.IdUtil;
import kafka.common.OffsetAndMetadata;
import kafka.common.OffsetMetadata;
import kafka.common.TopicAndPartition;
import kafka.consumer.*;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class LegacyKafkaMessageIterator implements KafkaMessageIterator {
    private static final Logger LOG = LoggerFactory.getLogger(MessageReader.class);
    private SecorConfig mConfig;
    private ConsumerConnector mConsumerConnector;
    private ConsumerIterator mIterator;
    private KafkaMessageTimestampFactory mKafkaMessageTimestampFactory;
    private ConsumerConnector mKafkaCommitter;

    public LegacyKafkaMessageIterator() {
    }

    @Override
    public boolean hasNext() {
        try {
            return mIterator.hasNext();
        } catch (ConsumerTimeoutException e) {
            throw new LegacyConsumerTimeoutException(e);
        }
    }

    @Override
    public Message next() {
        MessageAndMetadata<byte[], byte[]> kafkaMessage;
        try {
            kafkaMessage = mIterator.next();
        } catch (ConsumerTimeoutException e) {
            throw new LegacyConsumerTimeoutException(e);
        }

        long timestamp = 0L;
        if (mConfig.useKafkaTimestamp()) {
            timestamp = mKafkaMessageTimestampFactory.getKafkaMessageTimestamp().getTimestamp(kafkaMessage);
        }

        return new Message(kafkaMessage.topic(), kafkaMessage.partition(),
                kafkaMessage.offset(), kafkaMessage.key(),
                kafkaMessage.message(), timestamp);
    }

    @Override
    public void init(SecorConfig config) throws UnknownHostException {
        this.mConfig = config;

        this.mConsumerConnector = createConsumerConnector(createConsumerConfig());
        this.mKafkaCommitter = createConsumerConnector(createKafkaCommitterConfig());

        if (!mConfig.getKafkaTopicBlacklist().isEmpty() && !mConfig.getKafkaTopicFilter().isEmpty()) {
            throw new RuntimeException("Topic filter and blacklist cannot be both specified.");
        }
        TopicFilter topicFilter = !mConfig.getKafkaTopicBlacklist().isEmpty() ? new Blacklist(mConfig.getKafkaTopicBlacklist()) :
                new Whitelist(mConfig.getKafkaTopicFilter());
        LOG.debug("Use TopicFilter {}({})", topicFilter.getClass(), topicFilter);
        List<KafkaStream<byte[], byte[]>> streams =
                mConsumerConnector.createMessageStreamsByFilter(topicFilter);
        KafkaStream<byte[], byte[]> stream = streams.get(0);
        mIterator = stream.iterator();
        mKafkaMessageTimestampFactory = new KafkaMessageTimestampFactory(mConfig.getKafkaMessageTimestampClass());
    }

    @Override
    public void commit(TopicPartition topicPartition, long offset) {
        // warn: commits all topics & partitions - the data may not have been written to output yet
        mConsumerConnector.commitOffsets();
    }

    @Override
    public void commitToKafka(TopicPartition topicPartition, long offset) {
        TopicAndPartition kafkaTopicPartition = new TopicAndPartition(
                topicPartition.getTopic(), topicPartition.getPartition());
        OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(new OffsetMetadata(offset, null),
                // calling scala API from java requires passing these even though defined as defaults in scala code
                OffsetCommitRequest.DEFAULT_TIMESTAMP, OffsetCommitRequest.DEFAULT_TIMESTAMP);
        Map<TopicAndPartition, OffsetAndMetadata> offsets = ImmutableMap.of(kafkaTopicPartition, offsetAndMetadata);
        try {
            LOG.debug("committing {} offset {} to kafka", topicPartition, offset);
            mKafkaCommitter.commitOffsets(offsets, true);
        } catch (CommitFailedException e) {
            LOG.trace("kafka commit failed due to group re-balance", e);
        }
    }

    private Properties createConsumerConfig() throws UnknownHostException {
        Properties props = new Properties();
        props.put("zookeeper.connect", mConfig.getZookeeperQuorum() + mConfig.getKafkaZookeeperPath());
        props.put("group.id", mConfig.getKafkaGroup());

        props.put("zookeeper.session.timeout.ms",
                Integer.toString(mConfig.getZookeeperSessionTimeoutMs()));
        props.put("zookeeper.sync.time.ms", Integer.toString(mConfig.getZookeeperSyncTimeMs()));
        props.put("auto.commit.enable", "false");
        props.put("auto.offset.reset", mConfig.getConsumerAutoOffsetReset());
        props.put("consumer.timeout.ms", Integer.toString(mConfig.getConsumerTimeoutMs()));
        props.put("consumer.id", IdUtil.getConsumerId());
        // Properties required to upgrade from kafka 0.8.x to 0.9.x
        props.put("dual.commit.enabled", mConfig.getDualCommitEnabled());
        props.put("offsets.storage", mConfig.getOffsetsStorage());

        props.put("partition.assignment.strategy", mConfig.getPartitionAssignmentStrategy());
        if (mConfig.getRebalanceMaxRetries() != null &&
                !mConfig.getRebalanceMaxRetries().isEmpty()) {
            props.put("rebalance.max.retries", mConfig.getRebalanceMaxRetries());
        }
        if (mConfig.getRebalanceBackoffMs() != null &&
                !mConfig.getRebalanceBackoffMs().isEmpty()) {
            props.put("rebalance.backoff.ms", mConfig.getRebalanceBackoffMs());
        }
        if (mConfig.getSocketReceiveBufferBytes() != null &&
                !mConfig.getSocketReceiveBufferBytes().isEmpty()) {
            props.put("socket.receive.buffer.bytes", mConfig.getSocketReceiveBufferBytes());
        }
        if (mConfig.getFetchMessageMaxBytes() != null && !mConfig.getFetchMessageMaxBytes().isEmpty()) {
            props.put("fetch.message.max.bytes", mConfig.getFetchMessageMaxBytes());
        }
        if (mConfig.getFetchMinBytes() != null && !mConfig.getFetchMinBytes().isEmpty()) {
            props.put("fetch.min.bytes", mConfig.getFetchMinBytes());
        }
        if (mConfig.getFetchWaitMaxMs() != null && !mConfig.getFetchWaitMaxMs().isEmpty()) {
            props.put("fetch.wait.max.ms", mConfig.getFetchWaitMaxMs());
        }

        return props;
    }

    private Properties createKafkaCommitterConfig() throws UnknownHostException {
        Properties kafkaCommitterProps = createConsumerConfig();
        kafkaCommitterProps.put("offsets.storage", "kafka");
        return kafkaCommitterProps;
    }

    private ConsumerConnector createConsumerConnector(Properties consumerConfig) {
        return Consumer.createJavaConsumerConnector(new ConsumerConfig(consumerConfig));
    }

}
