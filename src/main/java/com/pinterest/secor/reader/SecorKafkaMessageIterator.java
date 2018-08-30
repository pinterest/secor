package com.pinterest.secor.reader;

import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.common.ZookeeperConnector;
import com.pinterest.secor.message.Message;
import com.pinterest.secor.util.IdUtil;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.time.Duration;
import java.util.*;
import java.util.function.Consumer;
import java.util.regex.Pattern;

public class SecorKafkaMessageIterator implements KafkaMessageIterator {
    private static final Logger LOG = LoggerFactory.getLogger(SecorKafkaMessageIterator.class);
    private KafkaConsumer<byte[], byte[]> mKafkaConsumer;
    private Deque<ConsumerRecord<byte[], byte[]>> mRecordsBatch;
    private ZookeeperConnector mZookeeperConnector;
    private int mPollTimeout;

    @Override
    public boolean hasNext() {
        return true;
    }

    @Override
    public Message next() {
        if (mRecordsBatch.isEmpty()) {
            mKafkaConsumer.poll(Duration.ofSeconds(mPollTimeout)).forEach(mRecordsBatch::add);
        }

        if (mRecordsBatch.isEmpty()) {
            return null;
        } else {
            ConsumerRecord<byte[], byte[]> consumerRecord = mRecordsBatch.pop();
            return new Message(consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset(),
                    consumerRecord.key(), consumerRecord.value(), consumerRecord.timestamp());
        }
    }

    @Override
    public void init(SecorConfig config) throws UnknownHostException {
        Properties props = new Properties();
        String offsetResetConfig = config.getNewConsumerAutoOffsetReset();
        mPollTimeout = config.getNewConsumerPollTimeoutSeconds();

        props.put("bootstrap.servers", config.getKafkaSeedBrokerHost() + ":" + config.getKafkaSeedBrokerPort());
        props.put("group.id", config.getKafkaGroup());
        props.put("enable.auto.commit", false);
        props.put("auto.offset.reset", offsetResetConfig);
        // TODO: Check if old consumer.timeout.ms is equivalent as this
        props.put("request.timeout.ms", config.getConsumerTimeoutMs());
        props.put("client.id", IdUtil.getConsumerId());
        props.put("key.deserializer", ByteArrayDeserializer.class);
        props.put("value.deserializer", ByteArrayDeserializer.class);

        optionalConfig(config.getSocketReceiveBufferBytes(), conf -> props.put("receive.buffer.bytes", conf));
        optionalConfig(config.getFetchMinBytes(), conf -> props.put("fetch.min.bytes", conf));
        optionalConfig(config.getFetchMaxBytes(), conf -> props.put("fetch.max.bytes", conf));
        optionalConfig(config.getSslKeyPassword(), conf -> props.put("ssl.key.password", conf));
        optionalConfig(config.getSslKeystoreLocation(), conf -> props.put("ssl.keystore.location", conf));
        optionalConfig(config.getSslKeystorePassword(), conf -> props.put("ssl.keystore.password", conf));
        optionalConfig(config.getSslTruststoreLocation(), conf -> props.put("ssl.truststore.location", conf));
        optionalConfig(config.getSslTruststorePassword(), conf -> props.put("ssl.truststore.password", conf));
        optionalConfig(config.getIsolationLevel(), conf -> props.put("isolation.level", conf));
        optionalConfig(config.getMaxPollRecords(), conf -> props.put("max.poll.records", conf));
        optionalConfig(config.getSaslClientCallbackHandlerClass(), conf -> props.put("sasl.client.callback.handler.class", conf));
        optionalConfig(config.getSaslJaasConfig(), conf -> props.put("sasl.jaas.config", conf));
        optionalConfig(config.getSaslKerberosServiceName(), conf -> props.put("sasl.kerberos.service.name", conf));
        optionalConfig(config.getSaslLoginCallbackHandlerClass(), conf -> props.put("sasl.login.callback.handler.class", conf));
        optionalConfig(config.getSaslLoginClass(), conf -> props.put("sasl.login.class", conf));
        optionalConfig(config.getSaslMechanism(), conf -> props.put("sasl.mechanism", conf));
        optionalConfig(config.getSecurityProtocol(), conf -> props.put("security.protocol", conf));
        optionalConfig(config.getSslEnabledProtocol(), conf -> props.put("ssl.enabled.protocols", conf));
        optionalConfig(config.getSslKeystoreType(), conf -> props.put("ssl.keystore.type", conf));
        optionalConfig(config.getSslProtocol(), conf -> props.put("ssl.protocol", conf));
        optionalConfig(config.getSslProvider(), conf -> props.put("ssl.provider", conf));
        optionalConfig(config.getSslTruststoreType(), conf -> props.put("ssl.truststore.type", conf));

        mZookeeperConnector = new ZookeeperConnector(config);
        mRecordsBatch = new ArrayDeque<>();
        mKafkaConsumer = new KafkaConsumer<>(props);
        mKafkaConsumer.subscribe(Pattern.compile(config.getKafkaTopicFilter()), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> collection) {
                // TODO: Validate if secor needs something done here
                for (TopicPartition topicPartition : collection) {
                    LOG.debug("Topic partition {} revoked from consumer", topicPartition);
                }
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                Map<TopicPartition, Long> committedOffsets = getCommittedOffsets(collection);
                committedOffsets.forEach(((topicPartition, offset) -> {
                    if (offset == -1) {
                        if (offsetResetConfig.equals("earliest")) {
                            mKafkaConsumer.seekToBeginning(collection);
                        } else if (offsetResetConfig.equals("latest")) {
                            mKafkaConsumer.seekToEnd(collection);
                        }
                    } else {
                        LOG.debug("Seeking {} to offset {}", topicPartition, Math.max(0, offset));
                        mKafkaConsumer.seek(topicPartition, Math.max(0, offset));
                    }
                }));

            }
        });
    }

    private Map<TopicPartition, Long> getCommittedOffsets(Collection<TopicPartition> assignment) {
        Map<TopicPartition, Long> committedOffsets = new HashMap<>();

        for (TopicPartition topicPartition : assignment) {
            com.pinterest.secor.common.TopicPartition secorTopicPartition =
                    new com.pinterest.secor.common.TopicPartition(topicPartition.topic(), topicPartition.partition());
            try {
                long committedOffset = mZookeeperConnector.getCommittedOffsetCount(secorTopicPartition);
                committedOffsets.put(topicPartition, committedOffset);
            } catch (Exception e) {
                LOG.trace("Unable to fetch committed offsets from zookeeper", e);
                throw new RuntimeException(e);
            }
        }

        return committedOffsets;
    }

    @Override
    public void commit() {
        // TODO: Confirm that this will never work, since secor logic won't permit this commit after a rebalance
    }

    private void optionalConfig(String maybeConf, Consumer<String> configConsumer) {
        Optional.ofNullable(maybeConf).filter(conf -> !conf.isEmpty()).ifPresent(configConsumer);
    }
}
