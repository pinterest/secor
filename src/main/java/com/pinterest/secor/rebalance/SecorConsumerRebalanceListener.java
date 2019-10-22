package com.pinterest.secor.rebalance;

import com.pinterest.secor.common.ZookeeperConnector;
import com.pinterest.secor.util.StatsUtil;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SecorConsumerRebalanceListener implements ConsumerRebalanceListener {

    private static final Logger LOG = LoggerFactory.getLogger(SecorConsumerRebalanceListener.class);

    private KafkaConsumer<byte[], byte[]> mKafkaConsumer;

    private ZookeeperConnector mZookeeperConnector;

    private boolean skipZookeeperOffsetSeek;

    private String offsetResetConfig;

    private RebalanceHandler handler;


    public SecorConsumerRebalanceListener(KafkaConsumer<byte[], byte[]> mKafkaConsumer, ZookeeperConnector mZookeeperConnector, boolean skipZookeeperOffsetSeek, String offsetResetConfig, RebalanceHandler handler) {
        this.mKafkaConsumer = mKafkaConsumer;
        this.mZookeeperConnector = mZookeeperConnector;
        this.skipZookeeperOffsetSeek = skipZookeeperOffsetSeek;
        this.offsetResetConfig = offsetResetConfig;
        this.handler = handler;
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> assignedPartitions) {
        // Here we do a force upload/commit so the other consumer take over partition(s) later don't need to rewrite the same messages
        LOG.info("re-balance starting, forcing uploading current assigned partitions {}", assignedPartitions);

        List<com.pinterest.secor.common.TopicPartition> tps = assignedPartitions.stream().map(p -> new com.pinterest.secor.common.TopicPartition(p.topic(), p.partition())).collect(Collectors.toList());
        tps.stream().map(p -> p.getTopic()).collect(Collectors.toSet()).forEach(topic -> StatsUtil.incr("secor.consumer_rebalance_count." + topic));
        handler.uploadOnRevoke(tps);
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> collection) {
        if (skipZookeeperOffsetSeek) {
            LOG.debug("offset storage set to kafka. Skipping reading offsets from zookeeper");
            return;
        }
        Map<TopicPartition, Long> committedOffsets = getCommittedOffsets(collection);
        committedOffsets.forEach(((topicPartition, offset) -> {
            if (offset == -1) {
                if (offsetResetConfig.equals("earliest")) {
                    mKafkaConsumer.seekToBeginning(Collections.singleton(topicPartition));
                } else if (offsetResetConfig.equals("latest")) {
                    mKafkaConsumer.seekToEnd(Collections.singleton(topicPartition));
                }
            } else {
                long committedOffset = Math.max(0, offset);
                LOG.info("Seeking {} to offset {} after partition assigned", topicPartition, committedOffset);
                mKafkaConsumer.seek(topicPartition, committedOffset);
            }
        }));
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


}
