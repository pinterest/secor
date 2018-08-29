package com.pinterest.secor.common;

import com.pinterest.secor.message.Message;
import org.apache.thrift.TException;

public interface KafkaClient {
    int getNumPartitions(String topic);

    Message getLastMessage(TopicPartition topicPartition) throws TException;

    Message getCommittedMessage(TopicPartition topicPartition) throws Exception;

    void init(SecorConfig config);
}
