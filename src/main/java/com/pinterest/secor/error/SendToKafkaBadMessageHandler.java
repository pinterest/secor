package com.pinterest.secor.error;

import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Future;

import com.pinterest.secor.common.KafkaProperties;
import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.message.Message;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SendToKafkaBadMessageHandler implements BadMessageHandler {
    private static final Logger LOG = LoggerFactory.getLogger(SendToKafkaBadMessageHandler.class);

    private KafkaProducer<byte[], byte[]> mKafkaProducer;
    private String mTopicSuffix;

    public SendToKafkaBadMessageHandler(SecorConfig config) throws UnknownHostException {
        mKafkaProducer = new KafkaProducer<>(KafkaProperties.getProducerProperties(config));
        mTopicSuffix = config.getBadMessagesTopicSuffix();
    }
    
    @Override
    public Future<RecordMetadata> handleMessage(Message message, Exception exception) {
        Headers messageHeaders = new RecordHeaders(
            message.getHeaders().stream()
                .map(header -> new RecordHeader(header.getKey(), header.getValue()))
                .toArray(Header[]::new)
        );
        
        byte[] secorException = exception.getMessage().getBytes(StandardCharsets.UTF_8);
        messageHeaders.add("secor-exception", secorException);

        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(
            message.getTopic() + mTopicSuffix, 
            null,
            null,
            message.getKafkaKey(),
            message.getPayload(),
            messageHeaders
        );

        return mKafkaProducer.send(record, (recordMetadata, kafkaException) -> {
            if (kafkaException != null) {
                LOG.trace("Failed to send message to Kafka: {}", message, kafkaException);
            } else {
                Message sentMessage = new Message(
                    recordMetadata.topic(), 
                    recordMetadata.partition(), 
                    recordMetadata.offset(), 
                    message.getKafkaKey(), 
                    message.getPayload(), 
                    recordMetadata.timestamp(), 
                    message.getHeaders()
                );

                LOG.info("Sent message to Kafka: {}", sentMessage);
            }
        });
    }
}
