package com.pinterest.secor.error;

import java.util.concurrent.Future;

import com.pinterest.secor.message.Message;

import org.apache.kafka.clients.producer.RecordMetadata;

public interface BadMessageHandler {
    
    Future<RecordMetadata> handleMessage(Message message, Exception exception);

}
