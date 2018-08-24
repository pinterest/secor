package com.pinterest.secor.reader;

import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.message.Message;

import java.net.UnknownHostException;

public interface KafkaMessageIterator {
    boolean hasNext();
    Message next();
    void init(SecorConfig config) throws UnknownHostException;
    void commit();
}
