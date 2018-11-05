package com.pinterest.secor.reader;

import com.pinterest.secor.common.SecorConfig;

import java.net.UnknownHostException;

public class KafkaMessageIteratorFactory {
    public static KafkaMessageIterator getIterator(String className, SecorConfig config) {
        KafkaMessageIterator iterator;
        try {
            Class iteratorClass = Class.forName(className);
            iterator = (KafkaMessageIterator) iteratorClass.newInstance();
            iterator.init(config);
        } catch (ClassNotFoundException | IllegalAccessException | InstantiationException | UnknownHostException e) {
            throw new RuntimeException(e);
        }

        return iterator;
    }
}
