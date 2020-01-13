package com.pinterest.secor.parser;

import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.message.Message;

public class KafkaTimestampParser extends TimestampedMessageParser {
    public KafkaTimestampParser(SecorConfig config) {
        super(config);
    }

    @Override
    public long extractTimestampMillis(Message message) {
        return message.getTimestamp();
    }
}
