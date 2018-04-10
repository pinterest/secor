package com.pinterest.secor.util;

import java.util.Map;

import com.pinterest.secor.common.SecorConfig;

public class AvroSchemaUtil {

    public static String getAvroSubjectSuffix(SecorConfig config) {
        return config.getString("avro.schema.subject.suffix", "");
    }

    public static String getAvroSubjectOverrideGlobal(SecorConfig config) {
        return config.getString("avro.schema.subject.override.global", "");
    }

    public static Map<String, String> getAvroSubjectOverrideTopics(SecorConfig config) {
        return config.getPropertyMapForPrefix("avro.schema.subject.override.topic");
    }
}
