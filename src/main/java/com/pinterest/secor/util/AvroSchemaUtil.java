package com.pinterest.secor.util;

import com.pinterest.secor.common.SecorConfig;

public class AvroSchemaUtil {

    public static String getAvroSubjectSuffix(SecorConfig config) {
        return config.getString("avro.schema.subject.suffix", "");
    }

    public static String getAvroSubjectGlobalOverride(SecorConfig config) {
        return config.getString("avro.schema.subject.global.override", "");
    }
}
