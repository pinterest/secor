package com.pinterest.secor.testing;

import ai.humn.avro.RacDTO;
import ai.humn.telematics.avro.dto.ObdDataDTO;
import com.pinterest.secor.common.SecorConfig;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.TimeZone;

public class ConfigTestUtils {
    public static SecorConfig createMockTestConfig() {
        SecorConfig mock = Mockito.mock(SecorConfig.class);
        HashMap<String, String> specificClassesPerTopic = new HashMap<>();
        specificClassesPerTopic.put("test_topic_rac", RacDTO.class.getCanonicalName());
        specificClassesPerTopic.put("test_topic_obd", ObdDataDTO.class.getCanonicalName());

        Mockito.when(mock.getSchemaRegistryUrl()).thenReturn("");
        Mockito.when(mock.getAvroSpecificClassesSchemas()).thenReturn(specificClassesPerTopic);
        Mockito.when(mock.getFinalizerDelaySeconds()).thenReturn(100);
        Mockito.when(mock.isMessageTimestampRequired()).thenReturn(true);
        Mockito.when(mock.getTimeZone()).thenReturn(TimeZone.getTimeZone("UTC"));

        return mock;
    }
}
