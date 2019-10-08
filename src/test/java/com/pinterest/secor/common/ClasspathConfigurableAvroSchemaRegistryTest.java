package com.pinterest.secor.common;

import ai.humn.telematics.avro.DataHelper;
import ai.humn.telematics.avro.Serializer;
import ai.humn.telematics.avro.dto.RacDTO;
import com.pinterest.secor.util.AvroSchemaRegistryFactory;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.HashMap;

import static com.pinterest.secor.testing.ConfigTestUtils.createMockTestConfig;

public class ClasspathConfigurableAvroSchemaRegistryTest {
    private final String testTopicName = "test_topic_rac";

    @Test
    public void testSchemaIsRetrievedPropery() {

        SecorConfig secorConfig = createMockTestConfig();
        AvroSchemaRegistry registry = AvroSchemaRegistryFactory.getSchemaRegistry(secorConfig);
        Schema schema = registry.getSchema(testTopicName);
        Assert.assertEquals(schema, RacDTO.SCHEMA$);
    }

    @Test
    public void testRegistryCanDeserializeMessageProperly() throws IOException {
        SecorConfig secorConfig = createMockTestConfig();
        RacDTO racDTO = DataHelper.correctRacData();
        Serializer serializer = new Serializer();

        byte[] bytes = serializer.serialize(racDTO);
        AvroSchemaRegistry registry = AvroSchemaRegistryFactory.getSchemaRegistry(secorConfig);
        GenericRecord record = registry.deserialize(testTopicName, bytes);
        Assert.assertEquals(racDTO, record);
    }

    @Test(expected = SerializationException.class)
    public void testExceptionIsThrownWhenDeserializationFails() {
        SecorConfig secorConfig = createMockTestConfig();
        byte[] emptyByteArray = new byte[0];

        AvroSchemaRegistry registry = AvroSchemaRegistryFactory.getSchemaRegistry(secorConfig);
        GenericRecord record = registry.deserialize(testTopicName, emptyByteArray);
    }

    @Test(expected = RuntimeException.class)
    public void testExceptionIsThrownWhenClassNotPresentInClasspath() {
        SecorConfig secorConfig = createMockTestConfig();
        HashMap<String, String> nonExistingClassSchema = new HashMap<>();
        nonExistingClassSchema.put(testTopicName, "org.failure.SoFail");
        Mockito.when(secorConfig.getAvroSpecificClassesSchemas()).thenReturn(nonExistingClassSchema);
        secorConfig.getAvroSpecificClassesSchemas().put("test_topic_error", "org.error.this.should.fail");
        AvroSchemaRegistry registry = AvroSchemaRegistryFactory.getSchemaRegistry(secorConfig);
    }

}
