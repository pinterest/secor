package com.pinterest.secor.common;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDecoder;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import junit.framework.TestCase;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.modules.junit4.PowerMockRunner;
import java.util.Properties;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
public class SecorSchemaRegistryClientTest extends TestCase {

    private KafkaAvroDecoder kafkaAvroDecoder;
    private SchemaRegistryClient schemaRegistryClient;
    private SecorSchemaRegistryClient secorSchemaRegistryClient;
    private SecorConfig secorConfig;
    private SpecificDatumWriter<GenericRecord> writer;
    private KafkaAvroSerializer avroSerializer;

    @Override
    public void setUp() {
        initKafka();
        SecorConfig secorConfig = Mockito.mock(SecorConfig.class);
        when(secorConfig.getSchemaRegistryUrl()).thenReturn("schema-registry-url");
        secorSchemaRegistryClient = new SecorSchemaRegistryClient(secorConfig);
        secorSchemaRegistryClient.decoder = kafkaAvroDecoder;
        secorSchemaRegistryClient.schemaRegistryClient = schemaRegistryClient;
    }

    private void initKafka() {
        schemaRegistryClient = new MockSchemaRegistryClient();
        kafkaAvroDecoder = new KafkaAvroDecoder(schemaRegistryClient);
        Properties defaultConfig = new Properties();
        defaultConfig.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
        avroSerializer = new KafkaAvroSerializer(schemaRegistryClient);
    }

    @Test
    public void testDecodeMessage() throws Exception {
        Schema schemaV1 = SchemaBuilder.record("Foo")
                .fields()
                .name("data_field_1").type().intType().noDefault()
                .name("timestamp").type().longType().noDefault()
                .endRecord();
        //backward compatible schema change
        Schema schemaV2 = SchemaBuilder.record("Foo")
                .fields()
                .name("data_field_1").type().intType().noDefault()
                .name("data_field_2").type().stringType().noDefault()
                .name("timestamp").type().longType().noDefault()
                .endRecord();
        GenericRecord record1 = new GenericRecordBuilder(schemaV1)
                .set("data_field_1", 1)
                .set("timestamp", 1467176315L)
                .build();
        GenericRecord record2 = new GenericRecordBuilder(schemaV2)
                .set("data_field_1", 1)
                .set("data_field_2", "hello")
                .set("timestamp", 1467176316L)
                .build();
        GenericRecord output = secorSchemaRegistryClient.decodeMessage("test-avr-topic", avroSerializer.serialize("test-avr-topic", record1));
        assertEquals(secorSchemaRegistryClient.getSchema("test-avr-topic"), schemaV1);
        assertEquals(output.get("data_field_1"), 1);
        assertEquals(output.get("timestamp"), 1467176315L);

        output = secorSchemaRegistryClient.decodeMessage("test-avr-topic", avroSerializer.serialize("test-avr-topic", record2));
        assertEquals(secorSchemaRegistryClient.getSchema("test-avr-topic"), schemaV2);
        assertEquals(output.get("data_field_1"), 1);
        assertTrue(StringUtils.equals((output.get("data_field_2")).toString(), "hello"));
        assertEquals(output.get("timestamp"), 1467176316L);
    }
}