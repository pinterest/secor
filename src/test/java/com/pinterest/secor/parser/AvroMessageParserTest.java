package com.pinterest.secor.parser;

import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.message.Message;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.ByteArrayOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Date;

import static org.junit.Assert.assertEquals;

@RunWith(PowerMockRunner.class)
public class AvroMessageParserTest {

    private SecorConfig config;
    private Message messageWithTimestamp;
    private Message messageWithoutTimestamp;
    private Message messageWithBadTimestamp;
    private AvroMessageParser parser;
    private long timestamp;

    @Before
    public void setUp() throws Exception {
        String schemaText = "{\"namespace\": \"example.avro\",\n" +
                " \"type\": \"record\",\n" +
                " \"name\": \"User\",\n" +
                " \"fields\": [\n" +
                "     {\"name\": \"name\", \"type\": \"string\"},\n" +
                "     {\"name\": \"timestamp\", \"type\": [\"long\", \"null\", \"string\"]},\n" +
                "     {\"name\": \"favorite_number\",  \"type\": [\"int\", \"null\"]},\n" +
                "     {\"name\": \"favorite_color\", \"type\": [\"string\", \"null\"]}\n" +
                " ]\n" +
                "}";
        Path avroFile = Files.createTempFile("foo", "avro");
        FileUtils.writeStringToFile(avroFile.toFile(), schemaText);
        Schema schema = new Schema.Parser().parse(avroFile.toFile());
        config = Mockito.mock(SecorConfig.class);
        Mockito.when(config.getMessageTimestampName()).thenReturn("timestamp");
        Mockito.when(config.getMessageInputAvroSchema()).thenReturn(avroFile.toAbsolutePath().toString());
        parser = new AvroMessageParser(config);
        timestamp = new Date().getTime();

        GenericRecord user1 = new GenericData.Record(schema);
        user1.put("name", "able-baker-1");
        user1.put("timestamp", timestamp);
        user1.put("favorite_number", 42);

        GenericRecord user2 = new GenericData.Record(schema);
        user2.put("name", "able-baker-2");
        user2.put("favorite_number", 42);

        GenericRecord user3 = new GenericData.Record(schema);
        user3.put("name", "able-baker-2");
        user3.put("timestamp", "foo");
        user3.put("favorite_number", 42);

        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
        ByteArrayOutputStream baos1 = new ByteArrayOutputStream();
        ByteArrayOutputStream baos2 = new ByteArrayOutputStream();
        ByteArrayOutputStream baos3 = new ByteArrayOutputStream();

        BinaryEncoder encoder = EncoderFactory.get().blockingBinaryEncoder(baos1, null);
        datumWriter.write(user1, encoder);
        encoder.flush();
        messageWithTimestamp = new Message("test-topic", 0, 0, baos1.toByteArray());

        encoder = EncoderFactory.get().blockingBinaryEncoder(baos2, encoder);
        datumWriter.write(user2, encoder);
        encoder.flush();
        messageWithoutTimestamp = new Message("test-topic", 0, 0, baos2.toByteArray());

        encoder = EncoderFactory.get().blockingBinaryEncoder(baos3, encoder);
        datumWriter.write(user3, encoder);
        encoder.flush();
        messageWithBadTimestamp = new Message("test-topic", 0, 0, baos3.toByteArray());
    }

    @Test
    public void testExtractTimestampMillis() throws Exception {
        assertEquals(timestamp, parser.extractTimestampMillis(messageWithTimestamp));
        assertEquals(0L, parser.extractTimestampMillis(messageWithoutTimestamp));
        assertEquals(0L, parser.extractTimestampMillis(messageWithBadTimestamp));
    }
}
