package com.pinterest.secor.io.impl;

import static org.junit.Assert.assertArrayEquals;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.commons.io.FileUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.modules.junit4.PowerMockRunner;

import com.google.common.io.Files;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Message;
import com.pinterest.secor.common.LogFilePath;
import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.io.FileReader;
import com.pinterest.secor.io.FileWriter;
import com.pinterest.secor.io.KeyValue;
import com.pinterest.secor.protobuf.Messages.UnitTestMessage3;
import com.pinterest.secor.util.OrcProtoUtil;
import com.pinterest.secor.util.ReflectionUtil;
import com.sun.tools.javac.util.Pair;

@RunWith(PowerMockRunner.class)
public class OrcProtoReaderWriterFactoryTest extends TestCase {

    private SecorConfig config;
	private String[] types = {"bool", "string", "int32", "float", "double", "int64"};
	private File tempFile;
	@Rule
	private TemporaryFolder folder = new TemporaryFolder(new File("src/test/config"));
	private OrcProtoUtil orcProtoUtil;
	
    @Override
    public void setUp() throws Exception {

    	config = Mockito.mock(SecorConfig.class);
    	tempFile = folder.newFile("testfile");
        Map<String, String> classPerTopic = new HashMap<String, String>();
        classPerTopic.put("test-orc-topic", DynamicMessage.class.getName());
        classPerTopic.put("test-pb-topic", UnitTestMessage3.class.getName());
    	Mockito.when(config.getOrcSchemaMapFile()).thenReturn(tempFile.getAbsolutePath());
    	Map<String, String> filePerTopic = new HashMap<String, String>();
    	filePerTopic.put("test-orc-topic", tempFile.getAbsolutePath());
        Mockito.when(config.getFileReaderWriterFactory())
            .thenReturn(OrcProtoReaderWriterFactory.class.getName());
        Mockito.when(config.getProtobufMessageClassPerTopic()).thenReturn(classPerTopic);
        Mockito.when(config.getOrcSchemaMapping()).thenReturn(filePerTopic);
        StringBuilder builder = new StringBuilder();
        for (int i=0; i<types.length; i++) {
        	if (i != 0)
        		builder.append("\n");
        	builder.append("A" + i + ":" + types[i]);
        }
        FileUtils.write(tempFile, builder.toString());
        builder.setLength(0);
        orcProtoUtil = new OrcProtoUtil(config);
    }

    /**
     * Method to test ORC read write round trip with schema file.
     * @throws Exception
     */
    @Test
    public void testORCDMReadWrite() throws Exception {
    	
    	LogFilePath tempLogFilePath = new LogFilePath(Files.createTempDir().toString(), "test-orc-topic",
        		new String[] { "part-1" }, 0, 1, 23232, ".log");
        FileWriter fileWriter = ReflectionUtil.createFileWriter(config.getFileReaderWriterFactory(), tempLogFilePath,
        		null, config);
        Pair<KeyValue, KeyValue> pair = dynamicMessageWriteTest("test-orc-topic", fileWriter);
        fileWriter.close();
        FileReader fileReader = ReflectionUtil.createFileReader(config.getFileReaderWriterFactory(), tempLogFilePath,
                null, config);
        KeyValue kvout = fileReader.next();
        assertEquals(pair.fst.getOffset(), kvout.getOffset());
        assertArrayEquals(pair.fst.getValue(), kvout.getValue());

        kvout = fileReader.next();
        assertEquals(pair.snd.getOffset(), kvout.getOffset());
        assertArrayEquals(pair.snd.getValue(), kvout.getValue());
        
    }
    
    /**
     * Method to generate key-values for dynamic-message tests.
     * @param topic
     * @param fileWriter
     * @return key-values generated for test 
     * @throws IOException
     */
    private Pair<KeyValue, KeyValue> dynamicMessageWriteTest(String topic, FileWriter fileWriter) throws IOException {

    	Object[] values = {false, "1", 2, 3.0f, 4.0d, 5l};
    	Object[] values2 = {true, "110", 210, 310.0f, 410.0d, 510l};
    	Message message = orcProtoUtil.getMessageInstance(topic);
        Message.Builder protoBuilder = message.toBuilder();
        List<FieldDescriptor> fieldsMap = message.getDescriptorForType().getFields();
        int i=0;
        for (FieldDescriptor field: fieldsMap) {
        	protoBuilder.setField(field, values[i]);
        	i++;
        }
        Message outputMessage = protoBuilder.build();
        
        long message1size = outputMessage.toByteArray().length;
        KeyValue kv1 = (new KeyValue(23232, outputMessage.toByteArray()));
        i=0;
        for (FieldDescriptor field: fieldsMap) {
        	protoBuilder.setField(field, values2[i]);
        	i++;
        }
        Message message2 = protoBuilder.build();
        long message2size = message2.toByteArray().length;
        KeyValue kv2 = (new KeyValue(23233, message2.toByteArray()));
        fileWriter.write(kv1);
        long size = fileWriter.getLength();
        assertEquals(message1size, size);
        fileWriter.write(kv2);
        size = fileWriter.getLength();
        assertEquals(message2size+message1size, size);
        return new Pair<KeyValue, KeyValue>(kv1, kv2);
        
    }
    
    /**
     * Method to test specified protobuf message class read-write cycle
     * @throws Exception
     */
    @Test
    public void testProtobufMessageOrcReadWrite() throws Exception {
    	
        LogFilePath tempLogFilePath = new LogFilePath(Files.createTempDir().toString(), "test-pb-topic",
                new String[] { "part-1" }, 0, 1, 23232, ".log");

        FileWriter fileWriter = ReflectionUtil.createFileWriter(config.getFileReaderWriterFactory(), tempLogFilePath,
                null, config);

        UnitTestMessage3 msg1 = UnitTestMessage3.newBuilder().setData("abc").setTimestamp(1467176315L).build();
        UnitTestMessage3 msg2 = UnitTestMessage3.newBuilder().setData("XYZ").setTimestamp(1467176344L).build();

        KeyValue kv1 = (new KeyValue(23232, msg1.toByteArray()));
        KeyValue kv2 = (new KeyValue(23233, msg2.toByteArray()));
        fileWriter.write(kv1);
        fileWriter.write(kv2);
        fileWriter.close();

        FileReader fileReader = ReflectionUtil.createFileReader(config.getFileReaderWriterFactory(), tempLogFilePath,
                null, config);

        KeyValue kvout = fileReader.next();
        assertEquals(kv1.getOffset(), kvout.getOffset());
        assertArrayEquals(kv1.getValue(), kvout.getValue());
        assertEquals(msg1.getData(), UnitTestMessage3.parseFrom(kvout.getValue()).getData());

        kvout = fileReader.next();
        assertEquals(kv2.getOffset(), kvout.getOffset());
        assertArrayEquals(kv2.getValue(), kvout.getValue());
        assertEquals(msg2.getData(), UnitTestMessage3.parseFrom(kvout.getValue()).getData());
    }
    /**
     * Method to test both file based dynamic message generation and protobuf message class generation.
     * @throws Exception
     */
    @Test
    public void testMultipleMessageOrcReadWrite() throws Exception {
    	
        testORCDMReadWrite();
        testProtobufMessageOrcReadWrite();
        
    }
    
    @Override
    protected void tearDown() throws Exception {
    	tempFile.delete();
    }
    
}
