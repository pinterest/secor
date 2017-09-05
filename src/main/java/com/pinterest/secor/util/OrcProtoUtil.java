package com.pinterest.secor.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.TypeDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.os72.protobuf.dynamic.DynamicSchema;
import com.github.os72.protobuf.dynamic.MessageDefinition;
import com.github.os72.protobuf.dynamic.MessageDefinition.Builder;
import com.google.common.base.Throwables;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import com.pinterest.secor.common.SecorConfig;

public class OrcProtoUtil {

	private static final Logger LOG = LoggerFactory.getLogger(OrcProtoUtil.class);

	public static final String COLON_SEPARATOR = ":";
	public static final String COMMA_SEPARATOR = ",";
	public static final String STAR_SEPARATOR = "*";
	
	private boolean allTopics;
	
	private Map<String, TypeDescription> topicToOrcSchemaMap = new HashMap<String, TypeDescription>();
	private Map<String, Class<? extends Message>> topicToProtoMessageClassMap = new HashMap<String, Class<? extends Message>>();
	private Map<String, Message> topicToProtoMessageMap = new HashMap<String, Message>();
	private Map<String, Parser<? extends Message>> topicToMessageParserMap = new HashMap<String, Parser<? extends Message>>();
	private Map<String, String> topicSchemaMapping;

	public OrcProtoUtil(SecorConfig config) {
		topicSchemaMapping = config.getOrcSchemaMapping();
		Map<String, String> messageClassPerTopic = config.getProtobufMessageClassPerTopic();
        for (Entry<String, String> entry : messageClassPerTopic.entrySet()) {
            try {
                String topic = entry.getKey();
                Class<? extends Message> messageClass = 
                		Class.forName(entry.getValue()).asSubclass(Message.class);
                topicToProtoMessageClassMap.put(topic, messageClass);
                Message message = null;
                TypeDescription typeDescription = null;
                if (!messageClass.equals(DynamicMessage.class)) {
                	Method builderMethod = messageClass.getMethod("newBuilder", new Class[]{});
                	Message.Builder builder = (Message.Builder)builderMethod.invoke(null, (Object[])null);
                	message = builder.build();
                	typeDescription = convertFromProto(message);
                } else {
                	message = getMessageFromSchema(topic, topicSchemaMapping.get(topic));;
                	typeDescription = convertFromProto(message);
                }
                topicToProtoMessageMap.put(topic, message);
                topicToMessageParserMap.put(topic, message.getParserForType());
                topicToOrcSchemaMap.put(topic, typeDescription);
                allTopics = STAR_SEPARATOR.equals(topic);
                if (allTopics) {
                    LOG.info("Using protobuf message class: {} for all Kafka topics", message.getClass().getName());
                } 
            } catch (Exception e) {
                LOG.error("Unable to load protobuf message class", e);
                Throwables.propagate(e);
            } 
        }
	}

	/**
	 * Method to build protobuf message given topic and a filename containing schema for the dynamic message
	 * @param topic
	 * @param filename
	 * @return {@link Message}
	 */
	private Message getMessageFromSchema(String topic, String filename) {
		
		DynamicMessage message = null;
		DynamicSchema.Builder schemaBuilder = DynamicSchema.newBuilder();
		topic = StringUtils.capitalize(topic.replaceAll("-", " ")).replaceAll("\\s+", "");
		schemaBuilder.setName("SecorBuilder" + topic + ".proto");
		Builder builder = MessageDefinition.newBuilder(topic);
		try {
			BufferedReader reader = new BufferedReader(new FileReader(new File(filename)));
			String line;
			int i=0;
			while ((line = reader.readLine()) != null) {
				String[] splits = line.split(COLON_SEPARATOR);
				builder.addField("optional", splits[1], splits[0], ++i);
			}
			reader.close();
			MessageDefinition msgDef = builder.build();
			schemaBuilder.addMessageDefinition(msgDef);
			DynamicSchema schema = schemaBuilder.build();
			DynamicMessage.Builder msgBuilder = schema.newMessageBuilder(topic);
			message = msgBuilder.build();
		} catch (IOException e) {
			LOG.error("Unable to read schema for Orc due to {}, Orc conversion cannot work", e);
			Throwables.propagate(e);
		} catch (DescriptorValidationException e) {
			LOG.error("Unable to create protobuf messages for OrcScheme due to {}", e);
			Throwables.propagate(e);
		}
		return message;
		
	}

	/**
	 * Method to get orc-schema from protocol buffer message
	 * @param protoMessage
	 * @return {@link TypeDescription}
	 */
	private TypeDescription convertFromProto(Message protoMessage) {
		List<FieldDescriptor> fields = protoMessage.getDescriptorForType().getFields();
		StringBuilder stringBuilder = new StringBuilder();
		stringBuilder.append("struct<");
		for (int i=0; i<fields.size(); i++) {
			FieldDescriptor field = fields.get(i);
			if (i != 0) {
				stringBuilder.append(OrcUtil.COMMA_SEPARATOR);
			}
			switch(field.getJavaType()) {
			case BOOLEAN:
				String fieldName = field.getName();
				stringBuilder.append(fieldName + COLON_SEPARATOR + "boolean");
				break;
			case BYTE_STRING:
			case STRING:
				fieldName = field.getName();
				stringBuilder.append(fieldName + COLON_SEPARATOR + "string");
				break;
			case DOUBLE:
				fieldName = field.getName();
				stringBuilder.append(fieldName + COLON_SEPARATOR + "double");
				break;
			case FLOAT:
				fieldName = field.getName();
				stringBuilder.append(fieldName + COLON_SEPARATOR + "float");
				break;
			case INT:
				fieldName = field.getName();
				stringBuilder.append(fieldName + COLON_SEPARATOR + "int");
				break;
			case LONG:
				fieldName = field.getName();
				stringBuilder.append(fieldName + COLON_SEPARATOR + "bigint");
				break;
				default:
					Throwables.propagate(new IllegalArgumentException("Datatype not supported "+ field.getJavaType().name()));
			}
		}
		stringBuilder.append(">");
		return TypeDescription.fromString(stringBuilder.toString());
	}
	
	/**
	 * Method to populate data from protobuf to orc.
	 * @param protoMessage
	 * @param rowBatch
	 * @param row
	 */
	public void convertFromProtoToOrcType(Message protoMessage, VectorizedRowBatch rowBatch, int row) {
		List<FieldDescriptor> fields = protoMessage.getDescriptorForType().getFields();
		for (int i=0; i<fields.size(); i++) {
			FieldDescriptor field = fields.get(i);
			switch(field.getJavaType()) {
			case BOOLEAN:
				LongColumnVector col = (LongColumnVector) rowBatch.cols[i];
				col.vector[row] = ((Boolean)protoMessage.getField(field))?0:1;
				break;
			case BYTE_STRING:
			case STRING:
				BytesColumnVector bCol = (BytesColumnVector) rowBatch.cols[i];
				bCol.setVal(row, protoMessage.getField(field).toString().getBytes());
				break;
			case DOUBLE:
				DoubleColumnVector dCol = (DoubleColumnVector) rowBatch.cols[i];
				dCol.vector[row] = (Double)protoMessage.getField(field);
				break;
			case FLOAT:
				dCol = (DoubleColumnVector) rowBatch.cols[i];
				dCol.vector[row] = (Float)protoMessage.getField(field);
				break;
			case INT:
				col = (LongColumnVector) rowBatch.cols[i];
				col.vector[row] = (Integer)protoMessage.getField(field);
				break;
			case LONG:
				col = (LongColumnVector) rowBatch.cols[i];
				col.vector[row] = (Long)protoMessage.getField(field);
				break;
				default:
					Throwables.propagate(new IllegalArgumentException("Datatype not supported "+ field.getJavaType().name()));
			}
		}
	}
	
	/**
	 * Method to convert populate data from orc to protobuf  
	 * @param builder
	 * @param rowBatch
	 * @param row
	 * @return {@link Message}
	 */
	public Message convertFromOrcToProtoType(Message.Builder builder, VectorizedRowBatch rowBatch, int row) {
		builder.clear();
		List<FieldDescriptor> fields = builder.getDescriptorForType().getFields();
		for (int i=0; i<fields.size(); i++) {
			FieldDescriptor field = fields.get(i);
			switch(field.getJavaType()) {
			case BOOLEAN:
				LongColumnVector col = (LongColumnVector) rowBatch.cols[i];
				builder.setField(field, (col.vector[row] == 0)?true:false);
				break;
			case BYTE_STRING:
			case STRING:
				BytesColumnVector bCol = (BytesColumnVector) rowBatch.cols[i];
				builder.setField(field, bCol.toString(row));
				break;
			case DOUBLE:
				DoubleColumnVector dCol = (DoubleColumnVector) rowBatch.cols[i];
				builder.setField(field, dCol.vector[row]);
				break;
			case FLOAT:
				dCol = (DoubleColumnVector) rowBatch.cols[i];
				builder.setField(field, Float.parseFloat(Double.toString(dCol.vector[row])));
				break;
			case INT:
				col = (LongColumnVector) rowBatch.cols[i];
				builder.setField(field, Integer.parseInt(((Long)col.vector[row]).toString()));
				break;
			case LONG:
				col = (LongColumnVector) rowBatch.cols[i];
				builder.setField(field, (Long)col.vector[row]);
				break;
				default:
					Throwables.propagate(new IllegalArgumentException("Datatype not supported "+ field.getJavaType().name()));
			}
		}
		return builder.build();
	}
	
	/**
	 * Method to get the orc-schema based on topic
	 * @param topic
	 * @return {@link TypeDescription}
	 */
	public TypeDescription getTopicOrcSchema(String topic) {
		if (allTopics)
			topic = STAR_SEPARATOR;
		return topicToOrcSchemaMap.get(topic);
	}
	
	/**
	 * Method to get the default message instance based on topic 
	 * @param topic
	 * @return {@link Message}
	 */
	public Message getMessageInstance(String topic) {
		if (allTopics)
			topic = STAR_SEPARATOR;
		return topicToProtoMessageMap.get(topic);
	}
	
	/**
     * Decodes protobuf message
     * 
     * @param topic
     *            Kafka topic name
     * @param payload
     *            Byte array containing encoded protobuf message
     * @return protobuf message instance
     * @throws RuntimeException
     *             when there's problem decoding protobuf message
     */
    public Message decodeMessage(String topic, byte[] payload) {
        try {
        	if (allTopics)
        		topic = STAR_SEPARATOR;
        	Parser<? extends Message> parser = topicToMessageParserMap.get(topic);
        	return parser.parseFrom(payload);
        } catch (IllegalArgumentException e) {
            throw new RuntimeException("Can't parse protobuf message."
                    + "Please check your protobuf version (this code works with protobuf >= 2.6.1)", e);
        } catch (InvalidProtocolBufferException e) {
        	LOG.error("Unable to parse payload due to {}", e);
        	Throwables.propagate(e);
		}
        return null;
    }
	
}
