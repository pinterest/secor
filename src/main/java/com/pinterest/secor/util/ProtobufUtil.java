package com.pinterest.secor.util;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import org.apache.commons.io.Charsets;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.Message;
import com.pinterest.secor.common.SecorConfig;

/**
 * Various utilities for working with protocol buffer encoded messages. This
 * utility will look for protobuf class in the configuration. It can be either
 * per Kafka topic configuration, for example:
 * 
 * <code>secor.protobuf.message.class.&lt;topic&gt;=&lt;protobuf class name&gt;</code>
 * 
 * or, it can be global configuration for all topics (in case all the topics
 * transfer the same message type):
 * 
 * <code>secor.protobuf.message.class.*=&lt;protobuf class name&gt;</code>
 * 
 * @author Michael Spector (spektom@gmail.com)
 */
public class ProtobufUtil {

    private static final Logger LOG = LoggerFactory.getLogger(ProtobufUtil.class);
    private static String JSON = "json";
    private boolean allTopics;
    private Map<String, Class<? extends Message>> messageClassByTopic = new HashMap<String, Class<? extends Message>>();
    private Map<String, String> messageFormatByTopic;
    private Map<String, Method> messageParseMethodByTopic = new HashMap<String, Method>();
    private Class<? extends Message> messageClassForAll;
    private String messageFormatForAll;
    private Method messageParseMethodForAll;

    /**
     * Creates new instance of {@link ProtobufUtil}
     * 
     * @param config
     *            Secor configuration instance
     * @throws RuntimeException
     *             when configuration option
     *             <code>secor.protobuf.message.class</code> is invalid.
     */
    @SuppressWarnings("unchecked")
    public ProtobufUtil(SecorConfig config) {
        Map<String, String> messageClassPerTopic = config.getProtobufMessageClassPerTopic();

        messageFormatByTopic = config.getMessageFormatPerTopic();
        for (Entry<String, String> entry : messageFormatByTopic.entrySet()) {
            String topic = entry.getKey();
            String format = entry.getValue();
            if("*".equals(topic)) {
                messageFormatForAll = format;
                LOG.info("Assuming the format: {} for all Kafka topics", format);
                break;
            } else {
                messageFormatByTopic.putIfAbsent(topic, format);
                LOG.info("Assuming the format: {} for Kafka topic {}", format, topic);
            }
        }

        for (Entry<String, String> entry : messageClassPerTopic.entrySet()) {
            try {
                String topic = entry.getKey();
                Class<? extends Message> messageClass = (Class<? extends Message>) Class.forName(entry.getValue());
                Method messageParseMethod = messageClass.getDeclaredMethod("parseFrom",
                        new Class<?>[] { byte[].class });

                allTopics = "*".equals(topic);


                if (allTopics) {
                    messageClassForAll = messageClass;
                    messageParseMethodForAll = messageParseMethod;
                    LOG.info("Using protobuf message class: {} for all Kafka topics", messageClass.getName());
                } else {
                    messageClassByTopic.put(topic, messageClass);
                    messageParseMethodByTopic.put(topic, messageParseMethod);
                    LOG.info("Using protobuf message class: {} for Kafka topic: {}", messageClass.getName(), topic);
                }
            } catch (ClassNotFoundException e) {
                LOG.error("Unable to load protobuf message class", e);
            } catch (NoSuchMethodException e) {
                LOG.error("Unable to find parseFrom() method in protobuf message class", e);
            } catch (SecurityException e) {
                LOG.error("Unable to use parseFrom() method from protobuf message class", e);
            }
        }
    }

    /**
     * Returns whether there was a protobuf class configuration
     */
    public boolean isConfigured() {
        return allTopics || !messageClassByTopic.isEmpty();
    }

    /**
     * Returns configured protobuf message class for the given Kafka topic
     * 
     * @param topic
     *            Kafka topic
     * @return protobuf message class used by this utility instance, or
     *         <code>null</code> in case valid class couldn't be found in the
     *         configuration.
     */
    public Class<? extends Message> getMessageClass(String topic) {
        return allTopics ? messageClassForAll : messageClassByTopic.get(topic);
    }

    /**
     * Decodes protobuf message
     *
     * @param topic
     *            Kafka topic name
     * @param payload
     *            Byte array containing encoded json message
     * @return protobuf message instance
     * @throws RuntimeException
     *             when there's problem decoding protobuf message
     */
    private static final JsonFormat.Parser jsonParser = JsonFormat.parser().ignoringUnknownFields();
    public Message decodeJsonMessage(String topic, byte[] payload) throws InvalidProtocolBufferException {
        try {
            Method builderGetter = allTopics ? messageClassForAll.getDeclaredMethod("newBuilder") : messageClassByTopic.get(topic).getDeclaredMethod("newBuilder");
            com.google.protobuf.GeneratedMessageV3.Builder builder = (com.google.protobuf.GeneratedMessageV3.Builder) builderGetter.invoke(null);
            jsonParser.merge(new InputStreamReader(new ByteArrayInputStream(payload)), builder);
            return builder.build();
        } catch (InvalidProtocolBufferException e){
            throw e;
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException("Error parsing JSON message", e);
        } catch (IOException e) {
            throw new RuntimeException("Error creating read stream for JSON message", e);
        }
    }

    /**
     * Decodes protobuf message
     *
     * @param topic
     *            Kafka topic name
     * @param payload
     *            Byte array containing encoded protobuf
     * @return protobuf message instance
     * @throws RuntimeException
     *             when there's problem decoding protobuf
     */
    public Message decodeProtobufMessage(String topic, byte[] payload){
        Method parseMethod = allTopics ? messageParseMethodForAll : messageParseMethodByTopic.get(topic);
        try {
            return (Message) parseMethod.invoke(null, payload);
        } catch (IllegalArgumentException e) {
            throw new RuntimeException("Can't parse protobuf message, since parseMethod() is not callable. "
                    + "Please check your protobuf version (this code works with protobuf >= 2.6.1)", e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Can't parse protobuf message, since parseMethod() is not accessible. "
                    + "Please check your protobuf version (this code works with protobuf >= 2.6.1)", e);
        } catch (InvocationTargetException e) {
            throw new RuntimeException("Error parsing protobuf message", e);
        }
    }

    /**
     * Decodes protobuf message
     * If the secor.topic.message.format property is set to "JSON" for "topic" assume "payload" is JSON
     *
     * @param topic
     *            Kafka topic name
     * @param payload
     *            Byte array containing encoded protobuf or JSON message
     * @return protobuf message instance
     * @throws RuntimeException
     *             when there's problem decoding protobuf or JSON message
     */
    public Message decodeProtobufOrJsonMessage(String topic, byte[] payload) {
        try {
            if (shouldDecodeFromJsonMessage(topic)) {
                return decodeJsonMessage(topic, payload);
            }
        } catch (InvalidProtocolBufferException e) {
            //When trimming files, the Uploader will read and then decode messages in protobuf format
            LOG.debug("Unable to translate JSON string {} to protobuf message", new String(payload, Charsets.UTF_8));
        }
        return decodeProtobufMessage(topic, payload);
    }

    private boolean shouldDecodeFromJsonMessage(String topic){
        if (StringUtils.isNotEmpty(messageFormatForAll) && StringUtils.equalsIgnoreCase(messageFormatForAll, JSON)) {
            return true;
        } else if (StringUtils.equalsIgnoreCase(messageFormatByTopic.getOrDefault(topic, ""), JSON)) {
            return true;
        }
        return false;
    }
}
