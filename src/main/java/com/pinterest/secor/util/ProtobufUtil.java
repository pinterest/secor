package com.pinterest.secor.util;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import com.google.protobuf.Message;
import com.pinterest.secor.common.SecorConfig;

/**
 * Various utilities for working with protocol buffer encoded messages.
 * 
 * @author Michael Spector (spektom@gmail.com)
 */
public class ProtobufUtil {

    private Class<? extends Message> messageClass;
    private Method messageParseMethod;

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
        String messageClassName = config.getProtobufMessageClass();
        try {
            messageClass = (Class<? extends Message>) Class.forName(messageClassName);
            messageParseMethod = messageClass.getDeclaredMethod("parseFrom", new Class<?>[] { byte[].class });
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Unable to load protobuf message class", e);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("Unable to find parseFrom() method in protobuf message class", e);
        } catch (SecurityException e) {
            throw new RuntimeException("Unable to use parseFrom() method from protobuf message class", e);
        }
    }

    /**
     * @return protobuf message class used by this utility instance
     */
    public Class<? extends Message> getMessageClass() {
        return messageClass;
    }

    /**
     * Decodes protobuf message
     * 
     * @param payload
     *            Byte array containing encoded protobuf message
     * @return protobuf message instance
     * @throws RuntimeException
     *             when there's problem decoding protobuf message
     */
    public Message decodeMessage(byte[] payload) {
        try {
            return (Message) messageParseMethod.invoke(null, payload);
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
}
