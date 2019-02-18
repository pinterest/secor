/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.pinterest.secor.util;

import com.pinterest.secor.common.SecorConfig;
import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Adapted from ProtobufUtil Various utilities for working with thrift encoded messages. This utility will look for
 * thrift class in the configuration. It can be either per Kafka topic configuration, for example:
 *
 * <code>secor.thrift.message.class.<topic>=<thrift class name></code>
 * <p>
 * or, it can be global configuration for all topics (in case all the topics transfer the same message type):
 *
 * <code>secor.thrift.message.class.*=<thrift class name></code>
 *
 * @author jaime sastre (jaime sastre.s@gmail.com)
 */
public class ThriftUtil {

    private static final Logger LOG = LoggerFactory.getLogger(ThriftUtil.class);

    private boolean allTopics;
    @SuppressWarnings("rawtypes")
    private Map<String, Class<? extends TBase>> messageClassByTopic = new HashMap<String, Class<? extends TBase>>();
    @SuppressWarnings("rawtypes")
    private Class<? extends TBase> messageClassForAll;
    private TProtocolFactory messageProtocolFactory;

    /**
     * Creates new instance of {@link ThriftUtil}
     *
     * @param config Secor configuration instance
     * @throws RuntimeException when configuration option <code>secor.thrift.message.class</code> is invalid.
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public ThriftUtil(SecorConfig config) {
        Map<String, String> messageClassPerTopic = config.getThriftMessageClassPerTopic();

        for (Entry<String, String> entry : messageClassPerTopic.entrySet()) {
            try {
                String topic = entry.getKey();
                Class<? extends TBase> messageClass = (Class<? extends TBase>) Class.forName(entry.getValue());

                allTopics = "*".equals(topic);

                if (allTopics) {
                    messageClassForAll = messageClass;
                    LOG.info("Using thrift message class: {} for all Kafka topics", messageClass.getName());
                } else {
                    messageClassByTopic.put(topic, messageClass);
                    LOG.info("Using thrift message class: {} for Kafka topic: {}", messageClass.getName(), topic);
                }
            } catch (ClassNotFoundException e) {
                LOG.error("Unable to load thrift message class", e);
            }
        }

        try {
            String protocolName = config.getThriftProtocolClass();

            if (protocolName != null) {
                String factoryClassName = protocolName.concat("$Factory");
                messageProtocolFactory =
                        ((Class<? extends TProtocolFactory>) Class.forName(factoryClassName)).newInstance();
            } else {
                messageProtocolFactory = new TBinaryProtocol.Factory();
            }

        } catch (ClassNotFoundException e) {
            LOG.error("Unable to load thrift protocol class", e);
        } catch (InstantiationException e) {
            LOG.error("Unable to load thrift protocol class", e);
        } catch (IllegalAccessException e) {
            LOG.error("Unable to load thrift protocol class", e);
        }
    }

    /**
     * Returns configured thrift message class for the given Kafka topic
     *
     * @param topic Kafka topic
     * @return thrift message class used by this utility instance, or <code>null</code> in case valid class couldn't be
     * found in the configuration.
     */
    @SuppressWarnings("rawtypes")
    public Class<? extends TBase> getMessageClass(String topic) {
        return allTopics ? messageClassForAll : messageClassByTopic.get(topic);
    }

    @SuppressWarnings("rawtypes")
    public TBase decodeMessage(String topic, byte[] payload)
            throws InstantiationException, IllegalAccessException, TException {
        TDeserializer serializer = new TDeserializer(messageProtocolFactory);
        TBase result = this.getMessageClass(topic).newInstance();
        serializer.deserialize(result, payload);
        return result;
    }

    @SuppressWarnings("rawtypes")
    public byte[] encodeMessage(TBase object) throws InstantiationException, IllegalAccessException, TException {
        TSerializer serializer = new TSerializer(messageProtocolFactory);
        return serializer.serialize(object);
    }
}
