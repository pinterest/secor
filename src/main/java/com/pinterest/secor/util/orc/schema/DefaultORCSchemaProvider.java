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
package com.pinterest.secor.util.orc.schema;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.orc.TypeDescription;

import com.pinterest.secor.common.LogFilePath;
import com.pinterest.secor.common.SecorConfig;

/**
 * Default implementation for ORC schema provider. It fetches ORC schemas from
 * configuration. User has to specify one schema per kafka topic or can have
 * same schema for all the topics.
 * 
 * @author Ashish (ashu.impetus@gmail.com)
 *
 */
public class DefaultORCSchemaProvider implements ORCSchemaProvider {

    private Map<String, TypeDescription> topicToSchemaMap;
    private TypeDescription schemaForAlltopic;

    public DefaultORCSchemaProvider(SecorConfig config) {
        topicToSchemaMap = new HashMap<String, TypeDescription>();
        setSchemas(config);
    }

    @Override
    public TypeDescription getSchema(String topic, LogFilePath logFilePath) {
        TypeDescription topicSpecificTD = topicToSchemaMap.get(topic);
        if (null != topicSpecificTD) {
            return topicSpecificTD;
        }
        return schemaForAlltopic;
    }

    /**
     * This method is used for fetching all ORC schemas from config
     * 
     * @param config
     */
    private void setSchemas(SecorConfig config) {
        Map<String, String> schemaPerTopic = config.getORCMessageSchema();
        for (Entry<String, String> entry : schemaPerTopic.entrySet()) {
            String topic = entry.getKey();
            TypeDescription schema = TypeDescription.fromString(entry
                    .getValue());
            topicToSchemaMap.put(topic, schema);
            // If common schema is given
            if ("*".equals(topic)) {
                schemaForAlltopic = schema;
            }
        }
    }
}
