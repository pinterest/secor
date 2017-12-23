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
