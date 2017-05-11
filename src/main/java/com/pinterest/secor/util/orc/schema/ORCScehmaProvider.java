package com.pinterest.secor.util.orc.schema;

import org.apache.orc.TypeDescription;

import com.pinterest.secor.common.LogFilePath;

/**
 * ORC schema provider interface
 * 
 * @author Ashish (ashu.impetus@gmail.com)
 *
 */
public interface ORCScehmaProvider {

    /**
     * This implementation should take a kafka topic name and returns ORC
     * schema. ORC schema should be in the form of TypeDescription
     * 
     * @param topic kafka topic
     * @param logFilePath It may require to figure out the schema
     * @return
     */
    public TypeDescription getSchema(String topic, LogFilePath logFilePath);

}
