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

import org.apache.orc.TypeDescription;

import com.pinterest.secor.common.LogFilePath;

/**
 * ORC schema provider interface
 * 
 * @author Ashish (ashu.impetus@gmail.com)
 *
 */
public interface ORCSchemaProvider {

    /**
     * This implementation should take a kafka topic name and returns ORC
     * schema. ORC schema should be in the form of TypeDescription
     * 
     * @param topic kafka topic
     * @param logFilePath It may require to figure out the schema
     * @return TypeDescription
     */
    public TypeDescription getSchema(String topic, LogFilePath logFilePath);

}
