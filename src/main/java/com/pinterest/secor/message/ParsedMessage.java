/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.pinterest.secor.message;

import java.lang.String;
import java.util.Arrays;

/**
 * Parsed message is a Kafka message that has been processed by the parser that extracted its
 * partitions.
 *
 * @author Pawel Garbacki (pawel@pinterest.com)
 */
public class ParsedMessage extends Message {
    private String[] mPartitions;

    @Override
    public String toString() {
        return "ParsedMessage{" + fieldsToString() +  ", mPartitions=" +
               Arrays.toString(mPartitions) + '}';
    }

    public ParsedMessage(String topic, int kafkaPartition, long offset, byte[] payload,
                         String[] mPartitions) {
        super(topic, kafkaPartition, offset, payload);
        this.mPartitions = mPartitions;
    }

    public String[] getPartitions() {
        return mPartitions;
    }
}
