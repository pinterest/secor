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
package com.pinterest.secor.transformer;

import com.pinterest.secor.message.Message;

/**
 * Message transformer And filter Interface
 * 
 * @author Ashish (ashu.impetus@gmail.com)
 *
 */
public interface MessageTransformer {
    /**
     * Implement this method to add transformation logic at message level or filter out before
     * dumping it into Amazon S3
     * @param message message
     * @return The transformed message or null if message should not be dumped to storage.
     */
    public Message transform(Message message);
}
