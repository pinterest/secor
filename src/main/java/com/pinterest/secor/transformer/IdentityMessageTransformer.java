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

import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.message.Message;

/**
 * Default message transformer class which does no transformation
 *
 * @author Ashish (ashu.impetus@gmail.com)
 */
public class IdentityMessageTransformer implements MessageTransformer {

    protected SecorConfig mConfig;

    /**
     * Constructor
     *
     * @param config secor config
     */
    public IdentityMessageTransformer(SecorConfig config) {
        mConfig = config;
    }

    @Override
    public Message transform(Message message) {
        return message;
    }
}
