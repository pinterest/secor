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
package com.pinterest.secor.parser;

import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.message.Message;
import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;

import java.text.SimpleDateFormat;

/**
 * Custom parser for Barricade JSON messages
 */
public class BarricadeJsonMessageParser extends MessageParser {
    private static final String DAY_FORMAT = "yyyy-MM-dd";
    private static final String HOUR_FORMAT = "HH";

    public BarricadeJsonMessageParser(SecorConfig config) {
        super(config);
    }

    @Override
    public String[] extractPartitions(Message message) throws Exception {
        Object jsonObj = JSONValue.parse(message.getPayload());
        if (jsonObj instanceof JSONObject) {
            JSONObject obj = (JSONObject) JSONValue.parse(message.getPayload());
            if (obj != null) {
                String license = obj.get("license").toString();
                String uuid = obj.get("server_id").toString().replace(":", "");
                SimpleDateFormat dayFormatter = new SimpleDateFormat(DAY_FORMAT);
                SimpleDateFormat hourFormatter = new SimpleDateFormat(HOUR_FORMAT);
                // note: JSON message doesn't have packet capture time as a field
                // so use current time. This'll be addressed soon when we switch
                // to protobufs and use client time.
                long now = System.currentTimeMillis();
                return new String[]{dayFormatter.format(now), uuid, license, "hour=" + hourFormatter.format(now)};
            }
        }
        return new String[]{"default"};
    }
}
