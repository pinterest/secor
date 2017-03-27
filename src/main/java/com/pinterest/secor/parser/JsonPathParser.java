////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/*
 * Copyright © 2017 Unified Social, Inc.
 * 180 Madison Avenue, 23rd Floor, New York, NY 10016, U.S.A.
 * All rights reserved.
 *
 * This software (the "Software") is provided pursuant to the license agreement you entered into with Unified Social,
 * Inc. (the "License Agreement").  The Software is the confidential and proprietary information of Unified Social,
 * Inc., and you shall use it only in accordance with the terms and conditions of the License Agreement.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND "AS AVAILABLE."  UNIFIED SOCIAL, INC. MAKES NO WARRANTIES OF ANY KIND, WHETHER
 * EXPRESS OR IMPLIED, INCLUDING, BUT NOT LIMITED TO THE IMPLIED WARRANTIES AND CONDITIONS OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NON-INFRINGEMENT.
 */

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

package com.pinterest.secor.parser;

import java.util.LinkedHashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.message.Message;
import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;

public class JsonPathParser extends MessageParser {

    protected LinkedHashMap<String, String> mFieldPrefixToJsonPathMap;

    private static final Configuration JSON_PATH_CONFIG = Configuration
        .defaultConfiguration()
        .addOptions(Option.DEFAULT_PATH_LEAF_TO_NULL)
        .addOptions(Option.SUPPRESS_EXCEPTIONS);

    private static final Logger LOG = LoggerFactory.getLogger(JsonPathParser.class);

    public JsonPathParser (SecorConfig config) {
        super(config);
        mFieldPrefixToJsonPathMap = mConfig.getMessagePartitionFieldPrefixToJsonPathMap();
    }

    @Override
    public String[] extractPartitions (final Message message) throws Exception {
        JSONObject jsonObject = (JSONObject) JSONValue.parse(message.getPayload());
        if (jsonObject == null) {
            throw new RuntimeException("Failed to parse message as Json object");
        }

        String[] partitions = new String[mFieldPrefixToJsonPathMap.size()];
        int i = 0;
        for (Map.Entry<String, String> entry : mFieldPrefixToJsonPathMap.entrySet()) {
            Object parsedJson = JsonPath.using(JSON_PATH_CONFIG).parse(jsonObject).read(entry.getValue());
            if (parsedJson != null) {
                partitions[i++] = entry.getKey() + parsedJson.toString();
            } else {
                throw new RuntimeException(
                    "Failed to extract jsonPath: [" + entry.getValue() + "] from the message" + message);
            }
        }
        return partitions;
    }
}
