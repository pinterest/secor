////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/*
 * Copyright © 2017 Unified Social, Inc.
 * 180 Madison Avenue, 23rd Floor, New York, NY 10016, U.S.A.
 * All rights reserved.
 *
 * This software (the "Software") is provided pursuant to the license agreement you entered into with Unified Social,
 * Inc. (the "License Agreement").  The Software is the confidential and proprietary information of Unified Social,
 * Inc., and you shall use it only in accordance with the terms and conditions of the License Agreement.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND "AS AVAILABLE."  UNIFIED SOCIAL, INC. MAKES NO WARRANTIES OF ANY KIND, WHETHER
 * EXPRESS OR IMPLIED, INCLUDING, BUT NOT LIMITED TO THE IMPLIED WARRANTIES AND CONDITIONS OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NON-INFRINGEMENT.
 */

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

package com.pinterest.secor.parser;

import java.util.LinkedHashMap;
import java.util.Map;

import org.joda.time.format.DateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.message.Message;
import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;

import org.joda.time.DateTime;



public class TimestampJsonPathParser extends MessageParser {

    private final boolean                       m_timestampRequired;
    protected LinkedHashMap<String, String> mFieldPrefixToJsonPathMap;

    private static final Configuration JSON_PATH_CONFIG = Configuration
        .defaultConfiguration()
        .addOptions(Option.DEFAULT_PATH_LEAF_TO_NULL)
        .addOptions(Option.SUPPRESS_EXCEPTIONS);

    private static final Logger LOG = LoggerFactory.getLogger(TimestampJsonPathParser.class);

    public TimestampJsonPathParser(SecorConfig config) {
        super(config);
        m_timestampRequired = config.isMessageTimestampRequired();
        mFieldPrefixToJsonPathMap = mConfig.getMessagePartitionFieldPrefixToJsonPathMap();
    }

    private long extractTimestamp (final Message message) {
        JSONObject jsonObject = (JSONObject) JSONValue.parse(message.getPayload());
        if (jsonObject != null) {
            Object fieldValue = getJsonFieldValue(jsonObject);
            if (fieldValue != null) {
                return toMillis(Double.valueOf(fieldValue.toString()).longValue());
            }
        } else if (m_timestampRequired) {
            throw new RuntimeException("Missing timestamp field for message: " + message);
        }
        return 0;
    }

    @Override
    public String[] extractPartitions (final Message message) throws Exception {
        JSONObject jsonObject = (JSONObject) JSONValue.parse(message.getPayload());
        if (jsonObject == null) {
            throw new RuntimeException("Failed to parse message as Json object");
        }

        mFieldPrefixToJsonPathMap.putAll(getTimeBasedPartitions(message));
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

    private LinkedHashMap<String, String> getTimeBasedPartitions (final Message message) {
        long recordTimestamp = extractTimestamp(message);
        DateTime dt = new DateTime(recordTimestamp);
        LinkedHashMap<String, String> timePartitons = new LinkedHashMap<String, String>();
        timePartitons.put("year", dt.toString(DateTimeFormat.forPattern("yyyy")));
        timePartitons.put("month", dt.toString(DateTimeFormat.forPattern("MM")));
        timePartitons.put("day", dt.toString(DateTimeFormat.forPattern("dd")));
        timePartitons.put("hour",  dt.toString(DateTimeFormat.forPattern("HH")));
        return timePartitons;
    }

    protected static long toMillis(final long timestamp) {
        final long nanosecondDivider = (long) Math.pow(10, 9 + 9);
        final long microsecondDivider = (long) Math.pow(10, 9 + 6);
        final long millisecondDivider = (long) Math.pow(10, 9 + 3);
        long timestampMillis;
        if (timestamp / nanosecondDivider > 0L) {
            timestampMillis = timestamp / (long) Math.pow(10, 6);
        } else if (timestamp / microsecondDivider > 0L) {
            timestampMillis = timestamp / (long) Math.pow(10, 3);
        } else if (timestamp / millisecondDivider > 0L) {
            timestampMillis = timestamp;
        } else {  // assume seconds
            timestampMillis = timestamp * 1000L;
        }
        return timestampMillis;
    }
}

