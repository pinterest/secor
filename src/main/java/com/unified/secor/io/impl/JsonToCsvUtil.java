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

package com.unified.secor.io.impl;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.unified.utils.csv.CSVWriter;
import com.unified.utils.json.JSONParser;

public class JsonToCsvUtil {
    private static final Logger     LOG         = LoggerFactory.getLogger(JsonToCsvUtil.class);
    private static final JSONParser JSON_PARSER = new JSONParser();
    private static final String     TAG         = "tag";

    public static void convertToTsv (String jsonString, CSVWriter writer, Map<String, List<String>> tagToColumns)
        throws IOException {
        JsonObject  jsonObject = JSON_PARSER.parseAsJsonObject(jsonString);
        JsonElement jsonTag    = jsonObject.get(TAG);
        String      tag;
        if ( jsonTag == null || ! jsonTag.isJsonPrimitive() ) {
            LOG.error("Message:[{}] is missing tag.", jsonString);
            return;
        }
        tag = jsonTag.getAsString();

        List<String> columns = tagToColumns.get(tag);

        if ( columns == null || columns.isEmpty() ) {
            LOG.error("No specification found for tag:{}", tag);
            return;
        }

        String [] csvColumns = new String[columns.size()];
        int i = 0;
        for (String column : columns) {
            JsonElement element = jsonObject.get(column);

            if (element == null) {
                LOG.error("Dropping message due to missing column:[{}]. Message:{}", column, jsonString);
                return;
            }
            if ( element.isJsonPrimitive() ) {
                csvColumns[i] = element.getAsString();
            }
            else if ( element.isJsonObject() ) {
                csvColumns[i] = element.toString();
            }
            else if ( element.isJsonArray() ) {
                JsonArray array = element.getAsJsonArray();
                if ( array.size() == 1 ) {
                    csvColumns[i] = array.get(0).getAsString();
                }
                else {
                    csvColumns[i] = element.toString();
                }
            }
            else if ( element.isJsonNull() ) {
                csvColumns[i] = element.toString();
            }
            else {
                LOG.error("Unknown Json type ({}). jsonString: {}", element, jsonString);
                return;
            }
            ++i;
        }
        writer.writeln(csvColumns);
    }
}
