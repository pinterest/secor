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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.DatatypeConverter;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Iso8601MessageParser extracts timestamp field (specified by 'message.timestamp.name')
 *
 * @author Jurriaan Pruis (email@jurriaanpruis.nl)
 *
 */
public class Iso8601MessageParser extends MessageParser {
    private static final Logger LOG = LoggerFactory.getLogger(Iso8601MessageParser.class);
    protected static final String defaultDate = "dt=1970-01-01";
    protected static final String defaultFormatter = "yyyy-MM-dd";
    protected static final SimpleDateFormat outputFormatter = new SimpleDateFormat(defaultFormatter);

    public Iso8601MessageParser(SecorConfig config) {
        super(config);
        outputFormatter.setTimeZone(config.getTimeZone());
    }

    @Override
    public String[] extractPartitions(Message message) throws Exception {
        JSONObject jsonObject = (JSONObject) JSONValue.parse(message.getPayload());
        String result[] = { defaultDate };

        if (jsonObject != null) {
            Object fieldValue = getJsonFieldValue(jsonObject);
            if (fieldValue == null) {
                LOG.warn("Missing field value. Using default partition = {}", defaultDate);
            } else {
                try {
                    Date dateFormat = DatatypeConverter.parseDateTime(fieldValue.toString()).getTime();
                    result[0] = "dt=" + outputFormatter.format(dateFormat);
                } catch (Exception e) {
                    LOG.warn("Impossible to convert date = {} as ISO-8601. Using date default = {}",
                            fieldValue.toString(), result[0]);
                }
            }
        }

        return result;
    }
}
