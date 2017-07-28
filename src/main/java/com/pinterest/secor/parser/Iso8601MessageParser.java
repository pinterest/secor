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
import java.util.Date;

/**
 * Iso8601MessageParser extracts timestamp field (specified by 'message.timestamp.name')
 *
 * @author Jurriaan Pruis (email@jurriaanpruis.nl)
 *
 */
public class Iso8601MessageParser extends TimestampedMessageParser {
    private final boolean m_timestampRequired;

    public Iso8601MessageParser(SecorConfig config) {
        super(config);
        m_timestampRequired = config.isMessageTimestampRequired();
    }

    @Override
    public long extractTimestampMillis(final Message message) {
        JSONObject jsonObject = (JSONObject) JSONValue.parse(message.getPayload());
        Object fieldValue = jsonObject != null ? getJsonFieldValue(jsonObject) : null;

        if (m_timestampRequired && fieldValue == null) {
            throw new RuntimeException("Missing timestamp field for message: " + message);
        }

        if (fieldValue != null) {
            try {
                Date dateFormat = DatatypeConverter.parseDateTime(fieldValue.toString()).getTime();
                return dateFormat.getTime();
            } catch (IllegalArgumentException ex) {
                if (m_timestampRequired){
                    throw new RuntimeException("Bad timestamp field for message: " + message);
                }
            }
        }

        return 0;
    }
}
