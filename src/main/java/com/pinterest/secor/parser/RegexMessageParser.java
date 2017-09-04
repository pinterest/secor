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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * RegexMessageParser extracts timestamp field (specified by 'message.timestamp.input.pattern')
 * The pattern specifies the regular exp to extract the timestamp field from a free-text line.
 *
 *  * @author Henry Cai (hcai@pinterest.com)
 */
public class RegexMessageParser extends TimestampedMessageParser {
    private static final Logger LOG = LoggerFactory.getLogger(RegexMessageParser.class);

    private final Pattern mTsPattern;

    public RegexMessageParser(SecorConfig config) {
        super(config);
        String patStr = config.getMessageTimestampInputPattern();
        LOG.info("timestamp pattern: {}", patStr);
        mTsPattern = Pattern.compile(patStr, Pattern.UNIX_LINES);
    }

    @Override
    public long extractTimestampMillis(final Message message) {
        String line = new String(message.getPayload());
        Matcher m = mTsPattern.matcher(line);
        if (m.find()) {
            String tsValue = m.group(1);
            if (tsValue != null) {
                return toMillis(Long.parseLong(tsValue));
            }
        }
        throw new NumberFormatException("Cannot find timestamp field in: " + line);
    }
}
