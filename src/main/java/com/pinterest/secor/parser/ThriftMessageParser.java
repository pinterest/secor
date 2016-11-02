/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.pinterest.secor.parser;

import org.apache.commons.lang3.StringUtils;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;

import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.message.Message;

/**
 * Thrift message parser extracts date partitions from thrift messages.
 *
 * @author Pawel Garbacki (pawel@pinterest.com)
 */
public class ThriftMessageParser extends TimestampedMessageParser {
    private final TDeserializer mDeserializer;
    private final ThriftPath mThriftPath;
    private final String mTimestampType;

    class ThriftPath implements TFieldIdEnum {
        private final String mFieldName;
        private final short mFieldId;

        public ThriftPath(final String fieldName, final short fieldId) {
            this.mFieldName = fieldName;
            this.mFieldId = fieldId;
        }

        @Override
        public short getThriftFieldId() {
            return mFieldId;
        }

        @Override
        public String getFieldName() {
            return mFieldName;
        }
    }

    public ThriftMessageParser(SecorConfig config)
            throws InstantiationException, IllegalAccessException,
            ClassNotFoundException {
        super(config);
        TProtocolFactory protocolFactory = null;
        String protocolName = mConfig.getThriftProtocolClass();
        
        if (StringUtils.isNotEmpty(protocolName)) {
            String factoryClassName = protocolName.concat("$Factory");
            protocolFactory = ((Class<? extends TProtocolFactory>) Class.forName(factoryClassName)).newInstance();
        } else
            protocolFactory = new TBinaryProtocol.Factory();
        
        mDeserializer = new TDeserializer(protocolFactory);
        mThriftPath = new ThriftPath(mConfig.getMessageTimestampName(),(short) mConfig.getMessageTimestampId());
        mTimestampType = mConfig.getMessageTimestampType();
    }

    @Override
    public long extractTimestampMillis(final Message message) throws TException {
        long timestamp;
        if ("i32".equals(mTimestampType)) {
            timestamp = (long) mDeserializer.partialDeserializeI32(message.getPayload(), mThriftPath);
        } else {
            timestamp = mDeserializer.partialDeserializeI64(message.getPayload(), mThriftPath);
        }

        return toMillis(timestamp);
    }
}
