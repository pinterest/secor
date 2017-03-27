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

package com.unified.secor.tools.schema;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

public class Message {
    protected static final String                  DATE_FORMAT = "yyyy/MM/dd HH:mm:ss SSS Z";
    protected static final ThreadLocal<DateFormat> FORMATTER   = new ThreadLocal<DateFormat>() {
        @Override
        protected DateFormat initialValue () {
            return new SimpleDateFormat(DATE_FORMAT);
        }
    };

    protected static String getDateFromTimestamp (long timestamp) {
        Date currentDate = new Date(timestamp);
        return Message.FORMATTER.get().format(currentDate);
    }


    public final String   messageType;
    public final String   network;
    public final String   jobInstanceId;
    public final String   tag;

    private final long   collectionTimestamp;
    private final String collectionDate;
    private final String collectorVersion;
    private final String apiVersion;

    /**
     * Time the event occurred, as opposed to the time the event was collected.
     */

    public Message (String   messageType,
             String   network,
             String   jobInstanceId,
             String   tag,
             String   collectorVersion,
             String   apiVersion) {
        this.messageType      = messageType;
        this.network          = network;
        this.jobInstanceId    = jobInstanceId;
        this.tag              = tag;
        this.collectorVersion = collectorVersion;
        this.apiVersion       = apiVersion;

        // Set the collection timestamp to now
        this.collectionTimestamp = DateTime.now(DateTimeZone.UTC).getMillis();
        this.collectionDate      = Message.getDateFromTimestamp(this.collectionTimestamp);
    }

}
