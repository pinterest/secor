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
package com.pinterest.secor.util;

import com.google.common.util.concurrent.RateLimiter;
import com.pinterest.secor.common.SecorConfig;

/**
 * Rate limit util wraps around a rate limiter shared across consumer threads.  The rate limiting
 * mechanism does not prevent temporary bursts of the load on the broker caused by the message
 * prefetching mechanism of the native kafka client.
 *
 * @author Pawel Garbacki (pawel@pinterest.com)
 */
public class RateLimitUtil {
    private static RateLimiter mRateLimiter = null;

    public static void configure(SecorConfig config) {
        // Lazy initialization of the rate limiter would have to be in a synchronized block so
        // creating it here makes things simple.
        mRateLimiter = RateLimiter.create(config.getMessagesPerSecond());
    }

    public static void acquire() {
        mRateLimiter.acquire();
    }
}
