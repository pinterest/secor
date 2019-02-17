/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.pinterest.secor.util;

import com.google.api.client.util.BackOff;
import com.google.api.client.util.ExponentialBackOff;

import java.io.IOException;

/**
 * @author Luke Sun (luke.skywalker.sun@gmail.com)
 */
public class BackOffUtil {

    private int count;
    private long lastBackOff;
    private BackOff backOff;

    public BackOffUtil() {
        this(true);
    }

    public BackOffUtil(boolean isExponential) {
        if (isExponential) {
            // aggressive
            backOff = new ExponentialBackOff.Builder().setInitialIntervalMillis(2)
                                                      .setMaxElapsedTimeMillis(Integer.MAX_VALUE)
                                                      .setMaxIntervalMillis(Integer.MAX_VALUE)
                                                      .setMultiplier(1.5)
                                                      .setRandomizationFactor(0)
                                                      .build();
        } else {
            // conservative
            backOff = new FixedBackOff();
        }
    }

    public BackOffUtil(int fixedBackOffInterval) {
        backOff = new FixedBackOff(fixedBackOffInterval);
    }

    public int getCount() {
        return count;
    }

    public long getLastBackOff() {
        return lastBackOff;
    }

    public boolean isBackOff() {
        count++;
        if (count > lastBackOff) {
            try {
                lastBackOff = backOff.nextBackOffMillis();
            } catch (IOException e) {
                // we don't care IOException here
            }
            return false;
        } else {
            return true;
        }
    }

    public class FixedBackOff implements BackOff {

        private final long max = Long.MAX_VALUE;
        private long interval;
        private int inc;

        public FixedBackOff() {
            this(5);
        }

        public FixedBackOff(int inc) {
            this.inc = inc;
        }

        @Override
        public void reset() {
            interval = 0;
        }

        @Override
        public long nextBackOffMillis() {
            if (interval < max) {
                interval += inc;
                if (interval < 0) {
                    interval = max;
                }
                return interval;
            } else {
                return max;
            }
        }
    }
}
