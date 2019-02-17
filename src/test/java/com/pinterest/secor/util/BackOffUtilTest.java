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

import org.junit.Before;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;

/**
 * @author Luke Sun (luke.skywalker.sun@gmail.com)
 */
public class BackOffUtilTest {

    @Before
    public void setUp() throws Exception {}

    /**
     * Test ExponentialBackOff
     */
    @Test
    public void testExponentialBackOff() {
        BackOffUtil back = new BackOffUtil();
        for (long i = 0; i < Integer.MAX_VALUE + 1L; i++) {
            back.isBackOff();
            // RandomizationFactor 0
            if (back.getLastBackOff() <= back.getCount()) {
                assertEquals(back.getLastBackOff(), back.getCount());
            }
            // count overflow
            if (i >= Integer.MAX_VALUE) {
                assertEquals(Integer.MIN_VALUE, back.getCount());
            }
        }
    }

    /**
     * Test FixedBackOff
     */
    @Test
    public void testFixedBackOff() {
        int interval = 10;
        BackOffUtil back = new BackOffUtil(interval);
        // LastBackOff = loop count + interval iff not backoff
        for (long i = 0; i < 1000; i++) {
            if (!back.isBackOff()) {
                assertEquals(back.getLastBackOff(), i + interval);
            }
        }
    }
}
