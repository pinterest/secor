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
package com.pinterest.secor.common;

import org.junit.After;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class ShutdownHookRegistryTest {

    @After
    public void cleanup() {
        ShutdownHookRegistry.clear();
    }

    @Test
    public void testHookExecutionOrder() {
        List<String> results = new ArrayList<>();
        ShutdownHookRegistry.registerHook(9, () -> results.add("priority9"));
        ShutdownHookRegistry.registerHook(1, () -> results.add("priority1"));

        ShutdownHookRegistry.runHooks();

        assertEquals("priority1", results.get(0));
        assertEquals("priority9", results.get(1));
    }

}
