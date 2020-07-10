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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry for shutdown hooks.
 * Allows running shutdown hooks by specific order by executing multiple Runnables on single shutdown hook.
 *
 * @author Paulius Dambrauskas (p.dambrauskas@gmail.com)
 *
 */
public final class ShutdownHookRegistry {
    private static final Logger LOG = LoggerFactory.getLogger(ShutdownHookRegistry.class);
    private static final Map<Integer, List<Runnable>> HOOKS = new ConcurrentHashMap<>();

    private ShutdownHookRegistry() {
        // static class cannot be initiated
    }

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(ShutdownHookRegistry::runHooks));
    }

    public static void registerHook(int priority, Runnable hook) {
        HOOKS.computeIfAbsent(priority, key -> new ArrayList<>()).add(hook);
        LOG.info("Shut down hook with priority {} added to shut down hook registry", priority);
    }

    public static void runHooks() {
        HOOKS.entrySet().stream().sorted(Map.Entry.comparingByKey()).forEach(entry -> {
            LOG.info("Running hooks for priority {}", entry.getKey());
            entry.getValue().parallelStream().forEach(Runnable::run);
        });
    }

    public static void clear() {
        HOOKS.clear();
    }
}
