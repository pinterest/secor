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
package com.pinterest.secor.common.monitoring;

import com.sun.net.httpserver.HttpExchange;
import com.twitter.ostrich.admin.CustomHttpHandler;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

/**
 * Initializes Http Endpoint for Prometheus
 *
 * @author Paulius Dambrauskas (p.dambrauskas@gmail.com)
 */
public class PrometheusHandler extends CustomHttpHandler {
    private static final Logger LOG = LoggerFactory.getLogger(PrometheusHandler.class);

    @Override
    public void handle(HttpExchange exchange) {
        Optional<PrometheusMeterRegistry> registry = Metrics.globalRegistry.getRegistries().stream()
                .filter(meterRegistry -> meterRegistry instanceof PrometheusMeterRegistry)
                .map(meterRegistry -> (PrometheusMeterRegistry) meterRegistry)
                .findFirst();
        if (registry.isPresent()) {
            this.render(registry.get().scrape(), exchange, HttpStatus.SC_OK);
        } else {
            LOG.warn("Trying to scrape prometheus, while it is disabled, " +
                    "set \"secor.monitoring.metrics.collector.micrometer.prometheus.enabled\" to \"true\"");
            this.render("Not Found", exchange, HttpStatus.SC_NOT_FOUND);
        }
    }
}
