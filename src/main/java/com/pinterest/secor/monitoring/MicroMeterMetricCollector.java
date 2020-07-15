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
package com.pinterest.secor.monitoring;

import com.pinterest.secor.common.SecorConfig;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.jmx.JmxConfig;
import io.micrometer.jmx.JmxMeterRegistry;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.micrometer.statsd.StatsdConfig;
import io.micrometer.statsd.StatsdMeterRegistry;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MicorMeter meters can integrate with many different metrics backend 
 * (StatsD/Promethus/Graphite/JMX etc, see https://micrometer.io/docs)
 */
public class MicroMeterMetricCollector implements MetricCollector {
    private static final Logger LOG = LoggerFactory.getLogger(MicroMeterMetricCollector.class);

    private final Map<String, Double> mGaugeCache = new HashMap<>();
    private SecorConfig mConfig;

    @Override
    public void initialize(SecorConfig config) {
        mConfig = config;

        if (config.getMicroMeterCollectorStatsdEnabled()) {
            MeterRegistry statsdRegistry =
                new StatsdMeterRegistry(StatsdConfig.DEFAULT, Clock.SYSTEM);
            Metrics.addRegistry(statsdRegistry);
        }

        if (config.getMicroMeterCollectorJmxEnabled()) {
            MeterRegistry jmxRegistry = new JmxMeterRegistry(JmxConfig.DEFAULT, Clock.SYSTEM);
            Metrics.addRegistry(jmxRegistry);
        }

        if (config.getMicroMeterCollectorPrometheusEnabled()) {
            MeterRegistry prometheusRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
            Metrics.addRegistry(prometheusRegistry);
        }
    }

    @Override
    public void increment(String label, String topic) {
        Metrics.counter(label, Collections.singletonList(Tag.of("topic", topic))).increment();
    }

    @Override
    public void increment(String label, int delta, String topic) {
        Metrics.counter(label, Collections.singletonList(Tag.of("topic", topic))).increment(delta);
    }

    @Override
    public void metric(String label, double value, String topic) {
        gauge(label, value, topic);
    }

    @Override
    public void gauge(String label, double value, String topic) {
        String key = label + "_" + topic;
        if (!mGaugeCache.containsKey(key) && mGaugeCache.size() >= mConfig.getMicroMeterCacheSize()) {
            LOG.error("Gauge cache size reached maximum, this may result in inaccurate metrics, "
                    + "you can increase cache size by changing "
                    + "\"secor.monitoring.metrics.collector.micrometer.cache.size\" property.");
            return;
        }

        mGaugeCache.put(key, value);
        Metrics.gauge(label, Collections.singletonList(
                Tag.of("topic", topic)), mGaugeCache, g -> g.get(key));
    }

}
