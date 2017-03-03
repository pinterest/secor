package com.pinterest.secor.monitoring;

import com.twitter.ostrich.stats.Stats;

public class OstrichMetricCollector implements MetricCollector {
    @Override
    public void increment(String label, String... tags) {
        Stats.incr(label);
    }

    @Override
    public void increment(String label, int delta, String... tags) {
        Stats.incr(label, delta);
    }

    @Override
    public void metric(String label, double value, String... tags) {
        Stats.addMetric(label, (int) value);
    }

    @Override
    public void gauge(String label, double value, String... tags) {
        Stats.setGauge(label, value);
    }
}
