package com.pinterest.secor.monitoring;

import com.twitter.ostrich.stats.Stats;

public class OstrichMetricCollector implements MetricCollector {
    @Override
    public void increment(String label, String topic) {
        Stats.incr(label);
    }

    @Override
    public void increment(String label, int delta, String topic) {
        Stats.incr(label, delta);
    }

    @Override
    public void metric(String label, double value, String topic) {
        Stats.addMetric(label, (int) value);
    }

    @Override
    public void gauge(String label, double value, String topic) {
        Stats.setGauge(label, value);
    }
}
