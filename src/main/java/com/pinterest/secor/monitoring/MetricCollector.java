package com.pinterest.secor.monitoring;

/**
 * Component which may be used to post metrics.
 *
 * All methods should be non-blocking and do not throw exceptions.
 */
public interface MetricCollector {
    /**
     * Increments the specified counter by one.
     * Convenience method equivalent to {@link #increment(String, int, String)}.
     *
     * @param label metric name
     * @param topic a tag which describes which topic this data is collected for
     */
    void increment(String label, String topic);

    /**
     * Adjusts the specified counter by a given delta
     *
     * @param label metric name
     * @param delta the amount to adjust the counter by
     * @param topic a tag which describes which topic this data is collected for
     */
    void increment(String label, int delta, String topic);

    /**
     * Used to track the statistical distribution of a set of values.
     * <p>
     * Metrics are collected by tracking the count, min, max, mean (average), and a simple bucket-based histogram of
     * the distribution. This distribution can be used to determine median, 90th percentile, etc.
     *
     * @param label metric name
     * @param value the value to be incorporated in the distribution
     * @param topic a tag which describes which topic this data is collected for
     */
    void metric(String label, double value, String topic);

    /**
     * Records the latest fixed value for the specified named gauge.
     *
     * @param label gauge name
     * @param value the new reading of the gauge
     * @param topic a tag which describes which topic this data is collected for
     */
    void gauge(String label, double value, String topic);
}
