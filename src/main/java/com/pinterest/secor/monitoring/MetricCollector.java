package com.pinterest.secor.monitoring;

/**
 * Component which may be used to post metrics.
 *
 * All methods should be non-blocking and do not throw exceptions.
 */
public interface MetricCollector {
    /**
     * Convenience method equivalent to {@link #increment(String, int, String...)}.
     *
     * @param label metric name
     * @param tags array of tags to be added to the data
     */
    void increment(String label, String... tags);

    /**
     * Adjusts the specified counter by a given delta
     *
     * @param label metric name
     * @param delta the amount to adjust the counter by
     * @param tags array of tags to be added to the data
     */
    void increment(String label, int delta, String... tags);

    /**
     * Used to track the statistical distribution of a set of values.
     * <p>
     * Metrics are collected by tracking the count, min, max, mean (average), and a simple bucket-based histogram of
     * the distribution. This distribution can be used to determine median, 90th percentile, etc.
     *
     * @param label metric name
     * @param value the value to be incorporated in the distribution
     * @param tags  array of tags to be added to the data
     */
    void metric(String label, double value, String... tags);

    /**
     * Records the latest fixed value for the specified named gauge.
     *
     * @param label gauge name
     * @param value the new reading of the gauge
     * @param tags  array of tags to be added to the data
     */
    void gauge(String label, double value, String... tags);
}
