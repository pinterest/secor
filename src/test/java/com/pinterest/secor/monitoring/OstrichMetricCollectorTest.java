package com.pinterest.secor.monitoring;

import com.twitter.ostrich.stats.Stats;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({Stats.class})
public class OstrichMetricCollectorTest {
    private OstrichMetricCollector metricCollector = new OstrichMetricCollector();

    @Before
    public void setUp() throws Exception {
        PowerMockito.mockStatic(Stats.class);
    }

    @Test
    public void incrementByOne() throws Exception {
        metricCollector.increment("expectedLabel", "ignored");

        PowerMockito.verifyStatic();
        Stats.incr("expectedLabel");
    }

    @Test
    public void increment() throws Exception {
        metricCollector.increment("expectedLabel", 42, "ignored");

        PowerMockito.verifyStatic();
        Stats.incr("expectedLabel", 42);
    }

    @Test
    public void metric() throws Exception {
        metricCollector.metric("expectedLabel", 42.0, "ignored");

        PowerMockito.verifyStatic();
        Stats.addMetric("expectedLabel", 42);
    }

    @Test
    public void gauge() throws Exception {
        metricCollector.gauge("expectedLabel", 4.2, "ignored");

        PowerMockito.verifyStatic();
        Stats.setGauge("expectedLabel", 4.2);
    }
}
