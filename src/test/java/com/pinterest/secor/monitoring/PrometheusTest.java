package com.pinterest.secor.monitoring;

import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.common.monitoring.PrometheusHandler;
import com.sun.net.httpserver.HttpExchange;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class PrometheusTest {

    @Test
    public void testPrometheusIntegration() throws IOException {
        PropertiesConfiguration properties = new PropertiesConfiguration();
        properties.addProperty("secor.monitoring.metrics.collector.micrometer.prometheus.enabled", true);
        SecorConfig config = new SecorConfig(properties);
        MetricCollector collector = new MicroMeterMetricCollector();
        collector.initialize(config);

        final List<String> responses = new ArrayList<>();
        PrometheusHandler handler = new PrometheusHandler() {
            @Override
            public void render(String body, HttpExchange exchange, int code) {
                responses.add(body);
            }
        };
        HttpExchange exchange = mock(HttpExchange.class);

        collector.gauge("test", 1, "topic");

        handler.handle(exchange);
        assertTrue(responses.get(0).contains("test{topic=\"topic\",} 1.0"));
    }

}
