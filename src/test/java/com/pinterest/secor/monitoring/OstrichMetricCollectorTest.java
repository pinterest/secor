///*
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *   http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing,
// * software distributed under the License is distributed on an
// * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// * KIND, either express or implied.  See the License for the
// * specific language governing permissions and limitations
// * under the License.
// */
//package com.pinterest.secor.monitoring;
//
//import com.twitter.ostrich.stats.Stats;
//import org.junit.Before;
//import org.junit.Test;
//import org.junit.runner.RunWith;
//import org.powermock.api.mockito.PowerMockito;
//import org.powermock.core.classloader.annotations.PrepareForTest;
//import org.powermock.modules.junit4.PowerMockRunner;
//
//@RunWith(PowerMockRunner.class)
//@PrepareForTest({Stats.class})
//public class OstrichMetricCollectorTest {
//    private OstrichMetricCollector metricCollector = new OstrichMetricCollector();
//
//    @Before
//    public void setUp() throws Exception {
//        PowerMockito.mockStatic(Stats.class);
//    }
//
//    @Test
//    public void incrementByOne() throws Exception {
//        metricCollector.increment("expectedLabel", "ignored");
//
//        PowerMockito.verifyStatic();
//        Stats.incr("expectedLabel");
//    }
//
//    @Test
//    public void increment() throws Exception {
//        metricCollector.increment("expectedLabel", 42, "ignored");
//
//        PowerMockito.verifyStatic();
//        Stats.incr("expectedLabel", 42);
//    }
//
//    @Test
//    public void metric() throws Exception {
//        metricCollector.metric("expectedLabel", 42.0, "ignored");
//
//        PowerMockito.verifyStatic();
//        Stats.addMetric("expectedLabel", 42);
//    }
//
//    @Test
//    public void gauge() throws Exception {
//        metricCollector.gauge("expectedLabel", 4.2, "ignored");
//
//        PowerMockito.verifyStatic();
//        Stats.setGauge("expectedLabel", 4.2);
//    }
//}
