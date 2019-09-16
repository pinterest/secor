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

import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.common.ZookeeperConnector;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ZookeeperConnectorTest {

    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void testGetCommittedOffsetGroupPath() throws Exception {
        verify("/", "/consumers/secor_cg/offsets");
        verify("/chroot", "/chroot/consumers/secor_cg/offsets");
        verify("/chroot/", "/chroot/consumers/secor_cg/offsets");
    }

    protected void verify(String zookeeperPath, String expectedOffsetPath) {
        ZookeeperConnector zookeeperConnector = new ZookeeperConnector();
        PropertiesConfiguration properties = new PropertiesConfiguration();
        properties.setProperty("kafka.zookeeper.path", zookeeperPath);
        properties.setProperty("secor.kafka.group", "secor_cg");
        SecorConfig secorConfig = new SecorConfig(properties);
        zookeeperConnector.setConfig(secorConfig);
        Assert.assertEquals(expectedOffsetPath, zookeeperConnector.getCommittedOffsetGroupPath());
    }
}