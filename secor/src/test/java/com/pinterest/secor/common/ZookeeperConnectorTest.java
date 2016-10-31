package com.pinterest.secor.common;

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