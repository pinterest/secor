/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.pinterest.secor.common;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import com.pinterest.secor.util.StatsUtil;
import com.twitter.ostrich.admin.*;
import com.twitter.util.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.collection.Map$;
import scala.collection.immutable.List;
import scala.collection.immutable.List$;
import scala.collection.immutable.Nil$;
import scala.util.matching.Regex;

/**
 * OstrichAdminService initializes export of metrics to Ostrich.
 *
 * @author Pawel Garbacki (pawel@pinterest.com)
 */
public class OstrichAdminService {
    private static final Logger LOG = LoggerFactory.getLogger(OstrichAdminService.class);
    private final int mPort;

    public OstrichAdminService(int port) {
        this.mPort = port;
    }

    public void start() {
        @SuppressWarnings("deprecation")
        AdminServiceFactory adminServiceFactory = new AdminServiceFactory(
            this.mPort,
            20,
            List$.MODULE$.<StatsFactory>empty(),
            Option.<String>empty(),
            List$.MODULE$.<Regex>empty(),
            Map$.MODULE$.<String, CustomHttpHandler>empty(),
            Nil$.MODULE$.$colon$colon(Duration.apply(1, TimeUnit.MINUTES))
        );
        RuntimeEnvironment runtimeEnvironment = new RuntimeEnvironment(this);
        adminServiceFactory.apply(runtimeEnvironment);
        try {
            Properties properties = new Properties();
            properties.load(this.getClass().getResource("build.properties").openStream());
            String buildRevision = properties.getProperty("build_revision", "unknown");
            LOG.info("build.properties build_revision: {}",
                     properties.getProperty("build_revision", "unknown"));
            StatsUtil.setLabel("secor.build_revision", buildRevision);
        } catch (Throwable t) {
            LOG.error("Failed to load properties from build.properties", t);
        }
    }
}
