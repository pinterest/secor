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
package com.pinterest.secor.main;

import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.tools.ProgressMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Executors;

/**
 * Progress monitor main.
 *
 * Run:
 *     $ cd optimus/secor
 *     $ mvn package
 *     $ cd target
 *     $ java -ea -Dlog4j.configuration=log4j.dev.properties -Dconfig=secor.dev.backup.properties \
 *         -cp "secor-0.1-SNAPSHOT.jar:lib/*" com.pinterest.secor.main.ProgressMonitorMain
 *
 * @author Pawel Garbacki (pawel@pinterest.com)
 */
public class ProgressMonitorMain {
    private static final Logger LOG = LoggerFactory.getLogger(ProgressMonitorMain.class);

    private static void loop(ProgressMonitor progressMonitor, long interval) {
	final ProgressMonitor monitor = progressMonitor;
	Runnable runner = new Runnable() {
		public void run() {
		    try {
			monitor.exportStats();
		    } catch (Throwable t) {
			LOG.error("Progress monitor failed", t);
		    }
		}
	    };

	ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
	scheduler.scheduleAtFixedRate(runner, 0, interval, TimeUnit.SECONDS);
    }

    public static void main(String[] args) {
        try {
            SecorConfig config = SecorConfig.load();
            ProgressMonitor progressMonitor = new ProgressMonitor(config);

	    long interval = config.getMonitoringIntervalSeconds();
	    if (interval > 0) {
		loop(progressMonitor, interval);
	    } else {
		progressMonitor.exportStats();
	    }
        } catch (Throwable t) {
            LOG.error("Progress monitor failed", t);
            System.exit(1);
        }
    }
}
