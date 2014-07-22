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

import com.pinterest.secor.common.OstrichAdminService;
import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.consumer.Consumer;
import com.pinterest.secor.tools.LogFileDeleter;
import com.pinterest.secor.util.FileUtil;
import com.pinterest.secor.util.RateLimitUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;

/**
 * Secor consumer.  See
 * https://docs.google.com/a/pinterest.com/document/d/1RHeH79O0e1WzsxumE24MIYqJFnRoRzQ3c74Wq3Q4R40/edit
 * for detailed design.
 *
 * Run:
 *     $ cd optimus/secor
 *     $ mvn package
 *     $ cd target
 *     $ java -ea -Dlog4j.configuration=log4j.dev.properties -Dconfig=secor.dev.backup.properties \
 *         -cp "secor-0.1-SNAPSHOT.jar:lib/*" com.pinterest.secor.main.ConsumerMain
 *
 * @author Pawel Garbacki (pawel@pinterest.com)
 */
public class ConsumerMain {
    private static final Logger LOG = LoggerFactory.getLogger(ConsumerMain.class);

    public static void main(String[] args) {
        if (args.length != 0) {
            System.err.println("Usage: java -Dconfig=<secor_properties> " +
                               "-Dlog4j.configuration=<log4j_properties> ConsumerMain");
            return;
        }
        try {
            SecorConfig config = SecorConfig.load();
            OstrichAdminService ostrichService = new OstrichAdminService(config.getOstrichPort());
            ostrichService.start();
            FileUtil.configure(config);

            LogFileDeleter logFileDeleter = new LogFileDeleter(config);
            logFileDeleter.deleteOldLogs();

            RateLimitUtil.configure(config);
            Thread.UncaughtExceptionHandler handler = new Thread.UncaughtExceptionHandler() {
                public void uncaughtException(Thread thread, Throwable exception) {
                    LOG.error("Thread " + thread + " failed", exception);
                    System.exit(1);
                }
            };
            LOG.info("starting " + config.getConsumerThreads() + " consumer threads");
            LinkedList<Consumer> consumers = new LinkedList<Consumer>();
            for (int i = 0; i < config.getConsumerThreads(); ++i) {
                Consumer consumer = new Consumer(config);
                consumer.setUncaughtExceptionHandler(handler);
                consumers.add(consumer);
                consumer.start();
            }
            for (Consumer consumer : consumers) {
                consumer.join();
            }
        } catch (Throwable t) {
            LOG.error("Consumer failed", t);
            System.exit(1);
        }
    }
}
