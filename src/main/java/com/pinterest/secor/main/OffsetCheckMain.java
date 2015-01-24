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
import com.pinterest.secor.tools.OffsetCheck;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tool that compares secor committed offsets to kafka offsets and lets you fix secor offsets
 * to earliest kafka offset.
 *
 * Run:
 *     $ cd secor
 *     $ mvn package
 *     $ cd target
 *     TO REPORT:
 *     $ java -ea -Dlog4j.configuration=log4j.dev.properties -Dconfig=secor.dev.backup.properties \
 *         -cp "secor-0.1-SNAPSHOT.jar:lib/*" com.pinterest.secor.main.OffsetCheckMain
 *     TO FIX:
 *     $ java -ea -Dlog4j.configuration=log4j.dev.properties -Dconfig=secor.dev.backup.properties \
 *         -cp "secor-0.1-SNAPSHOT.jar:lib/*" com.pinterest.secor.main.OffsetCheckMain --fix
 *
 * @author Ramki Venkatachalam (ramki@pinterest.com)
 */
public class OffsetCheckMain {
    private static final Logger LOG = LoggerFactory.getLogger(OffsetCheckMain.class);

    public static void main(String[] args) {
        boolean fix = false;
        if (args.length == 1 && args[0].contentEquals("--fix")) {
          fix = true;
        }
        try {
            SecorConfig config = SecorConfig.load();
            OffsetCheck offsetCheck = new OffsetCheck(config);
            offsetCheck.printOffsets(fix);
        } catch (Throwable t) {
            LOG.error("offset check failed", t);
            System.exit(1);
        }
    }
}
