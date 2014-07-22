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
package com.pinterest.secor.tools;

import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.util.FileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

/**
 * Log file deleter removes message old log files stored locally.
 *
 * @author Pawel Garbacki (pawel@pinterest.com)
 */
public class LogFileDeleter {
    private static final Logger LOG = LoggerFactory.getLogger(LogFileDeleter.class);
    private SecorConfig mConfig;

    public LogFileDeleter(SecorConfig config) throws IOException {
        mConfig = config;
    }

    public void deleteOldLogs() throws Exception {
        if (mConfig.getLocalLogDeleteAgeHours() <= 0) {
            return;
        }
        String[] consumerDirs = FileUtil.list(mConfig.getLocalPath());
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");
        format.setTimeZone(TimeZone.getTimeZone("UTC"));
        for (String consumerDir : consumerDirs) {
            long modificationTime = FileUtil.getModificationTimeMsRecursive(consumerDir);
            String modificationTimeStr = format.format(modificationTime);
            LOG.info("Consumer log dir " + consumerDir + " last modified at " +
                    modificationTimeStr);
            final long localLogDeleteAgeMs =
                    mConfig.getLocalLogDeleteAgeHours() * 60L * 60L * 1000L;
            if (System.currentTimeMillis() - modificationTime > localLogDeleteAgeMs) {
                LOG.info("Deleting directory " + consumerDir + " last modified at " +
                        modificationTimeStr);
                FileUtil.delete(consumerDir);
            }
        }
    }
}
