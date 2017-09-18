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
package com.pinterest.secor.uploader;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pinterest.secor.common.LogFilePath;
import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.util.FileUtil;

/**
 * Manages uploads to S3 using the Hadoop API.
 *
 * @author Pawel Garbacki (pawel@pinterest.com)
 */
public class HadoopS3UploadManager extends UploadManager {
    private static final Logger LOG = LoggerFactory.getLogger(HadoopS3UploadManager.class);

    protected static final ExecutorService executor = Executors.newFixedThreadPool(256);

    public HadoopS3UploadManager(SecorConfig config) {
        super(config);
    }

    public Handle<?> upload(LogFilePath localPath) throws Exception {
        String prefix = FileUtil.getPrefix(localPath.getTopic(), mConfig);
        LogFilePath path = localPath.withPrefix(prefix);
        final String localLogFilename = localPath.getLogFilePath();
        final String logFileName;

        if (FileUtil.s3PathPrefixIsAltered(path.getLogFilePath(), mConfig)) {
            logFileName = localPath.withPrefix(FileUtil.getS3AlternativePrefix(mConfig)).getLogFilePath();
            LOG.info("Will upload file to alternative s3 prefix path {}", logFileName);
        }
        else {
            logFileName = path.getLogFilePath();
        }

        LOG.info("uploading file {} to {}", localLogFilename, logFileName);

        final Future<?> f = executor.submit(new Runnable() {
            final int totalRetries = Integer.parseInt(mConfig.getUploaderRetries());
            final long backoffMillis = Long.parseLong(mConfig.getUploaderRetryBackoffMillis());
            @Override
            public void run() {
                IOException uploadException = null;
                long millisToSleep = backoffMillis;
                int retriesLeft = totalRetries;
                boolean success = false;
                while (retriesLeft >= 0 && !success) {
                    try {
                        FileUtil.moveToCloud(localLogFilename, logFileName);
                        success = true;
                    } catch (IOException e) {
                        success = false;
                        retriesLeft--;
                        uploadException = e;
                        LOG.warn("Error uploading files to s3. Will retry {} times. Exception {}", retriesLeft, e);
                        try {
                            LOG.info("Sleeping {} millis, will retry upload after sleep", millisToSleep);
                            Thread.sleep(millisToSleep);
                            millisToSleep = millisToSleep*2;
                        } catch(InterruptedException ie) {
                            LOG.info("Interrupted when sleeping for retry.");
                        }
                    }
                }
                if (!success)
                    throw new RuntimeException(uploadException);
            }
        });
        return new FutureHandle(f);
    }
}
