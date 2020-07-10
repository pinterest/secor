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
package com.pinterest.secor.io;

import java.io.File;
import java.io.IOException;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runnable used to delete staging folder content.
 * Deletes folders content, while keeping folder itself.
 *
 * @author Paulius Dambrauskas (p.dambrauskas@gmail.com)
 *
 */
public class StagingDirectoryCleaner implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(StagingDirectoryCleaner.class);

    private final File mStagingDir;

    public StagingDirectoryCleaner(String stagingPath) {
        this.mStagingDir = new File(stagingPath);
    }

    @Override
    public void run() {
        try {
            FileUtils.deleteDirectory(this.mStagingDir);
        } catch (IOException e) {
            LOG.error("Failed deleting file", e);
        }
    }
}
