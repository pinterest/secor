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

import com.pinterest.secor.common.LogFilePath;
import com.pinterest.secor.uploader.Handle;
import com.pinterest.secor.util.FileUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Wraps an Upload of a temp file
 * Deletes the temp file after the
 * wrapped Handle.get() resolves
 *
 * @author Jason Butterfield (jason.butterfield@outreach.io)
 */
public class TempFileUploadHandle<T> implements Handle<T> {
    private static final Logger LOG = LoggerFactory.getLogger(TempFileUploadHandle.class);

    private Handle<T> mHandle;
    private LogFilePath mPath;

    public TempFileUploadHandle(Handle<T> h, LogFilePath srcPath) {
        mHandle = h;
        mPath = srcPath;
    }

    public T get() throws Exception {
        T result = null;
        try {
            result = mHandle.get();
        } finally {
            FileUtil.delete(mPath.getLogFilePath());
            FileUtil.delete(mPath.getLogFileCrcPath());
            LOG.debug("deleting temp files {} and {}",
                mPath.getLogFilePath(),
                mPath.getLogFileCrcPath()
            );
        }

        return result;
    }

    public LogFilePath getTempFilePath() {
      return mPath;
    }
}
