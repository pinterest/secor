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
import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.io.FileReader;
import com.pinterest.secor.io.FileWriter;
import com.pinterest.secor.io.KeyValue;
import com.pinterest.secor.uploader.UploadManager;
import com.pinterest.secor.util.CompressionUtil;
import com.pinterest.secor.util.FileUtil;
import com.pinterest.secor.util.IdUtil;
import com.pinterest.secor.util.ReflectionUtil;

import java.io.File;
import java.nio.file.Paths;

import org.apache.hadoop.io.compress.CompressionCodec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages uploads using the configured upload manager
 * Converts the local files to the specified format before upload
 *
 * @author Jason Butterfield (jason.butterfield@outreach.io)
 */
public class FormatConvertingUploadManager extends UploadManager {
    private static final Logger LOG = LoggerFactory.getLogger(FormatConvertingUploadManager.class);

    private UploadManager mUploadManager;

    public FormatConvertingUploadManager(SecorConfig config) throws Exception {
        super(config);
        mUploadManager = createUploadManager();
    }

    @Override
    public TempFileUploadHandle<Handle<?>> upload(LogFilePath localPath) throws Exception {
        // convert the file from the internal format to the external format
        LogFilePath convertedFilePath = convertFile(localPath);

        // The TempFileUploadHandle will delete the temp file after it resolves
        return new TempFileUploadHandle(mUploadManager.upload(convertedFilePath), convertedFilePath);
    }

    private LogFilePath convertFile(LogFilePath srcPath) throws Exception {
        FileReader reader = null;
        FileWriter writer = null;
        int copiedMessages = 0;

        String localConvertedPrefix = Paths.get(mConfig.getLocalPath(), IdUtil.getLocalMessageDir(), "convertedForUpload").toString();
        LogFilePath convertedFilePath = srcPath.withPrefix(localConvertedPrefix);

        try {
            CompressionCodec codec = null;
            String extension = "";
            if (mConfig.getCompressionCodec() != null && !mConfig.getCompressionCodec().isEmpty()) {
                codec = CompressionUtil.createCompressionCodec(mConfig.getCompressionCodec());
                extension = codec.getDefaultExtension();
            }

            reader = createReader(srcPath, codec);
            writer = createWriter(convertedFilePath, codec);

            KeyValue keyVal;
            while ((keyVal = reader.next()) != null) {
              writer.write(keyVal);
              copiedMessages++;
            }
        } finally {
            if (reader != null) {
                reader.close();
            }
            if (writer != null) {
                writer.close();
            }
        }

        LOG.info("converted {} messages from {} to {}",
            copiedMessages,
            srcPath.getLogFilePath(),
            convertedFilePath.getLogFilePath()
        );

        return convertedFilePath;
    }

    /**
     * This method is intended to make mocking easier in tests.
     * @param srcPath source Path
     * @param codec compression codec
     * @return FileReader created file reader
     * @throws Exception on error
     */
    protected FileReader createReader(LogFilePath srcPath, CompressionCodec codec) throws Exception {
        return ReflectionUtil.createFileReader(
          mConfig.getFileReaderWriterFactory(),
          srcPath,
          codec,
          mConfig
        );
    }

    /**
     * This method is intended to make mocking easier in tests.
     * @param dstPath destination Path
     * @param codec compression codec
     * @return FileWriter created file writer
     * @throws Exception on error
     */
    protected FileWriter createWriter(LogFilePath dstPath, CompressionCodec codec) throws Exception {
        FileWriter writer = ReflectionUtil.createFileWriter(
          mConfig.getDestinationFileReaderWriterFactory(),
          dstPath,
          codec,
          mConfig
        );
        FileUtil.deleteOnExit(dstPath.getLogFilePath());
        FileUtil.deleteOnExit(dstPath.getLogFileCrcPath());
        return writer;
    }

    /**
     * This method is intended to make mocking easier in tests.
     * @return UploadManager created upload manager
     * @throws Exception on error
     */
    public UploadManager createUploadManager() throws Exception {
        return ReflectionUtil.createUploadManager(mConfig.getInnerUploadManagerClass(), mConfig);
    }
}
