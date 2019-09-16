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
package com.pinterest.secor.uploader;


import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.pinterest.secor.common.files.LogFilePath;
import com.pinterest.secor.common.SecorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Manages uploads to Microsoft Azure blob storage using Azure Storage SDK for java
 * https://github.com/azure/azure-storage-java
 *
 * @author Taichi Nakashima (nsd22843@gmail.com)
 *
 */
public class AzureUploadManager extends UploadManager {
    private static final Logger LOG = LoggerFactory.getLogger(AzureUploadManager.class);
    private static final ExecutorService executor = Executors.newFixedThreadPool(256);

    private CloudBlobClient blobClient;

    public AzureUploadManager(SecorConfig config) throws Exception {
        super(config);

        final String storageConnectionString =
                "DefaultEndpointsProtocol=" + mConfig.getAzureEndpointsProtocol() + ";" +
                "AccountName=" + mConfig.getAzureAccountName() + ";" +
                "AccountKey=" + mConfig.getAzureAccountKey() + ";";

        CloudStorageAccount storageAccount = CloudStorageAccount.parse(storageConnectionString);
        blobClient = storageAccount.createCloudBlobClient();
    }

    @java.lang.Override
    public Handle<?> upload(LogFilePath localPath) throws Exception {
        final String azureContainer = mConfig.getAzureContainer();
        final String azureKey = localPath.withPrefix(mConfig.getAzurePath()).getLogFilePath();
        final File localFile = new File(localPath.getLogFilePath());

        LOG.info("uploading file {} to azure://{}/{}", localFile, azureContainer, azureKey);
        final Future<?> f = executor.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    CloudBlobContainer container = blobClient.getContainerReference(azureContainer);
                    container.createIfNotExists();

                    CloudBlockBlob blob = container.getBlockBlobReference(azureKey);
                    blob.upload(new java.io.FileInputStream(localFile), localFile.length());

                } catch (URISyntaxException e) {
                    throw new RuntimeException(e);
                } catch (StorageException e) {
                    throw new RuntimeException(e);
                } catch (FileNotFoundException e) {
                    throw new RuntimeException(e);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        return new FutureHandle(f);
    }
}
