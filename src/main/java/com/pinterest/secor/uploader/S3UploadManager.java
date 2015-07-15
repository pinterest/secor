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

import com.pinterest.secor.common.*;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.transfer.Upload;
import com.amazonaws.services.s3.transfer.TransferManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Manages uploads to S3 using the TransferManager class from the AWS
 * SDK.
 *
 * It will use the aws.access.key and aws.secret.key configuration
 * settings if they are non-empty; otherwise, it will use the SDK's
 * default credential provider chain (supports environment variables,
 * system properties, credientials file, and IAM credentials).
 *
 * @author Liam Stewart (liam.stewart@gmail.com)
 */
public class S3UploadManager extends UploadManager {
    private static final Logger LOG = LoggerFactory.getLogger(S3UploadManager.class);

    private TransferManager mManager;

    public S3UploadManager(SecorConfig config) {
        super(config);

        String accessKey = mConfig.getAwsAccessKey();
        String secretKey = mConfig.getAwsSecretKey();
        String endpoint = mConfig.getAwsEndpoint();
        String region = mConfig.getAwsRegion();
        AmazonS3 client;

        if (accessKey.isEmpty() || secretKey.isEmpty()) {
            client = new AmazonS3Client();
        } else {
            client = new AmazonS3Client(new BasicAWSCredentials(accessKey, secretKey));
        }

        if (!endpoint.isEmpty()) {
            client.setEndpoint(endpoint);
        } else if (!region.isEmpty()) {
            client.setRegion(Region.getRegion(Regions.fromName(region)));
        }

        mManager = new TransferManager(client);
    }

    public Handle<?> upload(LogFilePath localPath) throws Exception {
        String s3Bucket = mConfig.getS3Bucket();
        String s3Key = localPath.withPrefix(mConfig.getS3Path()).getLogFilePath();
        File localFile = new File(localPath.getLogFilePath());

        LOG.info("uploading file " + localFile + " to s3://" + s3Bucket + "/" + s3Key);

        Upload upload = mManager.upload(s3Bucket, s3Key, localFile);
        return new S3UploadHandle(upload);
    }
}
