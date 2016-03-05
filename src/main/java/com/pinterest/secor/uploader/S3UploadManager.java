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

import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.SSEAwsKeyManagementParams;
import com.amazonaws.services.s3.model.SSECustomerKey;
import com.pinterest.secor.common.*;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
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
 * <p>
 * Set the <code>aws.sse.type</code> property to specify the type of
 * encryption to use. Supported options are:
 * <code>S3</code>, <code>KMS</code> and <code>customer</code>. See AWS
 * documentation for Server-Side Encryption (SSE) for details on these
 * options.<br/>
 * Leave blank to use unencrypted uploads.<br/>
 * If set to <code>KMS</code>, the <code>aws.sse.kms.key</code> property
 * specifies the id of the key to use. Leave unset to use the default AWS
 * key.<br/>
 * If set to <code>customer</code>, the <code>aws.sse.customer.key</code>
 * property must be set to the base64 encoded customer key to use.
 * </p>
 *
 * @author Liam Stewart (liam.stewart@gmail.com)
 */
public class S3UploadManager extends UploadManager {
    private static final Logger LOG = LoggerFactory.getLogger(S3UploadManager.class);
    private static final String KMS = "KMS";
    private static final String S3 = "S3";
    private static final String CUSTOMER = "customer";

    private TransferManager mManager;

    public S3UploadManager(SecorConfig config) {
        super(config);

        String accessKey = mConfig.getAwsAccessKey();
        String secretKey = mConfig.getAwsSecretKey();
        String endpoint = mConfig.getAwsEndpoint();
        String region = mConfig.getAwsRegion();
        AmazonS3 client;

        if (accessKey.isEmpty() || secretKey.isEmpty()) {
        	    AWSCredentialsProvider provider = new InstanceProfileCredentialsProvider();
            client = new AmazonS3Client(provider);
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

        // make upload request, taking into account configured options for encryption
        PutObjectRequest uploadRequest = new PutObjectRequest(s3Bucket, s3Key, localFile);
        uploadRequest.setCannedAcl(CannedAccessControlList.BucketOwnerFullControl);
        if (!mConfig.getAwsSseType().isEmpty()) {
            if (S3.equals(mConfig.getAwsSseType())) {
                LOG.info("uploading file {} to s3://{}/{} with S3-managed encryption", localFile, s3Bucket, s3Key);
                enableS3Encryption(uploadRequest);
            } else if (KMS.equals(mConfig.getAwsSseType())) {
                LOG.info("uploading file {} to s3://{}/{} using KMS based encryption", localFile, s3Bucket, s3Key);
                enableKmsEncryption(uploadRequest);
            } else if (CUSTOMER.equals(mConfig.getAwsSseType())) {
                LOG.info("uploading file {} to s3://{}/{} using customer key encryption", localFile, s3Bucket, s3Key);
                enableCustomerEncryption(uploadRequest);
            } else {
                // bad option
                throw new IllegalArgumentException(mConfig.getAwsSseType() + "is not a suitable type for AWS SSE encryption");
            }
        } else {
            LOG.info("uploading file {} to s3://{}/{} with no encryption", localFile, s3Bucket, s3Key);
        }

        Upload upload = mManager.upload(uploadRequest);
        return new S3UploadHandle(upload);
    }

    private void enableCustomerEncryption(PutObjectRequest uploadRequest) {
        SSECustomerKey sseKey = new SSECustomerKey(mConfig.getAwsSseCustomerKey());
        uploadRequest.withSSECustomerKey(sseKey);
    }

    private void enableKmsEncryption(PutObjectRequest uploadRequest) {
        String keyId = mConfig.getAwsSseKmsKey();
        if (!keyId.isEmpty()) {
            uploadRequest.withSSEAwsKeyManagementParams(new SSEAwsKeyManagementParams(keyId));
        } else {
            uploadRequest.withSSEAwsKeyManagementParams(new SSEAwsKeyManagementParams());
        }
    }

    private void enableS3Encryption(PutObjectRequest uploadRequest) {
        ObjectMetadata objectMetadata = new ObjectMetadata();
        objectMetadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
        uploadRequest.setMetadata(objectMetadata);
    }
}
