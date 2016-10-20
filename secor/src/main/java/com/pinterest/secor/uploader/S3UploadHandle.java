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

import com.amazonaws.services.s3.transfer.Upload;
import com.amazonaws.services.s3.transfer.model.UploadResult;

/**
 * Wraps an Upload being managed by the AWS SDK TransferManager. `get`
 * blocks until the upload completes.
 *
 * @author Liam Stewart (liam.stewart@gmail.com)
 */
public class S3UploadHandle implements Handle<UploadResult> {
    private Upload mUpload;

    public S3UploadHandle(Upload u) {
        mUpload = u;
    }

    public UploadResult get() throws Exception {
        return mUpload.waitForUploadResult();
    }
}
