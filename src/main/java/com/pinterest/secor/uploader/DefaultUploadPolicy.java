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

import com.pinterest.secor.common.FileRegistry;
import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.common.TopicPartition;

/**
 * This policy combines two other policies: SizeBasedUploadPolicy and AgeBasedUploadPolicy.
 * It uploads a TopicPartition if one OR other (or both) policy is satisfied.
 * Upload policy properties must be defined in the 'secor.uploadpolicy.properties' property.
 * 
 * For example: secor.uploadpolicy.properties=MAX_FILE_SIZE_BYTES=200000000,MAX_FILE_AGE_SECONDS=3600
 * 
 * @see com.pinterest.secor.uploader.SizeBasedUploadPolicy
 * @see com.pinterest.secor.uploader.AgeBasedUploadPolicy
 * 
 * @author Flavio Barata (flavio.barata@gmail.com)
 */
public class DefaultUploadPolicy extends UploadPolicy {
	
	private UploadPolicy mSizeBasedUploadPolicy;
	private UploadPolicy mAgeBasedUploadPolicy;

	public DefaultUploadPolicy(SecorConfig config, FileRegistry fileRegistry) {
		super(config, fileRegistry);
		
		mSizeBasedUploadPolicy = new SizeBasedUploadPolicy(config, fileRegistry);
		mAgeBasedUploadPolicy = new AgeBasedUploadPolicy(config, fileRegistry);
	}

	@Override
	public boolean shouldUpload(TopicPartition topicPartition) throws Exception {
		return mSizeBasedUploadPolicy.shouldUpload(topicPartition) ||
				mAgeBasedUploadPolicy.shouldUpload(topicPartition);
	}

}
