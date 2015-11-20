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
 * Upload policies must inherit this class and implement 'shouldUpload' method.
 * An UploadPolicy instance is responsible to define whether a given TopicPartition must be uploaded.
 * The subclass of UploadPolicy to be used is loaded from 'secor.uploadpolicy.strategy.class' property.
 * The 'shouldUpload' method is called periodically according to the value in 'secor.uploadpolicy.check.seconds'.
 * Should the policy need to read any specific property, it can define using 'secor.uploadpolicy.properties'.
 * Properties are in the form key1=value1,key2=value2.
 * 
 * @author Flavio Barata (flavio.barata@gmail.com)
 */
public abstract class UploadPolicy {

	protected SecorConfig mConfig;
	protected FileRegistry mFileRegistry;

	public UploadPolicy(SecorConfig config, FileRegistry fileRegistry) {
		mConfig = config;
		mFileRegistry = fileRegistry;
	}
	
	/**
	 * This method must be implemented by all policies and should return <code>true</code> whether a given TopicPartition is to be uploaded.
	 * @param topicPartition The TopicPartition to be (or not) uploaded.
	 * @return <code>true</code> if a TopicPartition must be uploaded.
	 * @throws Exception
	 */
	public abstract boolean shouldUpload(TopicPartition topicPartition) throws Exception;
}
