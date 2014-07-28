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
package com.pinterest.secor.storage;

import java.io.IOException;

import com.pinterest.secor.common.LogFilePath;

/**
 * Base marker for a storage type. Messages are consumed from Kafka, parsed and
 * then written to a specific storage format.
 * 
 * @author Leonardo Noleto (noleto.leonardo@gmail.com)
 * 
 */
public interface StorageFactory {

	Writer createWriter(LogFilePath path) throws IOException;

	/**
	 * Adds a extension to file before uploading to S3. For instance ".gz".
	 * 
	 * @param fullPath
	 *            the full path of local file.
	 * @return the full path of local file optionally added of an extension.
	 */
	String addExtension(String fullPath);
}
