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
package com.pinterest.secor.io;


import org.apache.hadoop.io.compress.CompressionCodec;

import com.pinterest.secor.common.LogFilePath;
import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.util.ReflectionUtil;

/**
 * File Writer Factory which returns a writer  based upon secor configuration
 * 
 * @author Praveen Murugesan (praveen@uber.com)
 *
 */
public class FileWriterFactory {
	
	public static FileWriter create(LogFilePath path, CompressionCodec codec, SecorConfig mConfig) throws Exception {
		if (mConfig.getFileWriter() != null && !mConfig.getFileWriter().isEmpty()) {
			return ((FileWriter) ReflectionUtil.createFileWriter(mConfig.getFileWriter(), path, codec));
        } 
		throw new IllegalArgumentException("File Writer not defined or empty");
	}

}
