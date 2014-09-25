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
package com.pinterest.secor.common;

import org.apache.hadoop.io.compress.CompressionCodec;

import com.pinterest.secor.util.ReflectionUtil;

/**
 * Attributes for a log file path
 * 
 * @author Praveen Murugesan (praveen@uber.com)
 *
 */
public class LogFilePathAttributes {
	
	private final CompressionCodec mCodec;
	private final String mFileExtension;
	
	public LogFilePathAttributes(SecorConfig mConfig) throws Exception {
		if (mConfig.getCompressionCodec() != null && !mConfig.getCompressionCodec().isEmpty()) {
			mCodec = 
                    ((CompressionCodec) ReflectionUtil.createCompressionCodec(mConfig.getCompressionCodec()));
            mFileExtension = mCodec.getDefaultExtension();
        } else {
        	mCodec = null;
        	String fileExtension = mConfig.getFileExtension();
            mFileExtension = (fileExtension == null)? "" : fileExtension;
        }
	}
	
	/**
	 * Get the log file path extension to be used
	 * @return
	 */
	public String getLogFileExtension() {
		return mFileExtension;
	}
	
	/**
	 * Get the log file path compression codec to use
	 * @return
	 */
	public CompressionCodec getCompressionCodec() {
		return mCodec;
	}

}
