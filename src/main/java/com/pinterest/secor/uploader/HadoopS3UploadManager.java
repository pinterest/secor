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
import com.pinterest.secor.util.FileUtil;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Future;

/**
 * Manages uploads to S3 using the Hadoop API.
 *
 * @author Pawel Garbacki (pawel@pinterest.com)
 */
public class HadoopS3UploadManager extends UploadManager {
    private static final Logger LOG = LoggerFactory.getLogger(HadoopS3UploadManager.class);

    protected static final ExecutorService executor = Executors.newFixedThreadPool(256);

    public HadoopS3UploadManager(SecorConfig config) {
        super(config);
    }
    
    private Map<String,String> getCustomTopicsNamesMap(SecorConfig mConfig)  {
    	Map<String,String> customTopicsNamesMap = null;
    	if(mConfig.getCustomTopicsNames() != null) {
	    	String customTopicsNamesField = mConfig.getCustomTopicsNames();
	    	if (customTopicsNamesField.length() != 0 && customTopicsNamesField.contains(";")) {
	        	String[] customTopicsNames = customTopicsNamesField.split(";");
	        	customTopicsNamesMap = new HashMap<String,String>();
	        	for (String topicNames: customTopicsNames) {
	        		if (topicNames.matches("[A-Za-z0-9_]+:[A-Za-z0-9_]+")) {
	            		customTopicsNamesMap.put(topicNames.split(":")[0], topicNames.split(":")[1]);
	        		}
	            }
	    	}
    	}
    	return customTopicsNamesMap;
	}

    public Handle<?> upload(LogFilePath localPath) throws Exception {
        String prefix = FileUtil.getPrefix(localPath.getTopic(), mConfig);
        LogFilePath path = localPath.withPrefix(prefix);
        final String localLogFilename = localPath.getLogFilePath();
        final String logFileName;
        final String topicName = localPath.getTopic();
        final Map<String,String> customTopicsNamesMap = getCustomTopicsNamesMap(mConfig);

        if (FileUtil.s3PathPrefixIsAltered(path.getLogFilePath(), mConfig)) {
           logFileName = localPath.withPrefix(FileUtil.getS3AlternativePrefix(mConfig)).getLogFilePath();
           LOG.info("Will upload file to alternative s3 prefix path {}", logFileName);
        }
        else {
            logFileName = path.getLogFilePath();
        }

        LOG.info("uploading file {} to {}", localLogFilename, logFileName);

        final Future<?> f = executor.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    if (customTopicsNamesMap != null) {
                    	String logFileNameCustomTopicName = logFileName;
                    	logFileNameCustomTopicName = logFileNameCustomTopicName.replace(topicName,
                    			customTopicsNamesMap.get(topicName));
                    	FileUtil.moveToCloud(localLogFilename, logFileNameCustomTopicName);
                    }else {
                    	FileUtil.moveToCloud(localLogFilename, logFileName);
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        return new FutureHandle(f);
    }
}
