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
package com.pinterest.secor.util;

import com.pinterest.secor.common.SecorConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * File util implements utilities for interactions with the file system.
 *
 * @author Pawel Garbacki (pawel@pinterest.com)
 */
public class FileUtil {
    private static Configuration mConf = new Configuration(true);

    public static void configure(SecorConfig config) {
        if (config != null) {
            if (config.getCloudService().equals("Swift")) {
                mConf.set("fs.swift.impl", "org.apache.hadoop.fs.swift.snative.SwiftNativeFileSystem");
                mConf.set("fs.swift.service.GENERICPROJECT.auth.url", config.getSwiftAuthUrl());
                mConf.set("fs.swift.service.GENERICPROJECT.username", config.getSwiftUsername());
                mConf.set("fs.swift.service.GENERICPROJECT.tenant", config.getSwiftTenant());
                mConf.set("fs.swift.service.GENERICPROJECT.http.port", config.getSwiftPort());
                mConf.set("fs.swift.service.GENERICPROJECT.use.get.auth", config.getSwiftGetAuth());
                mConf.set("fs.swift.service.GENERICPROJECT.public", config.getSwiftPublic());
                if (config.getSwiftGetAuth().equals("true")) {
                    mConf.set("fs.swift.service.GENERICPROJECT.apikey", config.getSwiftApiKey());
                } else {
                    mConf.set("fs.swift.service.GENERICPROJECT.password", config.getSwiftPassword());
                }
            } else if (config.getCloudService().equals("S3")) {
                if (config.getAwsAccessKey().isEmpty() != config.getAwsSecretKey().isEmpty()) {
                    throw new IllegalArgumentException(
                        "Must specify both aws.access.key and aws.secret.key or neither.");
                }
                if (!config.getAwsAccessKey().isEmpty()) {
                    mConf.set(Constants.ACCESS_KEY, config.getAwsAccessKey());
                    mConf.set(Constants.SECRET_KEY, config.getAwsSecretKey());
                    mConf.set("fs.s3n.awsAccessKeyId", config.getAwsAccessKey());
                    mConf.set("fs.s3n.awsSecretAccessKey", config.getAwsSecretKey());
                }
            }
        }
    }

    public static FileSystem getFileSystem(String path) throws IOException {
        return FileSystem.get(URI.create(path), mConf);
    }

    public static String getPrefix(String topic, SecorConfig config)  throws IOException {
        String prefix = null;
        if (config.getCloudService().equals("Swift")) {
            String container = null;
            if (config.getSeperateContainersForTopics()) {
                if (!exists("swift://" + topic + ".GENERICPROJECT")){
                    String containerUrl = "swift://" + topic + ".GENERICPROJECT";
                    Path containerPath = new Path(containerUrl);
                    getFileSystem(containerUrl).create(containerPath).close();
                }
                container = topic;
            } else {
                container = config.getSwiftContainer();
            }
            prefix = "swift://" + container + ".GENERICPROJECT/" + config.getSwiftPath();
        } else if (config.getCloudService().equals("S3")) {
            prefix = config.getS3Prefix();
        } else if (config.getCloudService().equals("GS")) {
            prefix = "gs://" + config.getGsBucket() + "/" + config.getGsPath();
        }
        return prefix;
    }


    public static String[] list(String path) throws IOException {
        FileSystem fs = getFileSystem(path);
        Path fsPath = new Path(path);
        ArrayList<String> paths = new ArrayList<String>();
        FileStatus[] statuses = fs.listStatus(fsPath);
        if (statuses != null) {
            for (FileStatus status : statuses) {
                Path statusPath = status.getPath();
                if (path.startsWith("s3://") || path.startsWith("s3n://") || path.startsWith("s3a://") ||
                        path.startsWith("swift://") || path.startsWith("gs://")) {
                    paths.add(statusPath.toUri().toString());
                } else {
                    paths.add(statusPath.toUri().getPath());
                }
            }
        }
        return paths.toArray(new String[] {});
    }

    public static String[] listRecursively(String path) throws IOException {
        ArrayList<String> paths = new ArrayList<String>();
        String[] directPaths = list(path);
        for (String directPath : directPaths) {
            if (directPath.equals(path)) {
                assert directPaths.length == 1: Integer.toString(directPaths.length) + " == 1";
                paths.add(directPath);
            } else {
                String[] recursivePaths = listRecursively(directPath);
                paths.addAll(Arrays.asList(recursivePaths));
            }
        }
        return paths.toArray(new String[] {});
    }

    public static boolean exists(String path) throws IOException {
        FileSystem fs = getFileSystem(path);
        Path fsPath = new Path(path);
        return fs.exists(fsPath);
    }

    public static void delete(String path) throws IOException {
        if (exists(path)) {
            Path fsPath = new Path(path);
            boolean success = getFileSystem(path).delete(fsPath, true);  // recursive
            if (!success) {
                throw new IOException("Failed to delete " + path);
            }
        }
    }

    public static void deleteOnExit(String path) {
        File file = new File(path);
        file.deleteOnExit();
    }

    public static void moveToCloud(String srcLocalPath, String dstCloudPath) throws IOException {
        Path srcPath = new Path(srcLocalPath);
        Path dstPath = new Path(dstCloudPath);
        getFileSystem(dstCloudPath).moveFromLocalFile(srcPath, dstPath);
    }

    public static void touch(String path) throws IOException {
        FileSystem fs = getFileSystem(path);
        Path fsPath = new Path(path);
        fs.create(fsPath).close();
    }

    public static long getModificationTimeMsRecursive(String path) throws IOException {
        FileSystem fs = getFileSystem(path);
        Path fsPath = new Path(path);
        FileStatus status = fs.getFileStatus(fsPath);
        long modificationTime = status.getModificationTime();
        FileStatus[] statuses = fs.listStatus(fsPath);
        if (statuses != null) {
            for (FileStatus fileStatus : statuses) {
                Path statusPath = fileStatus.getPath();
                String stringPath;
                if (path.startsWith("s3://") || path.startsWith("s3n://") || path.startsWith("s3a://") ||
                        path.startsWith("swift://") || path.startsWith("gs://")) {
                    stringPath = statusPath.toUri().toString();
                } else {
                    stringPath = statusPath.toUri().getPath();
                }
                if (!stringPath.equals(path)) {
                    modificationTime = Math.max(modificationTime,
                            getModificationTimeMsRecursive(stringPath));
                }
            }
        }
        return modificationTime;
    }
}
