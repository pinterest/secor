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

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;

import java.text.SimpleDateFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pinterest.secor.common.SecorConfig;

/**
 * File util implements utilities for interactions with the file system.
 *
 * @author Pawel Garbacki (pawel@pinterest.com)
 */
public class FileUtil {
    private static final Logger LOG = LoggerFactory.getLogger(FileUtil.class);
    private static Configuration mConf = new Configuration(true);
    private static final char[] m_digits = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a',
        'b', 'c', 'd', 'e', 'f'};
    private static final Pattern datePattern = Pattern.compile(".*dt=(\\d\\d\\d\\d-\\d\\d-\\d\\d).*");

    public static void configure(SecorConfig config) {
        if (config != null) {
            if (config.getCloudService().equals("Swift")) {
                mConf.set("fs.swift.impl", "org.apache.hadoop.fs.swift.snative.SwiftNativeFileSystem");
                mConf.set("fs.swift.service.GENERICPROJECT.auth.url", config.getSwiftAuthUrl());
                mConf.set("fs.swift.service.GENERICPROJECT.username", config.getSwiftUsername());
                mConf.set("fs.swift.service.GENERICPROJECT.tenant", config.getSwiftTenant());
                mConf.set("fs.swift.service.GENERICPROJECT.region", config.getSwiftRegion());
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

    public static boolean s3PathPrefixIsAltered(String logFileName, SecorConfig config)
                                                        throws Exception {
        Date logDate = null;
        if (config.getS3AlterPathDate() != null && !config.getS3AlterPathDate().isEmpty()) {

            Date s3AlterPathDate = new SimpleDateFormat("yyyy-MM-dd").parse(config.getS3AlterPathDate());

            // logFileName contains the log path, e.g. raw_logs/secor_topic/dt=2016-04-20/3_0_0000000000000292564
            Matcher dateMatcher = datePattern.matcher(logFileName);
            if (dateMatcher.find()) {
                logDate = new SimpleDateFormat("yyyy-MM-dd").parse(dateMatcher.group(1));
            }

            if (logDate == null) {
                throw new Exception("Did not find a date in the format yyyy-MM-dd in " + logFileName);
            }

            if (!s3AlterPathDate.after(logDate)) {
                return true;
            }
        }

        return false;
    }

    public static String getS3AlternativePathPrefix(SecorConfig config) {
        return config.getS3AlternativePath();
    }

    public static String getS3AlternativePrefix(SecorConfig config) {
        return config.getS3FileSystem() + "://" + config.getS3Bucket() + "/" + config.getS3AlternativePath();
    }

    public static String getPrefix(String topic, SecorConfig config)  throws IOException {
        String prefix = null;
        if (config.getCloudService().equals("Swift")) {
            String container = null;
            if (config.getSeparateContainersForTopics()) {
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
        } else if (config.getCloudService().equals("Azure")) {
            prefix = "azure://" + config.getAzureContainer() + "/" + config.getAzurePath();
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
    
    /** Generate MD5 hash of topic and partitions. And extract first 4 characters of the MD5 hash.
     * @param topic
     * @param partitions
     * @return
     */
    public static String getMd5Hash(String topic, String[] partitions) {
      ArrayList<String> elements = new ArrayList<String>();
      elements.add(topic);
      for (String partition : partitions) {
        elements.add(partition);
      }
      String pathPrefix = StringUtils.join(elements, "/");
      try {
        final MessageDigest messageDigest = MessageDigest.getInstance("MD5");
        byte[] md5Bytes = messageDigest.digest(pathPrefix.getBytes("UTF-8"));
        return getHexEncode(md5Bytes).substring(0, 4);
      } catch (NoSuchAlgorithmException e) {
        LOG.error(e.getMessage());
      } catch (UnsupportedEncodingException e) {
        LOG.error(e.getMessage());
      }
      return "";
    }

    private static String getHexEncode(byte[] bytes) {
      final char[] chars = new char[bytes.length * 2];
      for (int i = 0; i < bytes.length; ++i) {
        final int cx = i * 2;
        final byte b = bytes[i];
        chars[cx] = m_digits[(b & 0xf0) >> 4];
        chars[cx + 1] = m_digits[(b & 0x0f)];
      }
      return new String(chars);
    }
}
