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

import com.pinterest.secor.common.LogFilePath;
import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.io.FileReaderWriter;

import java.lang.reflect.Constructor;

import com.pinterest.secor.parser.MessageParser;
import org.apache.hadoop.io.compress.CompressionCodec;

/**
 * ReflectionUtil implements utility methods to construct objects of classes
 * specified by name.
 *
 * @author Pawel Garbacki (pawel@pinterest.com)
 */
public class ReflectionUtil {

    public static MessageParser createMessageParser(String className,
                                                    SecorConfig config) throws Exception {
        Class<?> clazz = Class.forName(className);
        if (!MessageParser.class.isAssignableFrom(clazz)) {
            throw new IllegalArgumentException(String.format("The class '%s' is not assignable to '%s'.",
                    className, MessageParser.class.getName()));
        }

        // Assume that possible subclass of MessageParser has a constructor with the same signature as MessageParser
        return (MessageParser) clazz.getConstructor(SecorConfig.class).newInstance(config);
    }

    public static FileReaderWriter createFileReaderWriter(String className, LogFilePath logFilePath,
                                                          CompressionCodec compressionCodec,
                                                          FileReaderWriter.Type type) throws Exception {
        Class<?> clazz = Class.forName(className);
        if (!FileReaderWriter.class.isAssignableFrom(clazz)) {
            throw new IllegalArgumentException(String.format("The class '%s' is not assignable to '%s'.",
                    className, FileReaderWriter.class.getName()));
        }

        // Assume that possible subclass of FileReaderWriter has a constructor with the same signature as FileReaderWriter
        return (FileReaderWriter) clazz.getConstructor(LogFilePath.class,
                CompressionCodec.class, FileReaderWriter.Type.class).newInstance(logFilePath, compressionCodec, type);
    }
}