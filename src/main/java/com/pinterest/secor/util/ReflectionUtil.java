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

import org.apache.hadoop.io.compress.CompressionCodec;

import com.pinterest.secor.common.LogFilePath;
import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.io.FileReader;
import com.pinterest.secor.io.FileReaderWriterFactory;
import com.pinterest.secor.io.FileWriter;
import com.pinterest.secor.parser.MessageParser;
import com.pinterest.secor.uploader.UploadManager;

/**
 * ReflectionUtil implements utility methods to construct objects of classes
 * specified by name.
 *
 * @author Pawel Garbacki (pawel@pinterest.com)
 * @author Silas Davis (github-code@silasdavis.net)
 */
public class ReflectionUtil {
	
    /**
     * Create an UploadManager from its fully qualified class name.
     *
     * The class passed in by name must be assignable to UploadManager
     * and have 1-parameter constructor accepting a SecorConfig.
     *
     * See the secor.upload.manager.class config option.
     *
     * @param className The class name of a subclass of UploadManager
     * @param config The SecorCondig to initialize the UploadManager with
     * @return an UploadManager instance with the runtime type of the class passed by name
     * @throws Exception
     */
    public static UploadManager createUploadManager(String className,
                                                    SecorConfig config) throws Exception {
        return createClass(className, UploadManager.class, config);
    }

    /**
     * Create a MessageParser from it's fully qualified class name.
     * The class passed in by name must be assignable to MessageParser and have 1-parameter constructor accepting a SecorConfig.
     * Allows the MessageParser to be pluggable by providing the class name of a desired MessageParser in config.
     *
     * See the secor.message.parser.class config option.
     *
     * @param className The class name of a subclass of MessageParser
     * @param config The SecorCondig to initialize the MessageParser with
     * @return a MessageParser instance with the runtime type of the class passed by name
     * @throws Exception
     */
    public static MessageParser createMessageParser(String className,
                                                    SecorConfig config) throws Exception {
        return createClass(className, MessageParser.class, config);
    }

    /**
     * Use the FileReaderWriterFactory specified by className to build a FileWriter
     *
     * @param className the class name of a subclass of FileReaderWriterFactory to create a FileWriter from
     * @param logFilePath the LogFilePath that the returned FileWriter should write to
     * @param codec an instance CompressionCodec to compress the file written with, or null for no compression
     * @return a FileWriter specialised to write the type of files supported by the FileReaderWriterFactory
     * @throws Exception
     */
    public static FileWriter createFileWriter(String className, LogFilePath logFilePath,
                                              CompressionCodec codec) throws Exception {
        return createClass(className, FileReaderWriterFactory.class).BuildFileWriter(logFilePath, codec);
    }

    /**
     * Use the FileReaderWriterFactory specified by className to build a FileReader
     *
     * @param className the class name of a subclass of FileReaderWriterFactory to create a FileReader from
     * @param logFilePath the LogFilePath that the returned FileReader should read from
     * @param codec an instance CompressionCodec to decompress the file being read, or null for no compression
     * @return a FileReader specialised to read the type of files supported by the FileReaderWriterFactory
     * @throws Exception
     */
    public static FileReader createFileReader(String className, LogFilePath logFilePath,
                                              CompressionCodec codec) throws Exception {
        return createClass(className, FileReaderWriterFactory.class).BuildFileReader(logFilePath, codec);
    }

    /**
     * Instantiate a class specified by className using its constructor that matches parameters.
     * The className must be a subclass of classType.
     * 
     * @param className the class name of a subclass of classType to be instantiated
     * @param classType the class type corresponding to the className
     * @param parameters optional parameters to be used in the constructor of className
     * @return the instantiated class
     * @throws Exception
     */
	private static <T> T createClass(String className, Class<T> classType, Object... parameters) throws Exception {
		Class<?> clazz = Class.forName(className);
		if (!classType.isAssignableFrom(clazz)) {
			throw new IllegalArgumentException(String.format("The class '%s' is not assignable to '%s'.",
					className, classType.getName()));
		}
		Class<?>[] classes = new Class<?>[parameters.length];
		for (int i = 0; i < classes.length; i++) {
			classes[i] = parameters[i].getClass();
		}
		return (T) clazz.getConstructor(classes).newInstance(parameters);
	}
}
