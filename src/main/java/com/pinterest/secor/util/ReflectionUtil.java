/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.pinterest.secor.util;

import com.pinterest.secor.common.LogFilePath;
import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.io.FileReader;
import com.pinterest.secor.io.FileReaderWriterFactory;
import com.pinterest.secor.io.FileWriter;
import com.pinterest.secor.monitoring.MetricCollector;
import com.pinterest.secor.parser.MessageParser;
import com.pinterest.secor.transformer.MessageTransformer;
import com.pinterest.secor.uploader.UploadManager;
import com.pinterest.secor.uploader.Uploader;
import com.pinterest.secor.util.orc.schema.ORCSchemaProvider;

import org.apache.hadoop.io.compress.CompressionCodec;

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
        Class<?> clazz = Class.forName(className);
        if (!UploadManager.class.isAssignableFrom(clazz)) {
            throw new IllegalArgumentException(String.format("The class '%s' is not assignable to '%s'.",
                    className, UploadManager.class.getName()));
        }

        // Assume that subclass of UploadManager has a constructor with the same signature as UploadManager
        return (UploadManager) clazz.getConstructor(SecorConfig.class).newInstance(config);
    }

    /**
     * Create an Uploader from its fully qualified class name.
     *
     * The class passed in by name must be assignable to Uploader.
     * See the secor.upload.class config option.
     *
     * @param className     The class name of a subclass of Uploader
     * @return an UploadManager instance with the runtime type of the class passed by name
     * @throws Exception
     */
    public static Uploader createUploader(String className) throws Exception {
        Class<?> clazz = Class.forName(className);
        if (!Uploader.class.isAssignableFrom(clazz)) {
            throw new IllegalArgumentException(String.format("The class '%s' is not assignable to '%s'.",
                    className, Uploader.class.getName()));
        }

        return (Uploader) clazz.newInstance();
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
        Class<?> clazz = Class.forName(className);
        if (!MessageParser.class.isAssignableFrom(clazz)) {
            throw new IllegalArgumentException(String.format("The class '%s' is not assignable to '%s'.",
                    className, MessageParser.class.getName()));
        }

        // Assume that subclass of MessageParser has a constructor with the same signature as MessageParser
        return (MessageParser) clazz.getConstructor(SecorConfig.class).newInstance(config);
    }

    /**
     * Create a FileReaderWriterFactory that is able to read and write a specific type of output log file.
     * The class passed in by name must be assignable to FileReaderWriterFactory.
     * Allows for pluggable FileReader and FileWriter instances to be constructed for a particular type of log file.
     *
     * See the secor.file.reader.writer.factory config option.
     *
     * @param className the class name of a subclass of FileReaderWriterFactory
     * @param config The SecorCondig to initialize the FileReaderWriterFactory with
     * @return a FileReaderWriterFactory with the runtime type of the class passed by name
     * @throws Exception
     */
    private static FileReaderWriterFactory createFileReaderWriterFactory(String className,
                                                                         SecorConfig config) throws Exception {
        Class<?> clazz = Class.forName(className);
        if (!FileReaderWriterFactory.class.isAssignableFrom(clazz)) {
            throw new IllegalArgumentException(String.format("The class '%s' is not assignable to '%s'.",
                    className, FileReaderWriterFactory.class.getName()));
        }

        try {
            // Try to load constructor that accepts single parameter - secor
            // configuration instance
            return (FileReaderWriterFactory) clazz.getConstructor(SecorConfig.class).newInstance(config);
        } catch (NoSuchMethodException e) {
            // Fallback to parameterless constructor
            return (FileReaderWriterFactory) clazz.newInstance();
        }
    }

    /**
     * Use the FileReaderWriterFactory specified by className to build a FileWriter
     *
     * @param className the class name of a subclass of FileReaderWriterFactory to create a FileWriter from
     * @param logFilePath the LogFilePath that the returned FileWriter should write to
     * @param codec an instance CompressionCodec to compress the file written with, or null for no compression
     * @param config The SecorCondig to initialize the FileWriter with
     * @return a FileWriter specialised to write the type of files supported by the FileReaderWriterFactory
     * @throws Exception
     */
    public static FileWriter createFileWriter(String className, LogFilePath logFilePath,
                                              CompressionCodec codec,
                                              SecorConfig config)
            throws Exception {
        return createFileReaderWriterFactory(className, config).BuildFileWriter(logFilePath, codec);
    }

    /**
     * Use the FileReaderWriterFactory specified by className to build a FileReader
     *
     * @param className the class name of a subclass of FileReaderWriterFactory to create a FileReader from
     * @param logFilePath the LogFilePath that the returned FileReader should read from
     * @param codec an instance CompressionCodec to decompress the file being read, or null for no compression
     * @param config The SecorCondig to initialize the FileReader with
     * @return a FileReader specialised to read the type of files supported by the FileReaderWriterFactory
     * @throws Exception
     */
    public static FileReader createFileReader(String className, LogFilePath logFilePath,
                                              CompressionCodec codec,
                                              SecorConfig config)
            throws Exception {
        return createFileReaderWriterFactory(className, config).BuildFileReader(logFilePath, codec);
    }

    /**
     * Create a MessageTransformer from it's fully qualified class name. The
     * class passed in by name must be assignable to MessageTransformers and have
     * 1-parameter constructor accepting a SecorConfig. Allows the MessageTransformers
     * to be pluggable by providing the class name of a desired MessageTransformers in
     * config.
     *
     * See the secor.message.transformer.class config option.
     *
     * @param className
     * @param config
     * @return
     * @throws Exception
     */
    public static MessageTransformer createMessageTransformer(
            String className, SecorConfig config) throws Exception {
        Class<?> clazz = Class.forName(className);
        if (!MessageTransformer.class.isAssignableFrom(clazz)) {
            throw new IllegalArgumentException(String.format(
                    "The class '%s' is not assignable to '%s'.", className,
                    MessageTransformer.class.getName()));
        }
        return (MessageTransformer) clazz.getConstructor(SecorConfig.class)
                .newInstance(config);
    }

    /**
     * Create an MetricCollector from its fully qualified class name.
     * <p>
     * The class passed in by name must be assignable to MetricCollector.
     * See the secor.monitoring.metrics.collector.class config option.
     *
     * @param className The class name of a subclass of MetricCollector
     * @return a MetricCollector with the runtime type of the class passed by name
     * @throws ClassNotFoundException if class with the {@code className} is not found in classpath
     * @throws IllegalAccessException if the class or its nullary
     *                                constructor is not accessible.
     * @throws InstantiationException if this {@code Class} represents an abstract class,
     *                                an interface, an array class, a primitive type, or void;
     *                                or if the class has no nullary constructor;
     *                                or if the instantiation fails for some other reason.
     */
    public static MetricCollector createMetricCollector(String className)
            throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        Class<?> clazz = Class.forName(className);
        if (!MetricCollector.class.isAssignableFrom(clazz)) {
            throw new IllegalArgumentException(String.format("The class '%s' is not assignable to '%s'.",
                    className, MetricCollector.class.getName()));
        }

        return (MetricCollector) clazz.newInstance();
    }
    
    /**
     * Create a ORCSchemaProvider from it's fully qualified class name. The
     * class passed in by name must be assignable to ORCSchemaProvider and have
     * 1-parameter constructor accepting a SecorConfig. Allows the ORCSchemaProvider
     * to be pluggable by providing the class name of a desired ORCSchemaProvider in
     * config.
     *
     * See the secor.orc.schema.provider config option.
     *
     * @param className
     * @param config
     * @return
     * @throws Exception
     */
    public static ORCSchemaProvider createORCSchemaProvider(
            String className, SecorConfig config) throws Exception {
        Class<?> clazz = Class.forName(className);
        if (!ORCSchemaProvider.class.isAssignableFrom(clazz)) {
            throw new IllegalArgumentException(String.format(
                    "The class '%s' is not assignable to '%s'.", className,
                    ORCSchemaProvider.class.getName()));
        }
        return (ORCSchemaProvider) clazz.getConstructor(SecorConfig.class)
                .newInstance(config);
    }
}
