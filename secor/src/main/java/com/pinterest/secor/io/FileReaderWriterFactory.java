/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.pinterest.secor.io;


import com.pinterest.secor.common.LogFilePath;
import org.apache.hadoop.io.compress.CompressionCodec;

import java.io.IOException;

/**
 * Provides a single factory class to make FileReader and FileWriter
 * instances that can read from and write to the same type of output file.
 *
 * Implementers of this interface should provide a zero-argument constructor so that they can
 * be constructed generically when referenced in configuration; see ReflectionUtil for details.
 *
 * @author Silas Davis (github-code@silasdavis.net)
 */
public interface FileReaderWriterFactory {
    /**
     * Build a FileReader instance to read from the target log file
     *
     * @param logFilePath the log file to read from
     * @param codec the compression codec the file was written with (use null for no codec,
     *              or to auto-detect from file headers where supported)
     * @return a FileReader instance to read from the target log file
     * @throws IllegalAccessException
     * @throws Exception
     * @throws InstantiationException
     */

    public FileReader BuildFileReader(LogFilePath logFilePath, CompressionCodec codec) throws Exception;
    /**
     * Build a FileWriter instance to write to the target log file
     *
     * @param logFilePath the log file to read from
     * @param codec the compression codec to write the file with
     * @return a FileWriter instance to write to the target log file
     * @throws Exception
     */
    public FileWriter BuildFileWriter(LogFilePath logFilePath, CompressionCodec codec) throws Exception;
}