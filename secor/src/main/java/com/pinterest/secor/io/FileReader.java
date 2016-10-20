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

import java.io.IOException;

/**
 * Generic file reader interface for a particular type of Secor output file
 *
 * Should be returned by a FileReaderWriterFactory that also knows how to build
 * a corresponding FileReader (that is able to read the files written by this FileWriter).
 *
 * @author Praveen Murugesan (praveen@uber.com)
 */
public interface FileReader {
    /**
     * Get the next key/value from the file
     *
     * @return
     * @throws IOException
     */
    public KeyValue next() throws IOException;

    /**
     * Close the file
     *
     * @throws IOException
     */
    public void close() throws IOException;
}