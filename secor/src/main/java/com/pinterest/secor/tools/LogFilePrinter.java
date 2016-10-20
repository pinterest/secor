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
package com.pinterest.secor.tools;

import com.pinterest.secor.util.FileUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;

import java.io.IOException;

/**
 * Log file printer displays the content of a log file.
 *
 * @author Pawel Garbacki (pawel@pinterest.com)
 */
public class LogFilePrinter {
    private boolean mPrintOffsetsOnly;

    public LogFilePrinter(boolean printOffsetsOnly) throws IOException {
        mPrintOffsetsOnly = printOffsetsOnly;
    }

    public void printFile(String path) throws Exception {
        FileSystem fileSystem = FileUtil.getFileSystem(path);
        Path fsPath = new Path(path);
        SequenceFile.Reader reader = new SequenceFile.Reader(fileSystem, fsPath,
                new Configuration());
        LongWritable key = (LongWritable) reader.getKeyClass().newInstance();
        BytesWritable value = (BytesWritable) reader.getValueClass().newInstance();
        System.out.println("reading file " + path);
        while (reader.next(key, value)) {
            if (mPrintOffsetsOnly) {
                System.out.println(Long.toString(key.get()));
            } else {
                byte[] nonPaddedBytes = new byte[value.getLength()];
                System.arraycopy(value.getBytes(), 0, nonPaddedBytes, 0, value.getLength());
                System.out.println(Long.toString(key.get()) + ": " + new String(nonPaddedBytes)); 
            }
        }
    }
}
