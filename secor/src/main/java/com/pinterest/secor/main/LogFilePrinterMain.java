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
package com.pinterest.secor.main;

import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.tools.LogFilePrinter;
import com.pinterest.secor.util.FileUtil;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Log file printer main.
 *
 * Run:
 *     $ cd optimus/secor
 *     $ mvn package
 *     $ cd target
 *     $ java -ea -Dlog4j.configuration=log4j.dev.properties -Dconfig=secor.dev.backup.properties \
 *         -cp "secor-0.1-SNAPSHOT.jar:lib/*" com.pinterest.secor.main.LogFilePrinterMain -f \
 *         s3n://bucket/path
 *
 * @author Pawel Garbacki (pawel@pinterest.com)
 */
public class LogFilePrinterMain {
    private static final Logger LOG = LoggerFactory.getLogger(LogFilePrinterMain.class);

    private static CommandLine parseArgs(String[] args) throws ParseException {
        Options options = new Options();
        options.addOption(OptionBuilder.withLongOpt("file")
                .withDescription("sequence file to read")
                .hasArg()
                .withArgName("<sequence_file_name>")
                .withType(String.class)
                .create("f"));
        options.addOption("o", "print_offsets_only", false, "whether to print only offsets " +
                "ignoring the message payload");

        CommandLineParser parser = new GnuParser();
        return parser.parse(options, args);
    }

    public static void main(String[] args) {
        try {
            CommandLine commandLine = parseArgs(args);
            SecorConfig config = SecorConfig.load();
            FileUtil.configure(config);
            LogFilePrinter printer = new LogFilePrinter(
                    commandLine.hasOption("print_offsets_only"));
            printer.printFile(commandLine.getOptionValue("file"));
        } catch (Throwable t) {
            LOG.error("Log file printer failed", t);
            System.exit(1);
        }
    }
}
