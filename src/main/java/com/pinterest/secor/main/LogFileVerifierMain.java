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
import com.pinterest.secor.tools.LogFileVerifier;
import com.pinterest.secor.util.FileUtil;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Log file verifier main.
 *
 * Run:
 *     $ cd optimus/secor
 *     $ mvn package
 *     $ cd target
 *     $ java -ea -Dlog4j.configuration=log4j.dev.properties -Dconfig=secor.dev.backup.properties \
 *         -cp "secor-0.1-SNAPSHOT.jar:lib/*" com.pinterest.secor.main.LogFileVerifierMain -t \
 *         topic -q
 *
 * @author Pawel Garbacki (pawel@pinterest.com)
 */
public class LogFileVerifierMain {
    private static final Logger LOG = LoggerFactory.getLogger(LogFileVerifierMain.class);

    private static CommandLine parseArgs(String[] args) throws ParseException {
        Options options = new Options();
        options.addOption(OptionBuilder.withLongOpt("topic")
               .withDescription("kafka topic name")
               .hasArg()
               .withArgName("<topic>")
               .withType(String.class)
               .create("t"));
        options.addOption(OptionBuilder.withLongOpt("start_offset")
               .withDescription("offset identifying the first set of files to check")
               .withArgName("<offset>")
               .withType(Long.class)
               .create("s"));
        options.addOption(OptionBuilder.withLongOpt("end_offset")
               .withDescription("offset identifying the last set of files to check")
               .withArgName("<offset>")
               .withType(Long.class)
               .create("e"));
        options.addOption(OptionBuilder.withLongOpt("messages")
               .withDescription("expected number of messages")
               .hasArg()
               .withArgName("<num_messages>")
               .withType(Number.class)
               .create("m"));
        options.addOption("q", "sequence_offsets", false, "whether to verify that offsets " +
                          "increase sequentially.  Requires loading all offsets in a snapshot " +
                          "to memory so use cautiously");

        CommandLineParser parser = new GnuParser();
        return parser.parse(options, args);
    }

    public static void main(String[] args) {
        try {
            CommandLine commandLine = parseArgs(args);
            SecorConfig config = SecorConfig.load();
            FileUtil.configure(config);
            LogFileVerifier verifier = new LogFileVerifier(config,
                commandLine.getOptionValue("topic"));
            long startOffset = -2;
            long endOffset = Long.MAX_VALUE;
            if (commandLine.hasOption("start_offset")) {
                startOffset = Long.parseLong(commandLine.getOptionValue("start_offset"));
                if (commandLine.hasOption("end_offset")) {
                    endOffset = Long.parseLong(commandLine.getOptionValue("end_offset"));
                }
            }
            int numMessages = -1;
            if (commandLine.hasOption("messages")) {
                numMessages = ((Number) commandLine.getParsedOptionValue("messages")).intValue();
            }
            verifier.verifyCounts(startOffset, endOffset, numMessages);
            if (commandLine.hasOption("sequence_offsets")) {
                verifier.verifySequences(startOffset, endOffset);
            }
            System.out.println("verification succeeded");
        } catch (Throwable t) {
            LOG.error("Log file verifier failed", t);
            System.exit(1);
        }
    }
}
