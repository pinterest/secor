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
import com.pinterest.secor.common.TopicPartition;
import com.pinterest.secor.common.ZookeeperConnector;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Zookeeper client main.
 *
 * Run:
 *     $ cd optimus/secor
 *     $ mvn package
 *     $ cd target
 *     $ java -ea -Dlog4j.configuration=log4j.dev.properties -Dconfig=secor.dev.backup.properties \
 *         -cp "secor-0.1-SNAPSHOT.jar:lib/*" com.pinterest.secor.main.ZookeeperClientMain -c \
 *         delete_committed_offsets -t test -p 0
 *
 * @author Pawel Garbacki (pawel@pinterest.com)
 */
public class ZookeeperClientMain {
    private static final Logger LOG = LoggerFactory.getLogger(LogFilePrinterMain.class);

    private static CommandLine parseArgs(String[] args) throws ParseException {
        Options options = new Options();
        options.addOption(OptionBuilder.withLongOpt("command")
            .withDescription("command name.  One of \"delete_committed_offsets\"")
            .hasArg()
            .withArgName("<command>")
            .withType(String.class)
            .create("c"));
        options.addOption(OptionBuilder.withLongOpt("topic")
                                       .withDescription("topic whose offset should be read")
                                       .hasArg()
                                       .withArgName("<topic>")
                                       .withType(String.class)
                                       .create("t"));
        options.addOption(OptionBuilder.withLongOpt("partition")
            .withDescription("kafka partition whose offset should be read")
            .hasArg()
            .withArgName("<partition>")
            .withType(Number.class)
            .create("p"));

        CommandLineParser parser = new GnuParser();
        return parser.parse(options, args);
    }

    public static void main(String[] args) {
        try {
            CommandLine commandLine = parseArgs(args);
            String command = commandLine.getOptionValue("command");
            if (!command.equals("delete_committed_offsets")) {
                throw new IllegalArgumentException(
                    "command has to be one of \"delete_committed_offsets\"");
            }
            SecorConfig config = SecorConfig.load();
            ZookeeperConnector zookeeperConnector = new ZookeeperConnector(config);
            String topic = commandLine.getOptionValue("topic");
            if (commandLine.hasOption("partition")) {
                int partition =
                    ((Number) commandLine.getParsedOptionValue("partition")).intValue();
                TopicPartition topicPartition = new TopicPartition(topic, partition);
                zookeeperConnector.deleteCommittedOffsetPartitionCount(topicPartition);
            } else {
                zookeeperConnector.deleteCommittedOffsetTopicCount(topic);
            }
        } catch (Throwable t) {
            LOG.error("Zookeeper client failed", t);
            System.exit(1);
        }
    }
}
