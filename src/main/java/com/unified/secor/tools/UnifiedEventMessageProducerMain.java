////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/*
 * Copyright © 2017 Unified Social, Inc.
 * 180 Madison Avenue, 23rd Floor, New York, NY 10016, U.S.A.
 * All rights reserved.
 *
 * This software (the "Software") is provided pursuant to the license agreement you entered into with Unified Social,
 * Inc. (the "License Agreement").  The Software is the confidential and proprietary information of Unified Social,
 * Inc., and you shall use it only in accordance with the terms and conditions of the License Agreement.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND "AS AVAILABLE."  UNIFIED SOCIAL, INC. MAKES NO WARRANTIES OF ANY KIND, WHETHER
 * EXPRESS OR IMPLIED, INCLUDING, BUT NOT LIMITED TO THE IMPLIED WARRANTIES AND CONDITIONS OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NON-INFRINGEMENT.
 */

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

package com.unified.secor.tools;

import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.pinterest.secor.main.TestLogMessageProducerMain;
import com.unified.secor.tools.schema.Message;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class UnifiedEventMessageProducerMain {

    private static final Logger LOG = LoggerFactory.getLogger(TestLogMessageProducerMain.class);

    private static CommandLine parseArgs (String[] args) throws ParseException {
        Options options = new Options();
        options.addOption(OptionBuilder.withLongOpt("topic")
                                       .withDescription("topic to post to")
                                       .hasArg()
                                       .withArgName("<topic>")
                                       .withType(String.class)
                                       .create("t"));
        options.addOption(OptionBuilder.withLongOpt("messages")
                                       .withDescription("number of messages per producer to post")
                                       .hasArg()
                                       .withArgName("<num_messages>")
                                       .withType(Number.class)
                                       .create("m"));
        options.addOption(OptionBuilder.withLongOpt("broker")
                                       .withDescription("broker string, e.g. localhost:9092")
                                       .hasArg()
                                       .withArgName("<broker>")
                                       .withType(String.class)
                                       .create("broker"));

        CommandLineParser parser = new GnuParser();
        return parser.parse(options, args);
    }

    public static void main (String[] args) {
        try {
            CommandLine commandLine     = parseArgs(args);
            String      topic           = commandLine.getOptionValue("topic");
            int         messages        = ((Number) commandLine.getParsedOptionValue("messages")).intValue();
            String      broker          = commandLine.getOptionValue("broker");

            new UnifiedEventMessageProducerMain().run(broker, topic, messages);
        }
        catch (Throwable t) {
            LOG.error("Log message producer failed", t);
            System.exit(1);
        }
    }

    public void run (String brokerList, String topic, int numMessages) {
        Properties properties = new Properties();
        if ( brokerList == null || brokerList.isEmpty() ) {
            properties.put("metadata.broker.list", "localhost:9092");
        }
        else {
            properties.put("metadata.broker.list", brokerList);
        }
        properties.put("partitioner.class", "com.pinterest.secor.tools.RandomPartitioner");
        //        properties.put("serializer.class", "kafka.serializer.DefaultEncoder");
        properties.put("serializer.class", "kafka.serializer.StringEncoder");
        properties.put("key.serializer.class", "kafka.serializer.StringEncoder");
        properties.put("request.required.acks", "1");

        ProducerConfig           config   = new ProducerConfig(properties);
        Producer<String, String> producer = new Producer<String, String>(config);

        Gson gson = new Gson();

        for (int i = 0; i < numMessages; ++i) {
            Message testMessage = new Message(
                "messageType",
                "test",
                "job_4",
                "tag",
                "1.0",
                "2.0"
            );

            String message;
            message = gson.toJson(testMessage);
            LOG.info("Writing Topic: {} Message: {}", topic, message);

            KeyedMessage<String, String> data = new KeyedMessage<String, String>(
                topic, Integer.toString(i), message);
            producer.send(data);
        }
        producer.close();
    }
}
