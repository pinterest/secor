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
package com.pinterest.secor.parser;

import com.pinterest.secor.message.Message;

import java.util.List;

/**
 * The Partitioner knows when to finalize a file folder partition.
 *
 * A file folder partition (e.g. dt=2015-07-07) can be finalized when all
 * messages in that date arrived.  The caller (PartitionFinalizer) will do the
 * finalization work (e.g. generate _SUCCESS file, perform hive registration)
 *
 * The partitioner provide the method to calculate the range of file
 * folder partitions to be finalized and provide the method to iterate through
 * the range.
 *
 * The caller will first provide a list of last-consumed messages for a given
 * kafka topic and call #getFinalizedUptoPartitions to get the finalized-up-to
 * partition and then walk backwards by calling #getPreviousPartitions to
 * collect all the previous partitions which are ready to be finalized.
 *
 * Note that finalize-up-to partition itself is not inclusive in the range of
 * partitions to be finalized.
 *
 * The caller might repeat this loop multiple times when the filesystem partition
 * is multi-dimensional (e.g. [dt=2015-07-07,hr=05]).  it will loop once for the
 * hourly folder finalization and another time for the daily folder.
 *
 * Note that although we use daily/hourly partition illustrate the use of
 * partitioner, it is be no means the partitioner can only work with timestamp
 * based partitioning, it should also be able to work with offset based
 * partitioning as long as we establish an iterating order within those
 * partitions.
 *
 * @author Henry Cai (hcai@pinterest.com)
 */
public interface Partitioner {
    /**
     * Calculates the partition to finalize-up-to from a list of last-consumed
     * messages and a list of last-enqueued messages.
     *
     * For each kafka topic/partition for a given topic, the caller will provide
     * two messages:
     *     * lastMessage: the last message at the tail of the kafka queue
     *     * committedMessage: the message secor consumed and committed
     * And then iterate over all the kafka topic partitions for the given topic,
     * the caller will gather the above two messages into two lists.
     *
     * The Partitioner will compare the messages from all kafka partitions to
     * see which one is the earliest to finalize up to.  The partitioner will
     * normally use the timestamp from the committedMessage to decide
     * the finalize time.  But for some slow topics where there is no new
     * messages coming for a while (i.e. lastMessage == committedMessage),
     * the partitioner can use the current time as the finalize time.
     *
     * Note that the up-to partition itself is not inclusive in the range to be
     * finalized.  For example, when the last message is in 2015-07-07,
     * 7/7 itself is not complete yet.
     *
     * Note also that the partitioner might want to adjust down the finalize
     * time to allow a safety lag for late arrival messages.  e.g. adding one
     * extra hour lag
     *
     * @param lastMessages  the last message at the tail of the queue
     * @param committedMessages  the message secor consumed and committed
     *
     * @return  a String array to represent a file folder partition to finalize up to
     */
    String[] getFinalizedUptoPartitions(List<Message> lastMessages,
                                        List<Message> committedMessages) throws Exception;

    /**
     * Get the previous partition out of the incoming partition.
     * E.g. for ["dt=2015-07-07","hr=05"], it will return ["dt=2015-07-07","hr=04"]
     *
     * Note that the implementation might return the previous sequence in daily/mixed forms, e.g.
     * [dt=2015-07-07, hr=01]
     * [dt=2015-07-07, hr=00]
     * [dt=2015-07-07]        <-- dt folder in between
     * [dt=2015-07-06, hr=23]
     * [dt=2015-07-07, hr=22]
     *
     * @param partition
     * @return
     */
    String[] getPreviousPartitions(String[] partition) throws Exception;
}
