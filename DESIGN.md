# Pinterest Secor: log persistence layer

## Prerequisites

This document assumes familiarity with [Apache Kafka].

## Objectives

*Secor* is a service persisting [Kafka] logs to [Amazon S3]. Its design is motivated by the following needs:

* **minimized chance of data loss and corruption:** this is by far the highest priority objective. Logs are the basis of billing and as such they cannot lie,

* **protection against bad code****:** data corruption caused by bugs in the message parsers should be reversible,

* **low log-to-ready_for_consumption delay:** logged messages should be ready for consumption by analytical tools asap. We don’t exclude a near-real-time use case in the near future,

* **horizontal scalability:** the system should be able to outgrow its initial use case. The scalability should be achieved by adding more replicas of components rather than resources per component,

* **extensibility:** it should be easy to add new component types (e.g., log formats, message parsers and filters),

* **design clarity (and simplicity):** simple solutions are easy to understand, implement, maintain, and fix,

* **minimized incident-to-alert delay:** incidents are an unavoidable reality of working with distributed systems, no matter how reliable. A prompt alerting solution should be an integral part of the design,

* **zero downtime upgrades:** it should be possible to upgrade the system to a new version in a way transparent to the downstream data clients,

* **dependence on public APIs:** the system should reply on public [Kafka] APIs only. Furthermore, it should be compatible with the most recent [Kafka] version (0.8) which offers significant improvements over 0.7, and it comes with Go language bindings (required by other pieces of the Ads infra).

No-goals:

* **minimized resource footprint:** this may become an important objective at some point but currently we don’t optimize for machine or storage footprint.

Secor will be initially used to persist Ads impression logs but in the future it may be considered as a replacement of the current logging pipeline.

## Related work

There is a number of open source [Kafka] consumers saving data to [S3]. To the best of our knowledge, none of them is

* able to auto-detect new topics and kafka partitions,

* dynamically distribute the load across consumers at runtime,

* control the upload policy on a per-topic-kafka-partition basis (it requires the ability to commit message offsets at the granularity of an individual topic/kafka partition),

* plug in custom message parsing logic.

## Design overview

Secor is a distributed [Kafka] *consumer*. Individual consumers are divided into *groups*. For every partition (of a [Kafka] topic) there is exactly one consumer in each group processing messages posted to that partition. You may think of a group as an application implementing a specific message filtering and/or conversion logic.

In the initial implementation, there are two groups, the backup group, and the partition group. The *backup* group simply persists all messages on [S3] in the format they are received by [Kafka]. It does not look into message content. The *partition* group parses each message to extract application-defined partition names. Messages are then grouped and stored in a directory structure arranged according to those names. The name "partition" is a bit overloaded as it is used to describe both the numeric Kafka topic partition as well as an arbitrary string derived from the message content. To disambiguate those two, while talking about the former we will always explicitly call it *Kafka partition*.

## Backup group

The purpose of the backup group is to create a copy of the messages in the exact format they are received by the [Kafka] brokers. Consumers in this group are designed to be dead-simple, performant, and highly resilient. Since the data copied verbatim, no code upgrades are required to support new message types.

In most of the cases, the output of the copy backup is not suitable for direct consumption by data analysis tools. The existence of that group increases confidence levels and allows raw data to be reprocessed for backfilling of outputs that were messed up due to bugs in other, logically and computationally more involving groups. For instance, if a bug was introduced in the message parsing logic implemented by the partition group, the processed data may be incorrect. If that bug is subtle, noticing it may take a while. [Kafka] brokers store messages only for a limited time (usually a few days). If the original log data is lost, there is no way retroactively fix corrupted output. The backup group makes sure that no data is ever lost.

## Partition group

Partition group clusters log messages based on custom logic parametrized with message content. More precisely, a consumer in this group parses the message content to extract a list of partitions and places the message in a file with other messages that belong to those partitions. In the majority of cases, the message partition list contains a single value, the date corresponding to the timestamp recorded in the message.

The main complication in the implementation of consumers in this group is the fact that different topics may use different message formats (e.g., json vs thrift). Furthermore, extracting partitions requires the knowledge of the exact message schema (e.g., the name of the json or thrift field carrying the timestamp). Those two properties introduce the need for specialized parsers. Furthermore, since message formats may change, we need a mechanism to keep parser configs up to date. Versioning and version rewinding for retroactive backfills adds another level of complexity. See the section on upgrades for more details.

## Consumer

A consumer is a component responsible for reading messages submitted to [Kafka] topics, optionally transforming them, and writing the output to [S3]. A consumer belongs to exactly one group but it may process messages from multiple topics and [Kafka] partitions. [Kafka] guarantees that every message is delivered to exactly one consumer in a group.

[S3] does not support appending to existing files so messages need to be stored locally first before being uploaded (in batches) to [S3]. Neither [S3] nor data processing tools like [Hive] enjoy working with a large number of small files. Deciding when to upload a local file to [S3] is a matter of *upload policy*. We envision two types of policy rules: time-based and size-based. The time-based rule forces an upload if the time delta between the current time and file creation time is greater than the predefined threshold. The size-based rule forces uploads of files larger than a given size. A file is removed from local disk immediately after its upload to [S3]. At that time we also record the offset of the latest message in the uploaded file in Zookeeper.

In addition to a upload policy, a consumer is parametrized with a log reader, a message parser, log writer, and file uploader.

The *reader* reads the next message. In most of the cases, messages are retrieved from Kafka but it is possible to use alternative message sources. E.g., for the purpose of backfilling, we could implement a reader streaming messages from a file.

The *message parser* is responsible for extracting the payload and the list of partitions from a message.

The *writer* appends the message to a local file corresponding to the message’s topic and partitions. For a given combination of topic and partitions there is exactly one file appended to at a given time.

The *uploader* moves log files from local directory to [S3] and commits the last uploaded offsets to zookeeper.

### Offset management

A Secor consumer is built on top of [Kafka high level consumer API](https://kafka.apache.org/documentation.html#highlevelconsumerapi). The offset of the last committed (persisted in [S3]) message for each topic/Kafka partition pair is stored in zookeeper. Certain types of events such as join of a new consumer or crash of an existing one trigger so called *rebalance*. Rebalance may change the allocation of topic/Kafka partitions to consumers. A topic/Kafka partition that changes the ownership will be consumed from the last committed offset + 1 by the new owner (note: offsets increase by one between messages). The greatest challenge in the design of the consumer logic is to handle rebalance events while guaranteeing that each message is persisted (in [S3]) exactly once. Below we outline an algorithm implementing a logic with this property.

#### Data structures

Every consumer keeps the following information locally.

`last_seen_offset`: a mapping from `<topic, kafka partition>` to the greatest offset of a message seen (but not necessarily committed) in that topic/Kafka partition.

`last_committed_offset`: a mapping from `<topic, kafka partition>` to the greatest committed offset of a message in that topic/Kafka partition. Committed offset stored locally does not have to agree with the value stored currently in zookeeper but it corresponds to a value that zookeeper had at some point in the past.

#### Message consumption algorithm

For simplicity, the following pseudocode ignores all details related to message parsing.

```
consumer.run() {
  do forever {
    message = reader.read();
    writer.write(message);
    uploader.check_policy();
  }
}
```

```
reader.read() {
  message = kafka.next();
  t = message.topic;
  p = message.partition;
  if message.offset <= last_committed_offset[t, p] {
    return null;
  }
  return message;
}
```

```
writer.write(message) {
  t = message.topic;
  p = message.partition;
  if message.offset != last_seen_offset[t, p] + 1 {
    delete_local_files_for_topic_partition(t, p);
  }
  write_to_local_file(message);
  last_seen_offset[t, p] = message.offset;
}
```

```
uploader.check_policy() {
  for (t, p) in owned_topic_partitions {
    // Policy example:
    //     sum(file.size) > max_size or
    //     max(file.mtime) < now - max_age
    if policy.should_upload(t, p) {
      old_offset = last_committed_offset[t, p];
      new_offset = zookeeper.get_last_committed_offset(t, p);
      last_committed_offset[t, p] = new_offset;
      if old_offset == new_offset {
        // Exclusive lock: prevent any modifications of the committed
        // offset for a given topic/kafka partition.
        zookeeper.lock(t, p);
        upload_files_to_s3(t, p);
        zookeeper.set_last_committed_offset(t, p, seen_offset[t, p]);
        zookeeper.unlock();
      }

      // Rewrite local files for a given topic and partition removing
      // messages with offsets lower than or equal to the given
      // offset.
      trim_files(t, p, new_offset);
    }
  }
}
```

## Output file names

The output of consumers is stored on local (or EBS) disks first and eventually uploaded to s3. The local and s3 file name format follows the same pattern. Directory paths track topic and partition names. File basename contains the Kafka partition number and the Kafka offset of the first message in that file. Additionally, files are labeled with generation count. Generation is basically a version number of the Secor software that increments between non-compatible releases. Generations allow us to separate outputs of Secor versions during testing, rolling upgrades, etc. The consumer group is not included explicitly in the output path. We expect that the output of different consumer groups will go to different top-level directories.

Putting this all together, a message with timestamp `<some_date:some_time>` written to topic `<some_topic>`, Kafka partition `<some_kafka_partition>` at offset `<some_offset>` by software with generation `<generation>` will end up in file `s3://logs/<some_topic>/<some_date>/<generation>_<some_kafka_parition>_<first_message_offset>.seq` where `<first_message_offset>` <= `<some_offset>`.

The nice property of the proposed file format is that given a list of output files and a Kafka message, we can tell which file contains the output for that message. In other words, we can track correspondence between the output files of different consumer groups. For instance, assume that a bug in the code resulted in logs for a given date being incorrectly processed. We now need to remove all output files produced by the partition group and regenerate them from the files written by the backup group. The composition of file paths guarantees that we can tell which backup files contain the relevant raw records from the names of the removed partition group output files.

## New consumer code rollouts

The upgrade procedure is as simple as killing consumers running the old version of the code and letting them pick up new binaries upon restart. Generation numbers provide output isolation across incompatible releases.

## Failure scenarios

### Consumer crash

1. A consumer process dies unexpectedly.

2. [Kafka] notices consumer departure and assigns its topics/partitions to someone else during the rebalancing process.

3. A new consumer is assigned to a topic/partition previously handled by the failed consumer.

4. The new consumer starts processing messages from the last committed offset recorded in Zookeeper.

5. The failed consumer comes back (recovers), say on the same machine where it was running before.

6. The recovered consumer discards (i.e., removes) all local log files.

7. Kafka assigns the recovered consumer topics/partitions.

### Consumers cannot keep up with logging rate

To increase the processing capacity on the consumer end we can simply start more consumer processes. Kafka will automatically rebalance the topic/partition assignments across the population of all available consumers.

### Parser bug

Assume that a parser bug caused misformatted output being written to s3 starting with [Kafka] message offset <some_offset>. The output file name format allows us to easily identify misformatted files to remove. If <some_offset> is still in kafka, we can simply reset topic/partition offsets in Zookeeper to that value. Otherwise, we can write a simple MR applying the exactly same parsing logic and upload policy as the consumer. The nice thing about this approach is that backfilling can be done in parallel with the ongoing message processing. I.e., there is no need to take the consumers down while fixing corrupted output.

### Zookeeper failing

Secor won't be able to commit offsets while zk is down. As soon as zk comes back, Secor will resume commits.

[Apache Kafka]:http://kafka.apache.org/
[Kafka]:http://kafka.apache.org/
[Amazon S3]:http://aws.amazon.com/s3/
[S3]:http://aws.amazon.com/s3/
[Hive]:http://hive.apache.org/
