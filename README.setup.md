# Secor Setup/Configuration Guide

## Setup Guide

##### Get Secor code
```sh
git clone [git-repo-url] secor
cd secor
```

##### Customize configuration parameters
Edit `src/main/config/*.properties` files to specify parameters describing the environment. Those files contain comments describing the meaning of individual parameters.


##### Create and install jars
```sh
# By default this will install the "kafka-0.10.2.0" profile
mvn package
mkdir ${SECOR_INSTALL_DIR} # directory to place Secor binaries in.
tar -zxvf target/secor-0.1-SNAPSHOT-bin.tar.gz -C ${SECOR_INSTALL_DIR}

# To use the Kafka 2.0.0 kafka libraries with new features such as Kafka header fields and New consumer
mvn -Pkafka-2.0.0
```

##### Run Secor
```sh
cd ${SECOR_INSTALL_DIR}
java -ea -Dsecor_group=secor_partition \
  -Dlog4j.configuration=log4j.prod.properties \
  -Dconfig=secor.prod.partition.properties \
  -cp secor-0.1-SNAPSHOT.jar:lib/* \
  com.pinterest.secor.main.ConsumerMain
```

Please take note that Secor **requires** JRE8 for it's runtime as source code uses JRE8 language features.
JRE9 and JRE10 is *untested*.

## Configurations for your environment

### Configuration files

Most of the secor configurations are captured in secor properties files in src/main/config/, they are organized based prod/dev/test deployment mode.  There are also example properties file for people doing message hourly ingestion/finalization and uploading to various destinations (S3, Azure, GKS).  You can specify which config file to pick up when you run secor with `-Dconfig=<your-config-file>`

The config files are overlaid to share common definitions.  The most common chain of property files are:

- **secor.prod.partition.properties**: properties specific to secor parititioner in production deployment mode.  Secor partitioner is the most common mode to run Secor which partition the output files in date or hour folders.  There is also a Secor backup mode which backs up Kafka messages using offset orders (see Offset parser in the below Output group section).  You might need to configure `secor.s3.path`
- **secor.prod.properties**: properties common to both backup and partition mode.  You will need to setup `kafka.seed.broker.host`, `zookeeper.quorum`, `secor.s3.bucket`
- **secor.common.properties**: properties common to prod/dev/test deployment mode.  You will need to setup `secor.kafka.topic_filter`

If you feel the overlay of properties files a bit confusing, feel free to copy all the properties into one flat property file and customize for your own environment

## Output grouping

One of the convenience features of Secor is the ability to group messages and save them under common file prefixes. The partitioning is controlled by a message parser. Secor comes with the following parsers:

### Output grouping based on message timestamp

- **Thrift date parser**: parser that extracts timestamps from thrift messages and groups the output based on the date (at a day granularity). To keep things simple, this parser assumes that the timestamp is carried in the first field (id 1) of the thrift message schema by default. The field id can be changed by setting ```message.timestamp.id``` as long as the field is at the top level of the thrift object (i.e. it is not in a nested structure). The timestamp may be expressed either in seconds or milliseconds, or nanoseconds since the epoch. The output goes to date-partitioned paths (e.g., ```s3n://bucket/topic/dt=2014-05-01```, ```s3n://bucket/topic/dt=2014-05-02```). Date partitioning is particularly convenient if the output is to be consumed by ETL tools such as [Hive]. To use this parser, start Secor with properties file [secor.prod.partition.properties](src/main/config/secor.prod.partition.properties). Note the ```message.timestamp.name``` property has no effect on the thrift parsing, which is determined by the field id.

- **JSON timestamp parser**: parser that extracts UNIX timestamps from JSON messages and groups the output based on the date, similar to the Thrift parser above. To use this parser, start Secor with properties file [secor.prod.partition.properties](src/main/config/secor.prod.partition.properties) and set `secor.message.parser.class=com.pinterest.secor.parser.JsonMessageParser`. You may override the field used to extract the timestamp by setting the "message.timestamp.name" property.

- **Avro timestamp parser**: parser that extracts UNIX timestamps from AVRO messages and groups the output based on the date, similar to the Thrift parser above. To use this parser, start Secor with properties file [secor.prod.partition.properties](src/main/config/secor.prod.partition.properties) and set `secor.message.parser.class=com.pinterest.secor.parser.AvroMessageParser`. You may override the field used to extract the timestamp by setting the "message.timestamp.name" property.

- **JSON ISO 8601 date parser**: Assumes your timestamp field uses ISO 8601. To use this parser, start Secor with properties file [secor.prod.partition.properties](src/main/config/secor.prod.partition.properties) and set `secor.message.parser.class=com.pinterest.secor.parser.Iso8601MessageParser`. You may override the field used to extract the timestamp by setting the "message.timestamp.name" property.

- **MessagePack date parser**: parser that extracts timestamps from MessagePack messages and groups the output based on the date, similar to the Thrift and JSON parser. To use this parser, set `secor.message.parser.class=com.pinterest.secor.parser.MessagePackParser`. Like the Thrift parser, the timestamp may be expressed either in seconds or milliseconds, or nanoseconds since the epoch and respects the "message.timestamp.name" property.

- **[Protocol Buffers]** date parser: parser that extracts timestamps from protobuf messages and groups the output based on the date, similar to the Thrift, JSON or MessagePack parser. To use this parser, set `secor.message.parser.class=com.pinterest.secor.parser.ProtobufMessageParser`. Like the Thrift parser, the timestamp may be expressed either in seconds or milliseconds, or nanoseconds since the epoch and respects the "message.timestamp.name" property.

- **Output grouping with Flexible partitions**: The default partitioning granularity for date, hours and minutes have prefix for convenient consumption for `Hive`. If you require different naming of partition with(out) prefix and other date, hour or minute format update the following properties in `secor.common.properties`

          partitioner.granularity.date.prefix=dt=
          partitioner.granularity.hour.prefix=hr=
          partitioner.granularity.minute.prefix=min=

          partitioner.granularity.date.format=yyyy-MM-dd
          partitioner.granularity.hour.format=HH
          partitioner.granularity.minute.format=mm
          
### Output grouping based on Kafka message offset

Grouping output files based on Kafka message offset has the advantage of maintaining message order exactly as it is in Kafka queue.  In comparison, grouping output files based on message timestamp can lose the message order if there are late arrival messages.

- **offset parser**: parser that groups messages based on offset ranges. E.g., messages with offsets in range 0 to 999 will end up under ```s3n://bucket/topic/offset=0/```, offsets 1000 to 2000 will go to ```s3n://bucket/topic/offset=1000/```. To use this parser, start Secor with properties file [secor.prod.backup.properties](src/main/config/secor.prod.backup.properties).

If none of the parsers available out-of-the-box is suitable for your use case, note that it is very easy to implement a custom parser. All you have to do is to extend [MessageParser](src/main/java/com/pinterest/secor/parser/MessageParser.java) and tell Secor to use your parser by setting ```secor.message.parser.class``` in the properties file.


## Output File Formats

Currently secor supports the following output formats

- **Sequence Files**: Flat file containing binary key value pairs. To use this format, set `secor.file.reader.writer.factory=com.pinterest.secor.io.impl.SequenceFileReaderWriterFactory` option.

- **Delimited Text Files**: A new line delimited raw text file. To use this format, set `secor.file.reader.writer.factory=com.pinterest.secor.io.impl.DelimitedTextFileReaderWriterFactory` option.

- **ORC Files**: Optimized row columnar format. To use this format, set `secor.file.reader.writer.factory=com.pinterest.secor.io.impl.JsonORCFileReaderWriterFactory` option. Additionally, ORC schema must be specified per topic like this `secor.orc.message.schema.<topic>=<orc schema>`. If all Kafka topics receive same format data then this option can be used `secor.orc.message.schema.*=<orc schema>`. User can implement custom ORC schema provider by implementing ORCSchemaProvider interface and the new provider class should be specified using option `secor.orc.schema.provider=<orc schema provider class name>`. By default this property is DefaultORCSchemaProvider.

- **[Parquet] Files (for Protobuf messages)**: Columnar storage format. To use this output format, set `secor.file.reader.writer.factory=com.pinterest.secor.io.impl.ProtobufParquetFileReaderWriterFactory` option. In addition, Protobuf message class per Kafka topic must be defined using option `secor.protobuf.message.class.<topic>=<protobuf class name>`. If all Kafka topics transfer the same protobuf message type, set `secor.protobuf.message.class.*=<protobuf class name>`.

- **[Parquet] Files (for JSON messages)**: Columnar storage format. In addition to setting all options necessary to write Protobuf to Parquet (see above), the JSON topics must be explicitly defined using the option `secor.topic.message.format.<topic>=JSON` or `secor.topic.message.format.*=JSON` if all Kafka topics use JSON. The protobuf classes defined per topic will be used as intermediaries between the JSON messages and Parquet files.

- **[Parquet] Files (for Thrift messages)**: Columnar storage format. To use this output format, set `secor.file.reader.writer.factory=com.pinterest.secor.io.impl.ThriftParquetFileReaderWriterFactory` option. In addition, thrift message class per Kafka topic must be defined using option `secor.thrift.message.class.<topic>=<thrift class name>`. If all Kafka topics transfer the same thrift message type, set `secor.thrift.message.class.*=<thrift class name>`. It is assumed all messages use the same thrift protocol. Thrift protocol is set in `secor.thrift.protocol.class`.

- **[Parquet] Files (for Avro messages)**: Columnar storage format. To use this output format, set `secor.file.reader.writer.factory=com.pinterest.secor.io.impl.AvroParquetFileReaderWriterFactory` option. The `schema.registry.url` option must be set. 

- **Gzip upload format**:  To enable compression on uploaded files to the cloud, in `secor.common.properties` set `secor.compression.codec` to a valid compression codec implementing  `org.apache.hadoop.io.compress.CompressionCodec` interface, such as `org.apache.hadoop.io.compress.GzipCodec`.


## Tools
Secor comes with a number of tools for operational help.

##### Partition finalizer
Topic finalizer writes _SUCCESS files to date partitions that very likely won't be receiving any new messages and (optionally) adds the corresponding dates to [Hive] through [Qubole] API.

```sh
java -ea -Dlog4j.configuration=log4j.prod.properties -Dconfig=secor.prod.backup.properties -cp "secor-0.1-SNAPSHOT.jar:lib/*" com.pinterest.secor.main.PartitionFinalizerMain
```

##### Progress monitor
Progress monitor exports offset consumption lags per topic partition to [OpenTSDB] / [statsD]. Lags track how far Secor is behind the producers.

```sh
java -ea -Dlog4j.configuration=log4j.prod.properties -Dconfig=secor.prod.backup.properties -cp "secor-0.1-SNAPSHOT.jar:lib/*" com.pinterest.secor.main.ProgressMonitorMain
```

Set `monitoring.interval.seconds` to a value larger than 0 to run in a loop, exporting stats every `monitoring.interval.seconds` seconds.

##### Log file printer
Log file printer displays the content of a log file.

```sh
java -ea -Dlog4j.configuration=log4j.prod.properties -Dconfig=secor.prod.backup.properties -cp "secor-0.1-SNAPSHOT.jar:lib/*" com.pinterest.secor.main.LogFilePrinterMain -f s3n://bucket/path
```

##### Log file verifier
Log file verifier checks the consistency of log files.

```sh
java -ea -Dlog4j.configuration=log4j.prod.properties -Dconfig=secor.prod.backup.properties -cp "secor-0.1-SNAPSHOT.jar:lib/*" com.pinterest.secor.main.LogFileVerifierMain -t topic -q
```

## Secor configuration for Kubernetes/GKE environment

Extra Setup instruction for Kubernetes/GKE environment is available in [DESIGN.kubernetes.md](DESIGN.kubernetes.md).
