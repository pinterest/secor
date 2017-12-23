# Pinterest Secor

[![Build Status](https://travis-ci.org/pinterest/secor.svg)](https://travis-ci.org/pinterest/secor)

Secor is a service persisting [Kafka] logs to [Amazon S3], [Google Cloud Storage], [Microsoft Azure Blob Storage] and [Openstack Swift].

## Key features ##
  - **strong consistency**: as long as [Kafka] is not dropping messages (e.g., due to aggressive cleanup policy) before Secor is able to read them, it is guaranteed that each message will be saved in exactly one [S3] file. This property is not compromised by the notorious temporal inconsistency of [S3] caused by the [eventual consistency] model,
  - **fault tolerance**: any component of Secor is allowed to crash at any given point without compromising data integrity,
  - **load distribution**: Secor may be distributed across multiple machines,
  - **horizontal scalability**: scaling the system out to handle more load is as easy as starting extra Secor processes. Reducing the resource footprint can be achieved by killing any of the running Secor processes. Neither ramping up nor down has any impact on data consistency,
  - **output partitioning**: Secor parses incoming messages and puts them under partitioned s3 paths to enable direct import into systems like [Hive]. day,hour,minute level partitions are supported by secor
  - **configurable upload policies**: commit points controlling when data is persisted in S3 are configured through size-based and time-based policies (e.g., upload data when local buffer reaches size of 100MB and at least once per hour),
  - **monitoring**: metrics tracking various performance properties are exposed through [Ostrich] and optionally exported to [OpenTSDB] / [statsD],
  - **customizability**: external log message parser may be loaded by updating the configuration,
  - **event transformation**: external message level transformation can be done by using customized class.
  - **Qubole interface**: Secor connects to [Qubole] to add finalized output partitions to Hive tables.

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
# By default this will install the "release" (Kafka 0.10 profile)
mvn package
mkdir ${SECOR_INSTALL_DIR} # directory to place Secor binaries in.
tar -zxvf target/secor-0.1-SNAPSHOT-bin.tar.gz -C ${SECOR_INSTALL_DIR}

# To use the Kafka 0.8 client you should use the kafka-0.8-dev profile
mvn -Pkafka-0.8-dev package
```

##### Run tests (optional)
```sh
cd ${SECOR_INSTALL_DIR}
./scripts/run_tests.sh
```

##### Run Secor
```sh
cd ${SECOR_INSTALL_DIR}
java -ea -Dsecor_group=secor_backup \
  -Dlog4j.configuration=log4j.prod.properties \
  -Dconfig=secor.prod.backup.properties \
  -cp secor-0.1-SNAPSHOT.jar:lib/* \
  com.pinterest.secor.main.ConsumerMain
```

## Output grouping

One of the convenience features of Secor is the ability to group messages and save them under common file prefixes. The partitioning is controlled by a message parser. Secor comes with the following parsers:

- **offset parser**: parser that groups messages based on offset ranges. E.g., messages with offsets in range 0 to 999 will end up under ```s3n://bucket/topic/offset=0/```, offsets 1000 to 2000 will go to ```s3n://bucket/topic/offset=1000/```. To use this parser, start Secor with properties file [secor.prod.backup.properties](src/main/config/secor.prod.backup.properties).

- **Thrift date parser**: parser that extracts timestamps from thrift messages and groups the output based on the date (at a day granularity). To keep things simple, this parser assumes that the timestamp is carried in the first field (id 1) of the thrift message schema by default. The field id can be changed by setting ```message.timestamp.id``` as long as the field is at the top level of the thrift object (i.e. it is not in a nested structure). The timestamp may be expressed either in seconds or milliseconds, or nanoseconds since the epoch. The output goes to date-partitioned paths (e.g., ```s3n://bucket/topic/dt=2014-05-01```, ```s3n://bucket/topic/dt=2014-05-02```). Date partitioning is particularly convenient if the output is to be consumed by ETL tools such as [Hive]. To use this parser, start Secor with properties file [secor.prod.partition.properties](src/main/config/secor.prod.partition.properties). Note the ```message.timestamp.name``` property has no effect on the thrift parsing, which is determined by the field id.

- **JSON timestamp parser**: parser that extracts UNIX timestamps from JSON messages and groups the output based on the date, similar to the Thrift parser above. To use this parser, start Secor with properties file [secor.prod.partition.properties](src/main/config/secor.prod.partition.properties) and set `secor.message.parser.class=com.pinterest.secor.parser.JsonMessageParser`. You may override the field used to extract the timestamp by setting the "message.timestamp.name" property.

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
          

If none of the parsers available out-of-the-box is suitable for your use case, note that it is very easy to implement a custom parser. All you have to do is to extend [MessageParser](src/main/java/com/pinterest/secor/parser/MessageParser.java) and tell Secor to use your parser by setting ```secor.message.parser.class``` in the properties file.


## Output File Formats

Currently secor supports the following output formats

- **Sequence Files**: Flat file containing binary key value pairs. To use this format, set `secor.file.reader.writer.factory=com.pinterest.secor.io.impl.SequenceFileReaderWriterFactory` option.

- **Delimited Text Files**: A new line delimited raw text file. To use this format, set `secor.file.reader.writer.factory=com.pinterest.secor.io.impl.DelimitedTextFileReaderWriterFactory` option.

- **ORC Files**: Optimized row columnar format. To use this format, set `secor.file.reader.writer.factory=com.pinterest.secor.io.impl.JsonORCFileReaderWriterFactory` option. Additionally, ORC schema must be specified per topic like this `secor.orc.message.schema.<topic>=<orc schema>`. If all Kafka topics receive same format data then this option can be used `secor.orc.message.schema.*=<orc schema>`. User can implement custom ORC schema provider by implementing ORCSchemaProvider interface and the new provider class should be specified using option `secor.orc.schema.provider=<orc schema provider class name>`. By default this property is DefaultORCSchemaProvider.

- **[Parquet] Files (for Protobuf messages)**: Columnar storage format. To use this output format, set `secor.file.reader.writer.factory=com.pinterest.secor.io.impl.ProtobufParquetFileReaderWriterFactory` option. In addition, Protobuf message class per Kafka topic must be defined using option `secor.protobuf.message.class.<topic>=<protobuf class name>`. If all Kafka topics transfer the same protobuf message type, set `secor.protobuf.message.class.*=<protobuf class name>`.

- **[Parquet] Files (for Thrift messages)**: Columnar storage format. To use this output format, set `secor.file.reader.writer.factory=com.pinterest.secor.io.impl.ThriftParquetFileReaderWriterFactory` option. In addition, thrift message class per Kafka topic must be defined using option `secor.thrift.message.class.<topic>=<thrift class name>`. If all Kafka topics transfer the same thrift message type, set `secor.thrift.message.class.*=<thrift class name>`. It is asumed all messages use the same thrift protocol. Thrift protocol is set in `secor.thrift.protocol.class`.

- **Gzip upload format**:  To enable compression on uploaded files to the cloud, in `secor.common.properties` set `secor.compression.codec` to a valid compression codec implementing  `org.apache.hadoop.io.compress.CompressionCodec` interface, such as `org.apache.hadoop.io.compress.GzipCodec`.


## Tools
Secor comes with a number of tools implementing interactions with the environment.

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


## Detailed design

Design details are available in [DESIGN.md](DESIGN.md).

## License

Secor is distributed under [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0.html).

## Maintainers
  * [Pawel Garbacki](https://github.com/pgarbacki)
  * [Henry Cai](https://github.com/HenryCaiHaiying)

## Contributors
  * [Andy Kramolisch](https://github.com/andykram)
  * [Brenden Matthews](https://github.com/brndnmtthws)
  * [Lucas Zago](https://github.com/zago)
  * [James Green](https://github.com/jfgreen)
  * [Praveen Murugesan](https://github.com/lefthandmagic)
  * [Zack Dever](https://github.com/zackdever)
  * [Leo Woessner](https://github.com/estezz)
  * [Jerome Gagnon](https://github.com/jgagnon1)
  * [Taichi Nakashima](https://github.com/tcnksm)
  * [Lovenish Goyal](https://github.com/lovenishgoyal)
  * [Ahsan Nabi Dar](https://github.com/ahsandar)
  * [Ashish Kumar](https://github.com/ashubhumca)
  * [Ashwin Sinha](https://github.com/tygrash)


## Companies who use Secor

  * [Airbnb](https://www.airbnb.com)
  * [Pinterest](https://www.pinterest.com)
  * [Strava](https://www.strava.com)
  * [TiVo](https://www.tivo.com)
  * [Yelp](http://www.yelp.com)
  * [Credit Karma](https://www.creditkarma.com)
  * [VarageSale](http://www.varagesale.com)
  * [Skyscanner](http://www.skyscanner.net)
  * [Nextperf](http://www.nextperf.com)
  * [Zalando](http://www.zalando.com)
  * [Rakuten](http://techblog.rakuten.co.jp/)
  * [Appsflyer](https://www.appsflyer.com)
  * [Wego](https://www.wego.com)
  * [GO-JEK](http://gojekengineering.com/)
  * [Branch](http://branch.io)

## Help

If you have any questions or comments, you can reach us at [secor-users@googlegroups.com](https://groups.google.com/forum/#!forum/secor-users)

[Kafka]:http://kafka.apache.org/
[Amazon S3]:http://aws.amazon.com/s3/
[Microsoft Azure Blob Storage]:https://azure.microsoft.com/en-us/services/storage/blobs/
[S3]:http://aws.amazon.com/s3/
[Google Cloud Storage]:https://cloud.google.com/storage/
[eventual consistency]:http://docs.aws.amazon.com/AmazonS3/latest/dev/Introduction.html#ConsistencyMode
[Hive]:http://hive.apache.org/
[Ostrich]: https://github.com/twitter/ostrich
[OpenTSDB]: http://opentsdb.net/
[Qubole]: http://www.qubole.com/
[statsD]: https://github.com/etsy/statsd/
[Openstack Swift]: http://swift.openstack.org
[Protocol Buffers]: https://developers.google.com/protocol-buffers/
[Parquet]: https://parquet.apache.org/
