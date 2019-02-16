# Pinterest Secor

![Build Status](https://travis-ci.org/pinterest/secor.svg)

Secor is a service persisting [Kafka] logs to [Amazon S3],
[Google Cloud Storage], [Microsoft Azure Blob Storage] and [Openstack Swift].

## Key features ##

  - **Strong Consistency**: as long as [Kafka] is not dropping messages (e.g.,
    due to aggressive cleanup policy) before Secor is able to read them, it is
    guaranteed that each message will be saved in exactly one [S3] file. This
    property is not compromised by the notorious temporal inconsistency of
    [Amazon S3] caused by the [eventual consistency] model.

  - **Fault Tolerance**: any component of Secor is allowed to crash at any given
    point without compromising data integrity.

  - **Load Distribution**: Secor may be distributed across multiple machines.

  - **Horizontal Scalability**: scaling the system out to handle more load is as
    easy as starting extra Secor processes. Reducing the resource footprint can
    be achieved by killing any of the running Secor processes. Neither ramping
    up nor down has any impact on data consistency.

  - **Output Partitioning**: Secor parses incoming messages and puts them under
    partitioned s3 paths to enable direct import into systems like [Hive]. day,
    hour, minute level partitions are supported by secor.

  - **Configurable Upload Policies**: commit points controlling when data is
    persisted in [Amazon S3] are configured through size-based and time-based
    policies (e.g., upload data when local buffer reaches size of 100MB and at
    least once per hour).

  - **Monitoring**: metrics tracking various performance properties are exposed
    through [Ostrich] and optionally exported to [OpenTSDB]/[statsD].

  - **Customizability**: external log message parser may be loaded by updating
    the configuration.

  - **Event Transformation**: external message level transformation can be done
    by using customized class.

  - **Qubole Interface**: Secor connects to [Qubole] to add finalized output
    partitions to Hive tables.

## Setup Guide

##### Get Secor code

    $ git clone https://github.com/pinterest/secor.git secor
    $ cd secor

##### Customize configuration parameters

Edit `src/main/config/*.properties` files to specify parameters describing the
environment. Those files contain comments describing the meaning of individual
parameters.

##### Create and install jars

By default this will install the active profile profile:

    $ mvn help:all-profiles \
      | grep "Profile Id: kafka-" \
      | grep "Active: true" \
      | awk '{print $3}'
    kafka-0.10.2.0

Build secor package:

    $ mvn package

Discover secor version and set secor install directory:

    $ export SECOR_VERSION="$(ls -1 target/secor-*.jar \
      | head -n1 \
      | cut -d- -f2-3 \
      | sed s/.jar//)"
    $ export SECOR_INSTALL_DIR="/opt/secor"

Create directory to place secor binaries in and extract secor tarball:

    $ mkdir -p ${SECOR_INSTALL_DIR}
    $ tar xvf target/secor-${SECOR_VERSION}-bin.tar.gz -C ${SECOR_INSTALL_DIR}

To use other versions of kafka pic one of the list:

    $ mvn help:all-profiles \
      | grep "Profile Id: kafka-" \
      | awk '{print $3}' \
      | sort
    kafka-0.10.0.1
    kafka-0.10.2.0
    kafka-0.8.2.1
    kafka-1.0.0
    kafka-2.0.0

And pass as maven profile parameter, ie: to use the kafka 1.0.0 libraries with
scala 2.11:

    $ mvn -Pkafka-1.0.0

##### Run tests (optional)

    $ cd ${SECOR_INSTALL_DIR}
    $ ./scripts/run_tests.sh

Or

    MVN_PROFILE=<profile> ./scripts/run_tests.sh

##### Run Secor

    $ cd ${SECOR_INSTALL_DIR}
    $ java -ea -cp secor-${SECOR_VERSION}.jar:lib/* \
        -Dsecor_group=secor_backup \
        -Dlog4j.configuration=log4j.prod.properties \
        -Dconfig=secor.prod.backup.properties \
        com.pinterest.secor.main.ConsumerMain

Please take note that Secor **requires** JRE8 for it's runtime as source code
uses JRE8 language features.

JRE9 and above are *untested*.

## Output grouping

One of the convenience features of Secor is the ability to group messages and
save them under common file prefixes. The partitioning is controlled by a
message parser. Secor comes with the following parsers:

  - **Offset parser**: parser that groups messages based on offset ranges. E.g.,
    messages with offsets in range 0 to 999 will end up under
    `s3n://bucket/topic/offset=0/`, offsets 1000 to 2000 will go to
    `s3n://bucket/topic/offset=1000/`. To use this parser, start Secor with
    properties file [secor.prod.backup.properties].

  - **[Thrift] date parser**: parser that extracts timestamps from [Thrift]
    messages and groups the output based on the date (at a day granularity). To
    keep things simple, this parser assumes that the timestamp is carried in the
    first field (id 1) of the [Thrift] message schema by default. The field id
    can be changed by setting `message.timestamp.id` as long as the field is at
    the top level of the thrift object (i.e. it is not in a nested structure).
    The timestamp may be expressed either in seconds or milliseconds, or
    nanoseconds since the epoch. The output goes to date-partitioned paths
    (e.g., `s3n://bucket/topic/dt=2014-05-01`,
    `s3n://bucket/topic/dt=2014-05-02`). Date partitioning is particularly
    convenient if the output is to be consumed by ETL tools such as [Hive]. To
    use this parser, start Secor with properties file
    [secor.prod.partition.properties]. Note the `message.timestamp.name`
    property has no effect on the thrift parsing, which is determined by the
    field id.

  - **[JSON] timestamp parser**: parser that extracts UNIX timestamps from
    [JSON] messages and groups the output based on the date, similar to the
    [Thrift] parser above. To use this parser, start Secor with properties file
    [secor.prod.partition.properties] and set `secor.message.parser.class` to
    [com.pinterest.secor.parser.JsonMessageParser]. You may override the field
    used to extract the timestamp by setting the `message.timestamp.name`
    property.

  - **[Avro] timestamp parser**: parser that extracts UNIX timestamps from
    [Avro] messages and groups the output based on the date, similar to the
    [Thrift] parser above. To use this parser, start Secor with properties file
    [secor.prod.partition.properties] and set `secor.message.parser.class` to
    [com.pinterest.secor.parser.AvroMessageParser]. You may override the field
    used to extract the timestamp by setting the `message.timestamp.name`
    property.

  - **[JSON] ISO 8601 date parser**: Assumes your timestamp field uses
    [ISO 8601]. To use this parser, start Secor with properties file
    [secor.prod.partition.properties] and set `secor.message.parser.class` to
    [com.pinterest.secor.parser.Iso8601MessageParser]. You may override the
    field used to extract the timestamp by setting the `message.timestamp.name`
    property.

  - **[MessagePack] date parser**: parser that extracts timestamps from
    [MessagePack] messages and groups the output based on the date, similar to
    the [Thrift] and [JSON] parser. To use this parser, set
    `secor.message.parser.class` to
    [com.pinterest.secor.parser.MessagePackParser]. Like the Thrift parser, the
    timestamp may be expressed either in seconds or milliseconds, or nanoseconds
    since the epoch and respects the `message.timestamp.name` property.

  - **[Protobuf] date parser**: parser that extracts timestamps from protobuf
    messages and groups the output based on the date, similar to the [Thrift],
    [JSON] or [MessagePack] parser. To use this parser, set
    `secor.message.parser.class` to
    [com.pinterest.secor.parser.ProtobufMessageParser]. Like the Thrift parser,
    the timestamp may be expressed either in seconds or milliseconds, or
    nanoseconds since the epoch and respects the `message.timestamp.name`
    property.

  - **Output grouping with Flexible partitions**: The default partitioning
    granularity for date, hours and minutes have prefix for convenient
    consumption for [Hive]. If you require different naming of partition
    with(out) prefix and other date, hour or minute format update the following
    properties in `secor.common.properties`:

        partitioner.granularity.date.prefix=dt=
        partitioner.granularity.hour.prefix=hr=
        partitioner.granularity.minute.prefix=min=
        partitioner.granularity.date.format=yyyy-MM-dd
        partitioner.granularity.hour.format=HH
        partitioner.granularity.minute.format=mm
    
If none of the parsers available out-of-the-box is suitable for your use case,
note that it is very easy to implement a custom parser. All you have to do is to
extend [com.pinterest.secor.parser.MessageParser] and tell Secor to use your
parser by setting `secor.message.parser.class` in the properties file.

## Output File Formats

Currently secor supports the following output formats:

  - **[SequenceFile]**: Flat file containing binary key value pairs. To use this
    format, set `secor.file.reader.writer.factory` to
    [com.pinterest.secor.io.impl.SequenceFileReaderWriterFactory] option.

  - **Delimited Text Files**: A new line delimited raw text file. To use this
    format, set `secor.file.reader.writer.factory` to
    [com.pinterest.secor.io.impl.DelimitedTextFileReaderWriterFactory] option.

  - **[ORC] Files**: Optimized row columnar format. To use this format, set
    `secor.file.reader.writer.factory` to
    [com.pinterest.secor.io.impl.JsonORCFileReaderWriterFactory] option.
    Additionally, [ORC] schema must be specified per topic like this
    `secor.orc.message.schema.<topic>=<orc schema>`. If all Kafka topics receive
    same format data then this option can be used
    `secor.orc.message.schema.*=<orc schema>`. User can implement custom [ORC]
    schema provider by implementing
    [com.pinterest.secor.util.orc.schema.ORCSchemaProvider] interface and the
    new provider class should be specified using option
    `secor.orc.schema.provider=<orc schema provider class name>`. By default
    this property is
    [com.pinterest.secor.util.orc.schema.DefaultORCSchemaProvider].

  - **[Parquet] Files (for [Protobuf] messages)**: Columnar storage format. To
    use this output format, set `secor.file.reader.writer.factory` to
    [com.pinterest.secor.io.impl.ProtobufParquetFileReaderWriterFactory] option.
    In addition, Protobuf message class per Kafka topic must be defined using
    option `secor.protobuf.message.class.<topic>=<protobuf class name>`. If all
    Kafka topics transfer the same protobuf message type, set
    `secor.protobuf.message.class.*=<protobuf class name>`.

  - **[Parquet] Files (for [JSON] messages)**: Columnar storage format. In
    addition to setting all options necessary to
    write Protobuf to Parquet (see above), the [JSON] topics must be explicitly
    defined using the option `secor.topic.message.format.<topic>=JSON` or
    `secor.topic.message.format.*=JSON` if all Kafka topics use [JSON].
    The protobuf classes defined per topic will be used as intermediaries
    between the JSON messages and Parquet files.

  - **[Parquet] Files (for [Thrift] messages)**: Columnar storage format.
    To use this output format, set `secor.file.reader.writer.factory` to
    [com.pinterest.secor.io.impl.ThriftParquetFileReaderWriterFactory] option.
    In addition, thrift message class per Kafka topic must be defined using
    option `secor.thrift.message.class.<topic>=<thrift class name>`. If all
    Kafka topics transfer the same thrift message type, set
    `secor.thrift.message.class.*=<thrift class name>`. It is assumed all
    messages use the same thrift protocol. [Thrift] protocol is set in
    `secor.thrift.protocol.class`.

  - **[Parquet] Files (for [Avro] messages)**: Columnar storage format. To use
    this output format, set `secor.file.reader.writer.factory` to
    [com.pinterest.secor.io.impl.AvroParquetFileReaderWriterFactory] option. The
    `schema.registry.url` option must be set. 

  - **[Gzip] upload format**: To enable compression on uploaded files to the
    cloud, in `secor.common.properties` set `secor.compression.codec` to a valid
    compression codec implementing
    [org.apache.hadoop.io.compress.CompressionCodec] interface, such as
    [org.apache.hadoop.io.compress.GzipCodec].

## Tools

Secor comes with a number of tools implementing interactions with the
environment.

##### Log file printer

Log file printer displays the content of a log file.

    $ java -ea -cp "secor-${SECOR_VERSION}.jar:lib/*" \
        -Dlog4j.configuration=log4j.prod.properties \
        -Dconfig=secor.prod.backup.properties \
        com.pinterest.secor.main.LogFilePrinterMain -f s3n://bucket/path

##### Log file verifier

Log file verifier checks the consistency of log files.

    $ java -ea -cp "secor-${SECOR_VERSION}.jar:lib/*" \
        -Dlog4j.configuration=log4j.prod.properties \
        -Dconfig=secor.prod.backup.properties \
        com.pinterest.secor.main.LogFileVerifierMain -t topic -q

##### Partition finalizer

Topic finalizer writes _SUCCESS files to date partitions that very likely won't
be receiving any new messages and (optionally) adds the corresponding dates to
[Hive] through [Qubole] API.

    $ java -ea -cp "secor-${SECOR_VERSION}.jar:lib/*" \
        -Dlog4j.configuration=log4j.prod.properties \
        -Dconfig=secor.prod.backup.properties \
        com.pinterest.secor.main.PartitionFinalizerMain

##### Progress monitor

Progress monitor exports offset consumption lags per topic partition to
[OpenTSDB]/[statsD]. Lags track how far Secor is behind the producers.

    $ java -ea -cp "secor-${SECOR_VERSION}.jar:lib/*" \
        -Dlog4j.configuration=log4j.prod.properties \
        -Dconfig=secor.prod.backup.properties \
        com.pinterest.secor.main.ProgressMonitorMain

Set `monitoring.interval.seconds` to a value larger than 0 to run in a loop,
exporting stats every `monitoring.interval.seconds` seconds.

## Documentation

  - [doc/design.md] for design details. 
  - [doc/k8s.md] for setting up on kubernetes/GKE
  - [doc/dev.md] for development notes

## License

Secor is distributed under [Apache License, Version 2.0].

## Maintainers

 - [Pawel Garbacki](https://github.com/pgarbacki)
 - [Henry Cai](https://github.com/HenryCaiHaiying)

## Contributors

 - [Andy Kramolisch](https://github.com/andykram)
 - [Brenden Matthews](https://github.com/brndnmtthws)
 - [Lucas Zago](https://github.com/zago)
 - [James Green](https://github.com/jfgreen)
 - [Praveen Murugesan](https://github.com/lefthandmagic)
 - [Zack Dever](https://github.com/zackdever)
 - [Leo Woessner](https://github.com/estezz)
 - [Jerome Gagnon](https://github.com/jgagnon1)
 - [Taichi Nakashima](https://github.com/tcnksm)
 - [Lovenish Goyal](https://github.com/lovenishgoyal)
 - [Ahsan Nabi Dar](https://github.com/ahsandar)
 - [Ashish Kumar](https://github.com/ashubhumca)
 - [Ashwin Sinha](https://github.com/tygrash)
 - [Avi Chad-Friedman](https://github.com/achad4)

## Companies who use Secor

 - [Airbnb](https://www.airbnb.com)
 - [Pinterest](https://www.pinterest.com)
 - [Strava](https://www.strava.com)
 - [TiVo](https://www.tivo.com)
 - [Yelp](https://www.yelp.com)
 - [Credit Karma](https://www.creditkarma.com)
 - [VarageSale](https://www.varagesale.com)
 - [Skyscanner](https://www.skyscanner.net)
 - [Nextperf](https://www.nextperf.com)
 - [Zalando](https://www.zalando.com)
 - [Rakuten](https://techblog.rakuten.co.jp)
 - [Appsflyer](https://www.appsflyer.com)
 - [Wego](https://www.wego.com)
 - [GO-JEK](https://gojekengineering.com)
 - [Branch](https://branch.io)
 - [Viacom](https://www.viacom.com)
 - [Simplaex](https://www.simplaex.com)
 - [Zapier](https://www.zapier.com)

## Help

If you have any questions or comments, you can reach us at
[secor-users@googlegroups.com]

[Kafka]: https://kafka.apache.org
[Hive]: https://hive.apache.org
[Ostrich]: https://github.com/twitter/ostrich
[OpenTSDB]: https://opentsdb.net
[Qubole]: https://www.qubole.com
[statsD]: https://github.com/etsy/statsd

[Amazon S3]: https://aws.amazon.com/s3
[eventual consistency]:
https://docs.aws.amazon.com/AmazonS3/latest/dev/Introduction.html#ConsistencyMode
[Microsoft Azure Blob Storage]:
https://azure.microsoft.com/en-us/services/storage/blobs
[Google Cloud Storage]: https://cloud.google.com/storage
[Openstack Swift]: https://swift.openstack.org

[SequenceFile]: https://wiki.apache.org/hadoop/SequenceFile
[MessagePack]: https://msgpack.org
[Protobuf]: https://developers.google.com/protocol-buffers
[Parquet]: https://parquet.apache.org
[Avro]: https://avro.apache.org
[ORC]: https://orc.apache.org
[JSON]: https://json.org
[Thrift]: https://thrift.apache.org
[Gzip]: https://www.gzip.org

[ISO 8601]: https://en.wikipedia.org/wiki/ISO_8601
[secor-users@googlegroups.com]:
https://groups.google.com/forum/#!forum/secor-users
[Apache License, Version 2.0]: http://www.apache.org/licenses/LICENSE-2.0.html

[doc/design.md]: doc/design.md
[doc/k8s.md]: doc/k8s.md
[doc/dev.md]: doc/dev.md
[secor.prod.backup.properties]: src/main/config/secor.prod.backup.properties
[secor.prod.partition.properties]:
src/main/config/secor.prod.partition.properties
[com.pinterest.secor.parser.MessageParser]:
src/main/java/com/pinterest/secor/parser/MessageParser.java
[com.pinterest.secor.parser.ProtobufMessageParser]:
src/main/java/com/pinterest/secor/parser/ProtobufMessageParser.java
[com.pinterest.secor.parser.JsonMessageParser]:
src/main/java/com/pinterest/secor/parser/JsonMessageParser.java
[com.pinterest.secor.parser.Iso8601MessageParser]:
src/main/java/com/pinterest/secor/parser/Iso8601MessageParser.java
[com.pinterest.secor.parser.MessagePackParser]:
src/main/java/com/pinterest/secor/parser/MessagePackParser.java
[com.pinterest.secor.io.impl.ProtobufParquetFileReaderWriterFactory]:
src/main/java/com/pinterest/secor/io/impl/ProtobufParquetFileReaderWriterFactory.java
[com.pinterest.secor.io.impl.DelimitedTextFileReaderWriterFactory]:
src/main/java/com/pinterest/secor/io/impl/DelimitedTextFileReaderWriterFactory.java
[com.pinterest.secor.io.impl.SequenceFileReaderWriterFactory]:
src/main/java/com/pinterest/secor/io/impl/SequenceFileReaderWriterFactory.java
[com.pinterest.secor.util.orc.schema.DefaultORCSchemaProvider]:
src/main/java/com/pinterest/secor/util/orc/schema/DefaultORCSchemaProvider.java
[com.pinterest.secor.util.orc.schema.ORCSchemaProvider]:
src/main/java/com/pinterest/secor/util/orc/schema/ORCSchemaProvider.java
[com.pinterest.secor.io.impl.JsonORCFileReaderWriterFactory]:
src/main/java/com/pinterest/secor/io/impl/JsonORCFileReaderWriterFactory.java
[com.pinterest.secor.io.impl.ThriftParquetFileReaderWriterFactory]:
src/main/java/com/pinterest/secor/io/impl/ThriftParquetFileReaderWriterFactory.java
[com.pinterest.secor.io.impl.AvroParquetFileReaderWriterFactory]:
src/main/java/com/pinterest/secor/io/impl/AvroParquetFileReaderWriterFactory.java
[org.apache.hadoop.io.compress.CompressionCodec]:
https://github.com/apache/hadoop/blob/release-2.7.0/hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/io/compress/CompressionCodec.java
[org.apache.hadoop.io.compress.GzipCodec]:
https://github.com/apache/hadoop/blob/release-2.7.0/hadoop-common-project/hadoop-common/src/main/java/org/apache/hadoop/io/compress/GzipCodec.java
