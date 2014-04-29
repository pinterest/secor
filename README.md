# Pinterest Secor

Secor is a service persisting [Kafka] logs to [Amazon S3].

## Key features
  - **strong consistency**: as long as [Kafka] is not dropping messages (e.g., due to aggresive cleanup policy) before Secor is able to read them, it is guaranteed that each message will be saved in exacly one [S3] file. This property is not compromized by the notorious temporal inconsisteny of [S3] caused by the [eventual consistency] model,
  - **fault tolerance**: any component of Secor is allowed to crash at any given point without compromising data integrity,
  - **load distribution**: Secor may be distributed across multiple machines,
  - **horizontal scalability**: scaling the system out to handle more load is as easy as starting extra Secor processes. Reducing the resource footprint can be achieved by killing any of the running Secor processes. Neither ramping up nor down has any impact on data consistency,
  - **output partitioning**: Secor parses incoming messages and puts them under partitioned s3 paths to enable direct import into systems like [Hive],
  - **configurable upload policies**: commit points controlling when data is persisted in S3 are configured through size-based and time-based policies (e.g., upload data when local buffer reaches size of 100MB and at least once per hour),
  - **monitoring**: metrics tracking various performace properties are exposed through [Ostrich] and optionaly exported to [OpenTSDB],
  - **customizability**: external log message parser may be loaded by updating the configuration,
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
mvn package
mkdir ${SECOR_INSTALL_DIR} # directory to place Secor binaries in.
tar -zxvf target/secor-0.1-SNAPSHOT-bin.tar.gz -C ${SECOR_INSTALL_DIR}
```

##### Run tests (optional)
```sh
cd ${SECOR_INSTALL_DIR}
./scripts/run_tests.sh
```

##### Run Secor
```sh
cd ${SECOR_INSTALL_DIR}
java -ea -Dsecor_group=secor_partition -Dlog4j.configuration=log4j.prod.properties -Dconfig=secor.prod.backup.properties -cp secor-0.1-SNAPSHOT.jar:lib/* com.pinterest.secor.main.ConsumerMain
```

## Tools
Secor comes with a number of tools impelementing interactions with the environment.

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
Topic finalizer writes _SUCCESS files to date partitions that very likely won't be receiving any new messages and (optionaly) adds the corresponding dates to [Hive] through [Qubole] API.

```sh
java -ea -Dlog4j.configuration=log4j.prod.properties -Dconfig=secor.prod.backup.propertie -cp "secor-0.1-SNAPSHOT.jar:lib/*" com.pinterest.secor.main.PartitionFinalizerMain
```

##### Progress monitor
Progress monitor exports offset consumption lags per topic partition to [OpenTSDB]. Lags track how far Secor is behind the producers.

```sh
java -ea -Dlog4j.configuration=log4j.prod.properties -Dconfig=secor.prod.backup.properties -cp "secor-0.1-SNAPSHOT.jar:lib/*" com.pinterest.secor.main.ProgressMonitorMain
```

## Detailed design

Design details are available in [DESIGN.md](DESIGN.md).

## License

Secor is distributed under [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0.html).

## Maintainers
Pawel Garbacki

## Help

If you have any questions or comments, you can reach us at [secor-users@googlegroups.com](secor-users@googlegroups.com)

[Kafka]:http://kafka.apache.org/
[Amazon S3]:http://aws.amazon.com/s3/
[S3]:http://aws.amazon.com/s3/
[eventual consistency]:http://docs.aws.amazon.com/AmazonS3/latest/dev/Introduction.html#ConsistencyMode
[Hive]:http://hive.apache.org/
[Ostrich]: https://github.com/twitter/ostrich
[OpenTSDB]: http://opentsdb.net/
[Qubole]: http://www.qubole.com/

