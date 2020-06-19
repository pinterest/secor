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
  - **monitoring**: metrics tracking various performance properties are exposed through [Ostrich], [Micrometer] and optionally exported to [OpenTSDB] / [statsD],
  - **customizability**: external log message parser may be loaded by updating the configuration,
  - **event transformation**: external message level transformation can be done by using customized class.
  - **Qubole interface**: Secor connects to [Qubole] to add finalized output partitions to Hive tables.

## Release Notes

Release Notes for past versions can be found in [RELEASE.md](RELEASE.md).

## Setup/Configuration Guide

Setup/Configuration instruction is available in [README.setup.md](README.setup.md).

### Secor configuration for Kubernetes/GKE environment

Extra Setup instruction for Kubernetes/GKE environment is available in [README.kubernetes.md](README.kubernetes.md).

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
  * [Avi Chad-Friedman](https://github.com/achad4)


## Companies who use Secor

  * [Airbnb](https://www.airbnb.com)
  * [Appsflyer](https://www.appsflyer.com)
  * [Branch](http://branch.io)
  * [Coupang](https://www.coupang.com)
  * [Credit Karma](https://www.creditkarma.com)
  * [GO-JEK](http://gojekengineering.com/)
  * [Nextperf](http://www.nextperf.com)
  * [PayTM](https://www.paytm.com)
  * [Pinterest](https://www.pinterest.com)
  * [Rakuten](http://techblog.rakuten.co.jp/)
  * [Robinhood](http://www.robinhood.com/)
  * [Simplaex](https://www.simplaex.com/)
  * [Skyscanner](http://www.skyscanner.net)
  * [Strava](https://www.strava.com)
  * [TiVo](https://www.tivo.com)
  * [VarageSale](http://www.varagesale.com)
  * [Viacom](http://www.viacom.com)
  * [Wego](https://www.wego.com)
  * [Yelp](http://www.yelp.com)
  * [Zalando](http://www.zalando.com)
  * [Zapier](https://www.zapier.com)

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
[Micrometer]: https://micrometer.io
[OpenTSDB]: http://opentsdb.net/
[Qubole]: http://www.qubole.com/
[statsD]: https://github.com/etsy/statsd/
[Openstack Swift]: http://swift.openstack.org
[Protocol Buffers]: https://developers.google.com/protocol-buffers/
[Parquet]: https://parquet.apache.org/
