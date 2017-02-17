#!/bin/bash
set -e


SECOR_CONFIG=''

if [ -z "$ZOOKEEPER_QUORUM" ]; then
	echo "ZOOKEEPER_QUORUM variable not set, launch with -e ZOOKEEPER_QUORUM=zookeeper:2181"
    exit 1
else
    SECOR_CONFIG="$SECOR_CONFIG -Dzookeeper.quorum=$ZOOKEEPER_QUORUM"
    echo "zookeeper.quorum=$ZOOKEEPER_QUORUM"
fi

if [[ ! -z "$KAFKA_SEED_BROKER_HOST" ]]; then
	SECOR_CONFIG="$SECOR_CONFIG -Dkafka.seed.broker.host=$KAFKA_SEED_BROKER_HOST"
    echo "kafka.seed.broker.host=$KAFKA_SEED_BROKER_HOST"
fi
if [[ ! -z "$KAFKA_SEED_BROKER_PORT" ]]; then
	SECOR_CONFIG="$SECOR_CONFIG -Dkafka.seed.broker.port=$KAFKA_SEED_BROKER_PORT"
    echo "kafka.seed.broker.port=$KAFKA_SEED_BROKER_PORT"
fi

if [[ ! -z "$SECOR_GROUP" ]]; then
    SECOR_CONFIG="$SECOR_CONFIG -Dsecor.kafka.group=$SECOR_GROUP"
    echo "secor.kafka.group=$SECOR_GROUP"
fi


if [[ ! -z "$AWS_ACCESS_KEY" ]]; then
	SECOR_CONFIG="$SECOR_CONFIG -Daws.access.key=$AWS_ACCESS_KEY"
fi
if [[ ! -z "$AWS_SECRET_KEY" ]]; then
	SECOR_CONFIG="$SECOR_CONFIG -Daws.secret.key=$AWS_SECRET_KEY"
fi
if [[ ! -z "$SECOR_S3_BUCKET" ]]; then
    SECOR_CONFIG="$SECOR_CONFIG -Dsecor.s3.bucket=$SECOR_S3_BUCKET"
    echo "secor.s3.bucket=$SECOR_S3_BUCKET"
fi
if [[ ! -z "$S3_PATH" ]]; then
    SECOR_CONFIG="$SECOR_CONFIG -Dsecor.s3.path=$S3_PATH"
    echo "secor.s3.path=$S3_PATH"
fi


if [[ ! -z "$SECOR_MAX_FILE_BYTES" ]]; then
	SECOR_CONFIG="$SECOR_CONFIG -Dsecor.max.file.size.bytes=$SECOR_MAX_FILE_BYTES"
    echo "secor.max.file.size.bytes=$SECOR_MAX_FILE_BYTES"
fi
if [[ ! -z "$SECOR_MAX_FILE_SECONDS" ]]; then
	SECOR_CONFIG="$SECOR_CONFIG -Dsecor.max.file.age.seconds=$SECOR_MAX_FILE_SECONDS"
    echo "secor.max.file.age.seconds=$SECOR_MAX_FILE_SECONDS"
fi


if [[ ! -z "$SECOR_KAFKA_TOPIC_FILTER" ]]; then
	SECOR_CONFIG="$SECOR_CONFIG -Dsecor.kafka.topic_filter=$SECOR_KAFKA_TOPIC_FILTER"
    echo "secor.kafka.topic_filter=$SECOR_KAFKA_TOPIC_FILTER"
fi
if [[ ! -z "$SECOR_WRITER_FACTORY" ]]; then
	SECOR_CONFIG="$SECOR_CONFIG -Dsecor.file.reader.writer.factory=$SECOR_WRITER_FACTORY"
    echo "secor.file.reader.writer.factory=$SECOR_WRITER_FACTORY"
fi

SECOR_CONFIG="$SECOR_CONFIG $SECOR_EXTRA_OPTS"


cd /opt/secor


DEFAULT_CLASSPATH="*:lib/*"
CLASSPATH=${CLASSPATH:-$DEFAULT_CLASSPATH}

java -Xmx${JVM_MEMORY:-512m} $JAVA_OPTS -ea -Dsecor_group=events-dev -Dlog4j.configuration=file:./${LOG4J_CONFIGURATION:-log4j.docker.properties} \
        -Dconfig=secor.prod.partition.properties $SECOR_CONFIG \
        -cp $CLASSPATH com.pinterest.secor.main.ConsumerMain
