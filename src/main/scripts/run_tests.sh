#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# End-to-end test exercising the entire stack on the local machine.  The test starts a Zookeeper,
# a Kafka broker, and Secor on local host.  It then creates a topic and publishes a few messages.
# Secor is given some time to consume them.  After that time expires, servers are shut down and
# the output files are verified.
#
# To run the test:
#     cd ${OPTIMUS}/secor
#     mvn package
#     mkdir /tmp/test
#     cd /tmp/test
#     tar -zxvf ~/git/optimus/secor/target/secor-0.2-SNAPSHOT-bin.tar.gz
#
#     # copy Hadoop native libs to lib/, or change HADOOP_NATIVE_LIB_PATH to point to them
#     ./scripts/run_tests.sh
#
# Test logs are available in /tmp/secor_dev/logs/  The output files are in
# s3://pinterest-dev/secor_dev

# Author: Pawel Garbacki (pawel@pinterest.com)

PARENT_DIR=/tmp/secor_dev
LOGS_DIR=${PARENT_DIR}/logs
BUCKET=${SECOR_BUCKET:-test-bucket}
S3_LOGS_DIR=s3://${BUCKET}/secor_dev
SWIFT_CONTAINER=logsContainer
# Should match the secor.swift.containers.for.each.topic value
CONTAINER_PER_TOPIC=false
MESSAGES=100
MESSAGE_TYPE=binary
# For the compression tests to work, set this to the path of the Hadoop native libs.
HADOOP_NATIVE_LIB_PATH=lib
# by default additional opts is empty
ADDITIONAL_OPTS=

# various reader writer options to be used for testing
# note associate array needs bash v4 support
#
declare -A READER_WRITERS
READER_WRITERS[json]=com.pinterest.secor.io.impl.DelimitedTextFileReaderWriterFactory
READER_WRITERS[binary]=com.pinterest.secor.io.impl.SequenceFileReaderWriterFactory

# Hadoop supports multiple implementations of the s3 filesystem
S3_FILESYSTEMS=${S3_FILESYSTEMS:-s3a}

# The minimum wait time is 10 seconds plus delta.  Secor is configured to upload files older than
# 10 seconds and we need to make sure that everything ends up on s3 before starting verification.
WAIT_TIME=${SECOR_WAIT_TIME:-40}
BASE_DIR=$(dirname $0)
CONF_DIR=${BASE_DIR}/..

cloudService="s3"
if [ "$#" != "0" ]; then
   cloudService=${1}
fi

source ${BASE_DIR}/run_common.sh

run_command() {
    echo "running $@"
    eval "$@"
}

check_for_native_libs() {
    files=($(find "${HADOOP_NATIVE_LIB_PATH}" -maxdepth 1 -name "*.so" 2> /dev/null))
    if [ ${#files[@]} -eq 0 ]; then
        echo "Couldn't find Hadoop native libraries, skipping compressed binary tests"
        SKIP_COMPRESSED_BINARY="true"
    fi
}

recreate_dirs() {
    run_command "rm -r -f ${PARENT_DIR}"
    if [ ${cloudService} = "swift" ]; then
	if ${CONTAINER_PER_TOPIC}; then
	   run_command "swift delete test"
        else
           run_command "swift delete ${SWIFT_CONTAINER}"
           sleep 3
           run_command "swift post ${SWIFT_CONTAINER}"
        fi
    else
        if [ -n "${SECOR_LOCAL_S3}" ]; then
            run_command "s3cmd -c ${CONF_DIR}/test.s3cfg ls -r ${S3_LOGS_DIR} | awk '{ print \$4 }' | xargs -L 1 s3cmd -c ${CONF_DIR}/test.s3cfg del"
            run_command "s3cmd -c ${CONF_DIR}/test.s3cfg ls -r ${S3_LOGS_DIR}"
        else
            run_command "s3cmd del --recursive ${S3_LOGS_DIR}"
            run_command "s3cmd ls -r ${S3_LOGS_DIR}"
        fi
    fi
    # create logs directory
    if [ ! -d ${LOGS_DIR} ]; then
        run_command "mkdir -p ${LOGS_DIR}"
    fi
}

start_s3() {
    if [ -n "${SECOR_LOCAL_S3}" ]; then
        if command -v fakes3 > /dev/null 2>&1; then
            run_command "fakes3 --root=/tmp/fakes3 --port=5000 --hostname=localhost > /tmp/fakes3.log 2>&1 &"
            sleep 10
            run_command "s3cmd -c ${CONF_DIR}/test.s3cfg mb s3://${BUCKET}"
        else
            echo "Couldn't find FakeS3 binary, please install it using `gem install fakes3`"
        fi
    fi
}

stop_s3() {
    if [ -n "${SECOR_LOCAL_S3}" ]; then
        run_command "pkill -f 'fakes3' || true"
        run_command "rm -r -f /tmp/fakes3"
    fi
}

start_zookeeper() {
    run_command "${BASE_DIR}/run_kafka_class.sh \
        org.apache.zookeeper.server.quorum.QuorumPeerMain ${CONF_DIR}/zookeeper.test.properties > \
        ${LOGS_DIR}/zookeeper.log 2>&1 &"
}

stop_zookeeper() {
    run_command "pkill -f 'org.apache.zookeeper.server.quorum.QuorumPeerMain' || true"
}

start_kafka_server () {
    run_command "${BASE_DIR}/run_kafka_class.sh kafka.Kafka ${CONF_DIR}/kafka.test.properties > \
        ${LOGS_DIR}/kafka_server.log 2>&1 &"
}

stop_kafka_server() {
    run_command "pkill -f 'kafka.Kafka' || true"
}

start_secor() {
    run_command "${JAVA} -server -ea -Dlog4j.configuration=log4j.dev.properties \
        -Dconfig=secor.test.backup.properties ${ADDITIONAL_OPTS} -cp $CLASSPATH \
        com.pinterest.secor.main.ConsumerMain > ${LOGS_DIR}/secor_backup.log 2>&1 &"
    if [ "${MESSAGE_TYPE}" = "binary" ]; then
       run_command "${JAVA} -server -ea -Dlog4j.configuration=log4j.dev.properties \
           -Dconfig=secor.test.partition.properties ${ADDITIONAL_OPTS} -cp $CLASSPATH \
           com.pinterest.secor.main.ConsumerMain > ${LOGS_DIR}/secor_partition.log 2>&1 &"
    fi
}

stop_secor() {
    run_command "pkill -f 'com.pinterest.secor.main.ConsumerMain' || true"
}

run_finalizer() {
    run_command "${JAVA} -server -ea -Dlog4j.configuration=log4j.dev.properties \
        -Dconfig=secor.test.partition.properties ${ADDITIONAL_OPTS} -cp $CLASSPATH \
        com.pinterest.secor.main.PartitionFinalizerMain > ${LOGS_DIR}/finalizer.log 2>&1 "

    EXIT_CODE=$?
    if [ ${EXIT_CODE} -ne 0 ]; then
        echo -e "\e[1;41;97mFinalizer FAILED\e[0m"
        echo "See log ${LOGS_DIR}/finalizer.log for more details"
        exit ${EXIT_CODE}
    fi
}

create_topic() {
    run_command "${BASE_DIR}/run_kafka_class.sh kafka.admin.TopicCommand --create --zookeeper \
        localhost:2181 --replication-factor 1 --partitions 2 --topic test > \
        ${LOGS_DIR}/create_topic.log 2>&1"
}

# post messages
# $1 number of messages
# $2 timeshift in seconds
post_messages() {
    run_command "${JAVA} -server -ea -Dlog4j.configuration=log4j.dev.properties \
        -Dconfig=secor.test.backup.properties -cp ${CLASSPATH} \
        com.pinterest.secor.main.TestLogMessageProducerMain -t test -m $1 -p 1 -type ${MESSAGE_TYPE} -timeshift $2 > \
        ${LOGS_DIR}/test_log_message_producer.log 2>&1"
}

# verify the messages
# $1: number of messages
# $2: number of _SUCCESS files
verify() {
    echo "Verifying $1 $2"

    RUNMODE_0="backup"
    if [ "${MESSAGE_TYPE}" = "binary" ]; then
      RUNMODE_1="partition"
    else
       RUNMODE_1="backup"
    fi
    for RUNMODE in ${RUNMODE_0} ${RUNMODE_1}; do
      run_command "${JAVA} -server -ea -Dlog4j.configuration=log4j.dev.properties \
          -Dconfig=secor.test.${RUNMODE}.properties ${ADDITIONAL_OPTS} -cp ${CLASSPATH} \
          com.pinterest.secor.main.LogFileVerifierMain -t test -m $1 -q > \
          ${LOGS_DIR}/log_verifier_${RUNMODE}.log 2>&1"
      VERIFICATION_EXIT_CODE=$?
      if [ ${VERIFICATION_EXIT_CODE} -ne 0 ]; then
        echo -e "\e[1;41;97mVerification FAILED\e[0m"
        echo "See log ${LOGS_DIR}/log_verifier_${RUNMODE}.log for more details"
        tail -n 50 ${LOGS_DIR}/log_verifier_${RUNMODE}.log
        echo "See log ${LOGS_DIR}/secor_${RUNMODE}.log for more details"
        tail -n 50 ${LOGS_DIR}/secor_${RUNMODE}.log
        echo "See log ${LOGS_DIR}/test_log_message_producer.log for more details"
        tail -n 50 ${LOGS_DIR}/test_log_message_producer.log
        exit ${VERIFICATION_EXIT_CODE}
      fi

      # Verify SUCCESS file
      if [ ${cloudService} = "swift" ]; then
        run_command "swift list ${SWIFT_CONTAINER} |  grep _SUCCESS | wc -l > /tmp/secor_tests_output.txt"
      else
        if [ -n "${SECOR_LOCAL_S3}" ]; then
            run_command "s3cmd ls -c ${CONF_DIR}/test.s3cfg -r ${S3_LOGS_DIR} | grep _SUCCESS | wc -l > /tmp/secor_tests_output.txt"
        else
            run_command "s3cmd ls -r ${S3_LOGS_DIR} | grep _SUCCESS | wc -l > /tmp/secor_tests_output.txt"
        fi
      fi
      count=$(</tmp/secor_tests_output.txt)
      count="${count//[[:space:]]/}"
      echo "Success file count: $count"
      if [ "$count" != "$2" ]; then
        echo -e "\e[1;41;97m_SUCCESS files not as expected: $2 \e[0m"
        exit 1
      fi
    done
}

set_offsets_in_zookeeper() {
    for group in secor_backup secor_partition; do
        for partition in 0 1; do
            cat <<EOF | run_command "${BASE_DIR}/run_zookeeper_command.sh localhost:2181 > ${LOGS_DIR}/run_zookeeper_command.log 2>&1"
create /consumers ''
create /consumers/${group} ''
create /consumers/${group}/offsets ''
create /consumers/${group}/offsets/test ''
create /consumers/${group}/offsets/test/${partition} $1
quit
EOF
        done
    done
}

stop_all() {
    stop_secor
    sleep 1
    stop_kafka_server
    sleep 1
    stop_zookeeper
    sleep 1
}

initialize() {
    # Just in case.
    stop_all
    recreate_dirs

    start_zookeeper
    sleep 3
    start_kafka_server
    sleep 3
    create_topic
    sleep 3
}

# Post some messages and verify that they are correctly processed.
post_and_verify_test() {
    echo "********************************************************"
    echo "running post_and_verify_test"
    initialize

    start_secor
    sleep 3
    post_messages ${MESSAGES} 0
    echo "Waiting ${WAIT_TIME} sec for Secor to upload logs to s3"
    sleep ${WAIT_TIME}
    verify ${MESSAGES} 0

    stop_all
    echo -e "\e[1;42;97mpost_and_verify_test succeeded\e[0m"
}

# Post some messages and run the finalizer, count # of messages and success file
# $1: hr or dt, decides whether it's hourly or daily folder finalization
post_and_finalizer_verify_test() {
    echo "********************************************************"
    date=$(date -u +'%Y-%m-%d %H:%M:%S')
    read Y M D h m s <<< ${date//[-: ]/ }
    if [ $m -ge 55 ]; then
        # we have to know the number of hr/dt folders to be created
        echo "It's too close to the hour mark: $m, skip the test"
        return
    fi
    if [ $h -le 1 ]; then
        # This will make the timeshift pass the day boundary
        echo "It's too close to the day mark: $h, skip the test"
        return
    fi

    HOUR_TIMESHIFT=$((3600+3600))
    DAY_TIMESHIFT=$((86400+3600))

    OLD_ADDITIONAL_OPTS=${ADDITIONAL_OPTS}

    if [ $1 = "hr" ]; then
        ADDITIONAL_OPTS="${ADDITIONAL_OPTS} -Dpartitioner.granularity.hour=true -Dsecor.finalizer.lookback.periods=30"
        # should be 2 success files for hr folder, 1 for dt folder
        FILES=3
    else
        # should be 1 success files for dt folder
        FILES=1
    fi
    echo "Expected success file: $FILES"

    echo "running post_and_finalizer_verify_test $1"
    initialize

    start_secor
    sleep 3
    
    # post some messages for yesterday
    post_messages ${MESSAGES} ${DAY_TIMESHIFT}
    # post some messages for last hour
    post_messages ${MESSAGES} ${HOUR_TIMESHIFT}
    # post some current messages
    post_messages ${MESSAGES} 0

    echo "Waiting ${WAIT_TIME} sec for Secor to upload logs to s3"
    sleep ${WAIT_TIME}

    echo "start finalizer"
    run_finalizer

    verify $((${MESSAGES}*3)) ${FILES}

    stop_all
    ADDITIONAL_OPTS=${OLD_ADDITIONAL_OPTS}

    echo -e "\e[1;42;97mpost_and_finalizer_verify_test succeeded\e[0m"
}

# Adjust offsets so that Secor consumes only half of the messages.
start_from_non_zero_offset_test() {
    echo "********************************************************"
    echo "running start_from_non_zero_offset_test"
    initialize

    set_offsets_in_zookeeper $((${MESSAGES}/4))
    post_messages ${MESSAGES} 0
    start_secor
    echo "Waiting ${WAIT_TIME} sec for Secor to upload logs to s3"
    sleep ${WAIT_TIME}
    verify $((${MESSAGES}/2)) 0

    stop_all
    echo -e "\e[1;42;97mstart_from_non_zero_offset_test succeeded\e[0m"
}

# Set offset after consumers processed some of the messages.  This scenario simulates a
# re-balancing event and potential topic reassignment triggering the need to trim local log files.
move_offset_back_test() {
    echo "********************************************************"
    echo "running move_offset_back_test"
    initialize

    OLD_ADDITIONAL_OPTS=${ADDITIONAL_OPTS}
    ADDITIONAL_OPTS="${ADDITIONAL_OPTS} -Dsecor.max.file.age.seconds=30"

    start_secor
    sleep 3
    post_messages $((${MESSAGES}/10)) 0
    set_offsets_in_zookeeper 2
    post_messages $((${MESSAGES}*9/10)) 0

    # file.age increased to 30 from 10, so multiply wait time by 3.
    echo "Waiting $((${WAIT_TIME}*3)) sec for Secor to upload logs to s3"
    sleep $((${WAIT_TIME}*3))
    # 4 because we skipped 2 messages per topic partition and there are 2 partitions per topic.
    verify $((${MESSAGES}-4)) 0

    stop_all
    ADDITIONAL_OPTS=${OLD_ADDITIONAL_OPTS}

    echo -e "\e[1;42;97mmove_offset_back_test succeeded\e[0m"
}

# Post some messages and verify that they are correctly processed and compressed.
post_and_verify_compressed_test() {
    echo "********************************************************"
    echo "running post_and_verify_compressed_test"
    initialize

    OLD_ADDITIONAL_OPTS=${ADDITIONAL_OPTS}

    # add compression options
    ADDITIONAL_OPTS="${ADDITIONAL_OPTS} -Dsecor.compression.codec=org.apache.hadoop.io.compress.GzipCodec \
        -Djava.library.path=$HADOOP_NATIVE_LIB_PATH"
    start_secor
    sleep 3
    post_messages ${MESSAGES} 0
    echo "Waiting ${WAIT_TIME} sec for Secor to upload logs to s3"
    sleep ${WAIT_TIME}
    verify ${MESSAGES} 0

    stop_all
    ADDITIONAL_OPTS=${OLD_ADDITIONAL_OPTS}

    echo -e "\e[1;42;97mpost_and_verify_compressed_test succeeded\e[0m"
}

check_for_native_libs
if [ ${cloudService} = "s3" ]; then
    stop_s3
    start_s3
fi

for fkey in ${S3_FILESYSTEMS}; do
    FILESYSTEM_TYPE=${fkey}
    for key in ${!READER_WRITERS[@]}; do
        MESSAGE_TYPE=${key}
        ADDITIONAL_OPTS="-Dsecor.s3.filesystem=${FILESYSTEM_TYPE} -Dsecor.file.reader.writer.factory=${READER_WRITERS[${key}]}"
        echo "********************************************************"
        echo "Running tests for Message Type: ${MESSAGE_TYPE} and  ReaderWriter:${READER_WRITERS[${key}]} using filesystem: ${FILESYSTEM_TYPE}"
        post_and_verify_test
        if [ ${MESSAGE_TYPE} = "binary" ]; then
            # Testing finalizer in partition mode
            post_and_finalizer_verify_test hr
            post_and_finalizer_verify_test dt
        fi
        start_from_non_zero_offset_test
        move_offset_back_test
        if [ ${MESSAGE_TYPE} = "json" ]; then
            post_and_verify_compressed_test
        elif [ -z ${SKIP_COMPRESSED_BINARY} ]; then
            post_and_verify_compressed_test
        else
            echo "Skipping compressed tests for ${MESSAGE_TYPE}"
        fi
    done
done

if [ ${cloudService} = "s3" ]; then
    stop_s3
fi
