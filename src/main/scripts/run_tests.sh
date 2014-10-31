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
#     tar -zxvf ~/git/optimus/secor/target/secor-0.1-SNAPSHOT-bin.tar.gz
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
MESSAGES=1000
MESSAGE_TYPE=binary
# For the compression tests to work, set this to the path of the Hadoop native libs.
HADOOP_NATIVE_LIB_PATH=lib
# by default additional opts is empty
ADDITIONAL_OPTS=

# various reader writer options to be used for testing
declare -A READER_WRITERS
READER_WRITERS[json]=com.pinterest.secor.io.impl.DelimitedTextFileReaderWriterFactory
READER_WRITERS[binary]=com.pinterest.secor.io.impl.SequenceFileReaderWriterFactory

# The minimum wait time is one minute plus delta.  Secor is configured to upload files older than
# one minute and we need to make sure that everything ends up on s3 before starting verification.
WAIT_TIME=${SECOR_WAIT_TIME:-120}
BASE_DIR=$(dirname $0)
CONF_DIR=${BASE_DIR}/..

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
    if [ -n ${SECOR_LOCAL_S3} ]; then
        run_command "s3cmd -c ${CONF_DIR}/test.s3cfg ls ${S3_LOGS_DIR} | awk '{ print \$4 }' | xargs -L 1 s3cmd -c ${CONF_DIR}/test.s3cfg del"
    else
        run_command "s3cmd del --recursive ${S3_LOGS_DIR}"
    fi
    # create logs directory
    if [ ! -d ${LOGS_DIR} ]; then
        run_command "mkdir -p ${LOGS_DIR}"
    fi
}

start_s3() {
    if [ -n ${SECOR_LOCAL_S3} ]; then
        if command -v fakes3 > /dev/null 2>&1; then
            run_command "fakes3 --root=/tmp/fakes3 --port=5000 --hostname=localhost > /tmp/fakes3.log 2>&1 &"
            sleep 2
            run_command "s3cmd -c ${CONF_DIR}/test.s3cfg mb s3://${BUCKET}"
        else
            echo "Couldn't find FakeS3 binary, please install it using `gem install fakes3`"
        fi
    fi
}

stop_s3() {
    if [ -n ${SECOR_LOCAL_S3} ]; then
        run_command "pkill -9 'fakes3' > /dev/null 2>&1 || true"
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

create_topic() {
    run_command "${BASE_DIR}/run_kafka_class.sh kafka.admin.TopicCommand --create --zookeeper \
        localhost:2181 --replication-factor 1 --partitions 2 --topic test > \
        ${LOGS_DIR}/create_topic.log 2>&1"
}

post_messages() {
    run_command "${JAVA} -server -ea -Dlog4j.configuration=log4j.dev.properties \
        -Dconfig=secor.test.backup.properties -cp ${CLASSPATH} \
        com.pinterest.secor.main.TestLogMessageProducerMain -t test -m $1 -p 1 -type ${MESSAGE_TYPE} > \
        ${LOGS_DIR}/test_log_message_producer.log 2>&1"
}

verify() {
    RUNMODE_0="backup"
    if [ "${MESSAGE_TYPE}" = "binary" ]; then
      RUNMODE_1="partition"
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
        stop_all
        stop_s3
        exit ${VERIFICATION_EXIT_CODE}
      fi
    done
}

set_offsets_in_zookeeper() {
    for group in secor_backup secor_partition; do
        for partition in 0 1; do
            run_command "${BASE_DIR}/run_zookeeper_command.sh localhost:2181 create \
                /consumers \'\' > ${LOGS_DIR}/run_zookeeper_command.log 2>&1"
            run_command "${BASE_DIR}/run_zookeeper_command.sh localhost:2181 create \
                /consumers/${group} \'\' > ${LOGS_DIR}/run_zookeeper_command.log 2>&1"
            run_command "${BASE_DIR}/run_zookeeper_command.sh localhost:2181 create \
                /consumers/${group}/offsets \'\' > ${LOGS_DIR}/run_zookeeper_command.log 2>&1"
            run_command "${BASE_DIR}/run_zookeeper_command.sh localhost:2181 create \
                /consumers/${group}/offsets/test \'\' > ${LOGS_DIR}/run_zookeeper_command.log 2>&1"
            run_command "${BASE_DIR}/run_zookeeper_command.sh localhost:2181 create \
                /consumers/${group}/offsets/test/${partition} $1 > \
                ${LOGS_DIR}/run_zookeeper_command.log 2>&1"
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
    echo "running post_and_verify_test"
    initialize

    start_secor
    sleep 3
    post_messages ${MESSAGES}
    echo "Waiting ${WAIT_TIME} sec for Secor to upload logs to s3"
    sleep ${WAIT_TIME}
    verify ${MESSAGES}

    stop_all
    echo -e "\e[1;42;97mpost_and_verify_test succeeded\e[0m"
}

# Adjust offsets so that Secor consumes only half of the messages.
start_from_non_zero_offset_test() {
    echo "running start_from_non_zero_offset_test"
    initialize

    set_offsets_in_zookeeper $((${MESSAGES}/4))
    post_messages ${MESSAGES}
    start_secor
    echo "Waiting ${WAIT_TIME} sec for Secor to upload logs to s3"
    sleep ${WAIT_TIME}
    verify $((${MESSAGES}/2))

    stop_all
    echo -e "\e[1;42;97mstart_from_non_zero_offset_test succeeded\e[0m"
}

# Set offset after consumers processed some of the messages.  This scenario simulates a
# re-balancing event and potential topic reassignment triggering the need to trim local log files.
move_offset_back_test() {
    echo "running move_offset_back_test"
    initialize

    start_secor
    sleep 3
    post_messages $((${MESSAGES}/10))
    set_offsets_in_zookeeper 2
    post_messages $((${MESSAGES}*9/10))

    echo "Waiting ${WAIT_TIME} sec for Secor to upload logs to s3"
    sleep ${WAIT_TIME}
    # 4 because we skipped 2 messages per topic partition and there are 2 partitions per topic.
    verify $((${MESSAGES}-4))

    stop_all
    echo -e "\e[1;42;97mmove_offset_back_test succeeded\e[0m"
}

# Post some messages and verify that they are correctly processed and compressed.
post_and_verify_compressed_test() {
    echo "running post_and_verify_compressed_test"
    initialize

    # add compression options
    ADDITIONAL_OPTS="${ADDITIONAL_OPTS} -Dsecor.compression.codec=org.apache.hadoop.io.compress.GzipCodec \
        -Djava.library.path=$HADOOP_NATIVE_LIB_PATH"
    start_secor
    sleep 3
    post_messages ${MESSAGES}
    echo "Waiting ${WAIT_TIME} sec for Secor to upload logs to s3"
    sleep ${WAIT_TIME}
    verify ${MESSAGES}

    stop_all
    echo -e "\e[1;42;97mpost_and_verify_compressed_test succeeded\e[0m"
}

check_for_native_libs
start_s3

for key in ${!READER_WRITERS[@]}; do
   MESSAGE_TYPE=${key}
   ADDITIONAL_OPTS=-Dsecor.file.reader.writer.factory=${READER_WRITERS[${key}]}
   echo "Running tests for Message Type: ${MESSAGE_TYPE} and ReaderWriter: ${READER_WRITERS[${key}]}"
   post_and_verify_test
   start_from_non_zero_offset_test
   move_offset_back_test
   if [ ${key} = "json" ]; then
       post_and_verify_compressed_test
   elif [ -z ${SKIP_COMPRESSED_BINARY} ]; then
       post_and_verify_compressed_test
   else
       echo "Skipping compressed tests for ${key}"
   fi
done

stop_s3
