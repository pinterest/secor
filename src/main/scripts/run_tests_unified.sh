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

########################################################################################################################
#  MODIFIED COPY OF run_tests.sh
########################################################################################################################

BASE_DIR=$(dirname $0)
source ${BASE_DIR}/test_functions.sh

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

start_secor1() {
    CONFIG=${1:-secor.test.backup.properties}
    echo
    run_command "${JAVA} -server -ea -Dlog4j.configuration=log4j.dev.properties \
        -Dconfig=secor.test.backup.properties ${ADDITIONAL_OPTS} -cp $CLASSPATH \
        com.pinterest.secor.main.ConsumerMain > ${LOGS_DIR}/secor_backup.log 2>&1 &"

#    if [ "${MESSAGE_TYPE}" = "binary" ]; then
#       run_command "${JAVA} -server -ea -Dlog4j.configuration=log4j.dev.properties \
#           -Dconfig=secor.test.partition.properties ${ADDITIONAL_OPTS} -cp $CLASSPATH \
#           com.pinterest.secor.main.ConsumerMain > ${LOGS_DIR}/secor_partition.log 2>&1 &"
#    fi
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

        start_secor1
    done
done

if [ ${cloudService} = "s3" ]; then
    stop_s3
fi