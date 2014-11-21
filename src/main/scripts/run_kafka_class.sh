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

# Author: Pawel Garbacki (pawel@pinterest.com)

set -e

if [ $# -lt 1 ]; then
    echo "USAGE: $0 classname [opts]"
    exit 1
fi

base_dir=$(dirname $0)/..

SCALA_VERSION=2.8.0

# assume all dependencies have been packaged into one jar with sbt-assembly's task
# "assembly-package-dependency"
# for file in lib/*.jar; do
#   CLASSPATH=$CLASSPATH:$file
# done

# for file in $base_dir/kafka*.jar; do
#   CLASSPATH=$CLASSPATH:$file
# done

CLASSPATH=${CLASSPATH}:${base_dir}/lib/*

# JMX settings
KAFKA_JMX_OPTS="-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false "

# Log4j settings
KAFKA_LOG4J_OPTS="-Dlog4j.configuration=log4j.dev.properties"

# Generic jvm settings you want to add
KAFKA_OPTS=""

# Which java to use
if [ -z "${JAVA_HOME}" ]; then
    # try to use Java7 by default
    JAVA_HOME=/usr/lib/jvm/java-7-oracle
    if [ -e $JAVA_HOME ]; then
        JAVA=${JAVA_HOME}/bin/java
    else
        JAVA="java"
    fi
else
    JAVA="${JAVA_HOME}/bin/java"
fi

# Memory options
KAFKA_HEAP_OPTS="-Xmx256M"

# JVM performance options
KAFKA_JVM_PERFORMANCE_OPTS="-server -XX:+UseCompressedOops -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:+CMSClassUnloadingEnabled -XX:+CMSScavengeBeforeRemark -XX:+DisableExplicitGC -Djava.awt.headless=true"

exec $JAVA $KAFKA_HEAP_OPTS $KAFKA_JVM_PERFORMANCE_OPTS $KAFKA_GC_LOG_OPTS $KAFKA_JMX_OPTS $KAFKA_LOG4J_OPTS -cp $CLASSPATH $KAFKA_OPTS "$@"
