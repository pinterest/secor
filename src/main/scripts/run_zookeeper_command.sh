#!/bin/sh

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

if [ $# -lt 3 ]; then
    echo "USAGE: $0 zookeeper_host:port cmd args"
    exit 1
fi

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

${JAVA} -ea -cp "secor-0.1-SNAPSHOT.jar:lib/*" org.apache.zookeeper.ZooKeeperMain -server $@
