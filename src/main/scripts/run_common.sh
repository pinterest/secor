#!/usr/bin/env bash

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

DEFAULT_CLASSPATH="*:lib/*"
CLASSPATH=${CLASSPATH:-$DEFAULT_CLASSPATH}

