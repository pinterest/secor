#!/bin/bash
cd /opt/secor
if [ -z "$SECOR_CONFIG_FILE" ]; then
    echo "Please specify the SECOR_CONFIG_FILE environment variable" 1>&2
    exit 1
fi
java -ea -Dsecor_group=secor "-Dconfig=$SECOR_CONFIG_FILE" $JAVA_OPTS -cp target/secor-0.1-SNAPSHOT.jar:target/lib/* com.pinterest.secor.main.ConsumerMain
