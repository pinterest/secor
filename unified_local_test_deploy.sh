#!/usr/bin/env bash

mvn package -DskipTests

if [ -d secor_deploy ]; then
    rm -rf secor_deploy
fi

mkdir secor_deploy

# TODO: Make the version # a regex
tar -zxvf target/secor-0.23-SNAPSHOT-bin.tar.gz -C secor_deploy

cd secor_deploy

# copy over test s3 client configuration profile over to deployment
cp ../src/test/config/test.s3cfg .

sed -i '.bak' '/^host_bucket/ s/%(bucket)s/test-bucket/' test.s3cfg

# set ENV variable to use local secor
export SECOR_LOCAL_S3=true

