#!/usr/bin/env bash

[ -z "${SECOR_DEPLOY_DIR}" ] && declare SECOR_DEPLOY_DIR="secor_deploy"

mvn package -DskipTests

if [ -d ${SECOR_DEPLOY_DIR} ]; then
    echo "Removing old secor deployment: ${SECOR_DEPLOY_DIR}"
    rm -rf ${SECOR_DEPLOY_DIR}
fi

echo "Creating secor deployment: ${SECOR_DEPLOY_DIR}"
mkdir ${SECOR_DEPLOY_DIR}

# TODO: Make the version # a regex
tar -zxvf target/secor-0.23-SNAPSHOT-bin.tar.gz -C ${SECOR_DEPLOY_DIR}

# copy over test s3 client configuration profile over to deployment
cp src/test/config/test.s3cfg ${SECOR_DEPLOY_DIR}

sed -i '.bak' '/^host_bucket/ s/%(bucket)s/test-bucket/' ${SECOR_DEPLOY_DIR}/test.s3cfg

# set ENV variable to use local secor
export SECOR_LOCAL_S3=true

