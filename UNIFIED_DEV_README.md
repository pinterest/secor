# Unified Pinterest Secor

## Setup Guide

##### Get Secor code
```sh
git clone [git-repo-url] secor
cd secor
```

##### Copy over the Fake S3 overrides
```sh
cp src/test/config/jets3t.properties src/main/config
```

##### Make and Deploy a local development secor
```sh
./unified_local_test_deploy.sh

# starts fake s3
./secor_deploy/scripts/start_local.sh

# stops fake s3
./secor_deploy/scripts/start_local.sh
```

#### Running S3 in Intellij

VM Options
 - Create a Run configuration with the following VM Configurations
 - Change the `-Dconfig` to the configuration you're looking to test
```
-server
-ea
-Dlog4j.configuration=log4j.dev.properties
-Dconfig=unified/secor.unified.job_instance_id.properties
```
