# Unified Pinterest Secor

## Setup Guide

##### Get Secor code
```sh
git clone [git-repo-url] secor
cd secor
```

##### Install FakeS3
To run and test secor locally you need to install [fake-s3](https://github.com/jubos/fake-s3).

`gem install fakes3`

##### Copy over the Fake S3 overrides
<aside class="notice">DO NOT COMMIT THIS FILE</aside>

This configuration is to point secor to the fake s3 host/port.
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
 - Change the `-Dconfig` to the configuration you want to run
 ```
-server
-ea
-Dlog4j.configuration=log4j.dev.properties
-Dconfig=unified/secor.unified.job_instance_id.properties
```
![Image of IntelliJ Java Configuration](https://github.com/Unified/secor_pinterest/blob/feature/UIP-1087_csv_writer/secor_pinterest_idea.png?raw=true)
