# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
CONFIG=src/main/config
TEST_HOME=/tmp/secor_test
TEST_CONFIG=src/test/config
JAR_FILE=target/secor-*-SNAPSHOT-bin.tar.gz
MVN_PROFILE?=kafka-0.10.2.0
MVN_OPTS=-DskipTests=true -Dmaven.javadoc.skip=true -P $(MVN_PROFILE)
CONTAINERS=$(shell ls containers)

build:
	@mvn package $(MVN_OPTS) -P $(MVN_PROFILE)

dependency_tree:
	@mvn dependency:tree -P $(MVN_PROFILE)

unit:
	@mvn test -P $(MVN_PROFILE)

integration: build
	@rm -rf $(TEST_HOME)
	@mkdir -p $(TEST_HOME)
	@tar -xzf $(JAR_FILE) -C $(TEST_HOME)
	@cp $(TEST_CONFIG)/* $(TEST_HOME)
	@cp docker-compose.yaml $(TEST_HOME)
	@[ ! -e $(CONFIG)/core-site.xml ] && jar uf $(TEST_HOME)/secor-*.jar -C $(TEST_CONFIG) core-site.xml
	@[ ! -e $(CONFIG)/jets3t.properties ] && jar uf $(TEST_HOME)/secor-*.jar -C $(TEST_CONFIG) jets3t.properties
	cd $(TEST_HOME) && ./scripts/run_tests.sh

test: build unit integration

container_%:
	docker build -t secor_$* containers/$*

test_%: container_%
	@mkdir -p .m2
	docker run -v $(CURDIR)/.m2:/root/.m2:rw -v $(CURDIR):/work:rw secor_$* sh -c "echo 127.0.0.1 test-bucket.localhost >> /etc/hosts && make clean test"

docker_test: $(foreach container, $(CONTAINERS), test_$(container))

clean:
	rm -rf target/
