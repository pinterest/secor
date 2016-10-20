CONFIG=src/main/config
TEST_HOME=/tmp/secor_test
TEST_CONFIG=src/test/config
JAR_FILE=target/secor-*-SNAPSHOT-bin.tar.gz
MVN_OPTS=-DskipTests=true -Dmaven.javadoc.skip=true
CONTAINERS=$(shell ls containers)

build:
	@mvn package $(MVN_OPTS)

unit:
	@mvn test

integration: build
	@rm -rf $(TEST_HOME)
	@mkdir -p $(TEST_HOME)
	@tar -xzf $(JAR_FILE) -C $(TEST_HOME)
	@cp $(TEST_CONFIG)/* $(TEST_HOME)
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
