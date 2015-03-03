CONFIG=src/main/config
TEST_HOME=/tmp/secor_test
TEST_CONFIG=src/test/config
JAR_FILE=target/secor-*-SNAPSHOT-bin.tar.gz
MVN_OPTS=-DskipTests=true -Dmaven.javadoc.skip=true

build:
	@mvn package $(MVN_OPTS)

unit:
	@mvn test

integration: build
	@rm -rf $(TEST_HOME)
	@mkdir -p $(TEST_HOME)
	@tar -xzf $(JAR_FILE) -C $(TEST_HOME)
	@cp $(TEST_CONFIG)/* $(TEST_HOME)
	@[ ! -e $(CONFIG)/jets3t.properties ] && jar uf $(TEST_HOME)/secor-*.jar -C $(TEST_CONFIG) jets3t.properties
	cd $(TEST_HOME) && ./scripts/run_tests.sh

test: build unit integration

