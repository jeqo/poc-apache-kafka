all:

clients_install:
	cd clients && \
	./mvnw clean install

JAVA_HOME := $(GRAALVM_HOME)
export

cli-binary-%: clients_install
	cd cli/$(*) && \
	./mvnw clean package -Pnative