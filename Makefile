all:

clients_install:
	cd clients && \
	./mvnw clean install

cli_topic_list_binary: clients_install
	cd cli/topic-list && \
	./mvnw clean package -Pnative