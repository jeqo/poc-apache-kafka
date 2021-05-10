build:
	mvn clean package
package: build
	tar -czvf poc-kafka-streams-pageviews.tar.gz \
		target/poc-kafka-streams-pageviews-0.1.0-SNAPSHOT.jar \
		target/libs \
		config.properties \
		README.md