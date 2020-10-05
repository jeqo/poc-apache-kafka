all: build docker-build docker-publish

build:
	mvn clean package

docker-build: docker-build-producer docker-build-consumer

docker-build-producer:
	mvn jib:dockerBuild -Pproducer

docker-build-consumer:
	mvn jib:dockerBuild -Pconsumer

docker-publish: docker-publish-producer docker-publish-consumer

docker-publish-producer:
	mvn jib:build -Pproducer

docker-publish-consumer:
	mvn jib:build -Pconsumer
