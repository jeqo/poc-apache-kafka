dashboards:
	generate-dashboard grafana/kafka-metrics.py -o grafana/kafka-metrics-local.json
	SERVER_LABEL=pod generate-dashboard grafana/kafka-metrics.py -o grafana/kafka-metrics-k8s.json

docker_build:
	mvn clean compile jib:dockerBuild

docker_push:
	mvn clean compile jib:build -Djib.to.auth.username=jeqo -Djib.to.auth.password=${GITHUB_TOKEN}

load:
	hey -n 50000 -m POST -D pom.xml http://34.105.229.223/send/t_1p -c 64