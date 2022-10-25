dashboards:
	generate-dashboard grafana/kafka-metrics.py -o grafana/kafka-metrics-local.json
	SERVER_LABEL=pod generate-dashboard grafana/kafka-metrics.py -o grafana/kafka-metrics-k8s.json

docker_build:
	mvn clean compile jib:dockerBuild

docker_push:
	mvn clean compile jib:build -Djib.to.auth.username=jeqo -Djib.to.auth.password=${GITHUB_TOKEN}

load_tp1:
	hey -n 100000 -m POST -D ./data/random.txt http://34.105.229.223/send/t_1p -c 50

load_tp6:
	hey -n 100000 -m POST -D ./data/random.txt http://34.105.229.223/send/t_6p -c 50

load_tp12:
	hey -n 100000 -m POST -D ./data/random.txt http://34.105.229.223/send/t_12p -c 50

load_tp64:
	hey -n 100000 -m POST -D ./data/random.txt http://34.105.229.223/send/t_64p -c 50

load_sync_tp1:
	hey -n 100000 -m POST -D ./data/random.txt http://34.105.229.223/send_sync/t_1p -c 50

load_sync_tp6:
	hey -n 100000 -m POST -D ./data/random.txt http://34.105.229.223/send_sync/t_6p -c 50

load_sync_tp12:
	hey -n 100000 -m POST -D ./data/random.txt http://34.105.229.223/send_sync/t_12p -c 50

load_sync_tp64:
	hey -n 100000 -m POST -D ./data/random.txt http://34.105.229.223/send_sync/t_64p -c 50

k8s_delete:
	kustomize build kubernetes/ | kubectl delete -f -

k8s_apply:
	kustomize build kubernetes/ | kubectl apply -f -

k8s_deploy: k8s_delete k8s_apply