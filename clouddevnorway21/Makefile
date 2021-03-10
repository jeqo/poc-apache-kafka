
aks-helm-get-charts:
	helm pull confluentinc/cp-helm-charts --untar

aks-connect-deploy:
	helm upgrade connect-aks -f aks-connect.yml ./cp-helm-charts/charts/cp-kafka-connect --install

k8s-port-forward:
	kubectl port-forward service/connect-aks-cp-kafka-connect 8083

connector-deploy:
	curl -sSL http://localhost:8083/connectors -XPOST --data @connectors/hello-file-sink.json -H 'Content-Type: application/json'

connector-status:
	curl -sSL http://localhost:8083/connectors/hello-file-sink

producer-perf-1:
	${CONFLUENT_HOME}/bin/kafka-producer-perf-test  \
		--producer.config ccloud.properties --topic perftest1 \
		--num-records 300000 --throughput 1000 --record-size 100 > perfproducer1.txt

producer-perf-2:
	${CONFLUENT_HOME}/bin/kafka-producer-perf-test  \
		--producer.config ccloud.properties --topic perftest1 \
		--num-records 300000 --throughput 10000 --record-size 100 > perfproducer2.txt

producer-perf-3:
	${CONFLUENT_HOME}/bin/kafka-producer-perf-test  \
		--producer.config ccloud.properties --topic perftest1 \
		--producer-prop batch.size=10000 --producer-prop linger.ms=100 \
		--num-records 300000 --throughput 10000 --record-size 100 > perfproducer3.txt
