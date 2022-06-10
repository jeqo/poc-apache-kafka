all: stateless_inout

stateless_inout:
	mvn clean package jib:dockerBuild -Pstateless-in-out

up_monitoring:
	cd ../../monitoring/prometheus-grafana && \
		docker-compose -p benchmark up -d

restart_prometheus:
	cd ../../monitoring/prometheus-grafana && \
		docker-compose -p benchmark restart prometheus

destroy_monitoring:
	cd ../../monitoring/prometheus-grafana && \
		docker-compose -p benchmark down --remove-orphans

up: up_monitoring
	docker-compose up -d

destroy: destroy_monitoring
	docker-compose down --remove-orphans