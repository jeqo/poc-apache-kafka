local-sr-start:
	${CONFLUENT_HOME}/bin/confluent local services schema-registry start

SR_BASE_URL := http://localhost:8081

sr-subjects:
	curl -sSL -X GET ${SR_BASE_URL}/subjects | jq .

sr-subject-register:
	mvn io.confluent:kafka-schema-registry-maven-plugin:register

SR_SUBJECT := Payment_v1-value
SR_COMPATIBILITY := FORWARD

sr-compatibility:
	curl -sSL -X PUT \
		-H "Content-Type: application/vnd.schemaregistry.v1+json" \
		--data '{"compatibility":"${SR_COMPATIBILITY}"}' \
		${SR_BASE_URL}/config/${SR_SUBJECT} \
		| jq .

sr-subject-removed:
	curl -sSL -X DELETE \
		${SR_BASE_URL}/subjects/${SR_SUBJECT} \
		| jq .