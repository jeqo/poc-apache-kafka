# DB - Kafka Streams PoCs

## Proofs of Concept

- [Initial examples](src/main/java/poc/App.java)
- [Cache implementations](src/main/java/poc/indigo/StreamingCache.java): HTTP server exposing cache state created by Kafka Streams application.
- [Http-Kafka bridge](src/main/java/poc/HttpBridge.java): HTTP server to produce incoming events into `input` Kafka topic.

### Cache implementations

#### How to run it

1. Define client configurations [here](src/main/resources/streams.properties). 
2. Define which implementation to use `CacheTopology#get`
3. Create topics before running application
4. Produce input data e.g. `kafka-console-producer --bootstrap-server localhost:9092 --topic input --property "parse.key=true" --property "key.separator=:"`
5. Test endpoint e.g. `curl localhost:8080/window-cache/a` or `curl localhost:8080/cache/a`

### Tracing

#### How to run it

0. Download Open Telemetry Java agent: `make get-otel-agent`
1. Run Zipkin locally <https://github.com/openzipkin/zipkin#quick-start>
2. Add JVM arguments to the Main classes to be executed: `-javaagent:../otel/opentelemetry-javaagent-all.jar
   -Dotel.resource.attributes=service.name=http-bridge
   -Dotel.traces.exporter=zipkin`
3. Same for Kafka Streams application: `-javaagent:../otel/opentelemetry-javaagent-all.jar  -Dotel.resource.attributes=service.name=indigo-cache -Dotel.traces.exporter=zipkin  -Dotel.instrumentation.kafka.experimental-span-attributes=true`
4. Check traces reported to Zipkin