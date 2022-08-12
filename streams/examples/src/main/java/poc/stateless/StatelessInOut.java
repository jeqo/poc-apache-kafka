package poc.stateless;

import java.io.IOException;
import java.util.Map;
import kafka.context.KafkaContexts;
import kafka.serde.connect.SchemaAndValueSerde;
import kafka.serde.connect.util.Requirements;
import kafka.streams.rest.armeria.HttpKafkaStreamsServer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

public class StatelessInOut {

  public static void main(String[] args) throws IOException, InterruptedException {
    var props = KafkaContexts.load().getDefault().properties();
    props.put("application.id", "test_v1");
    //final var srConfig = props
    //  .keySet()
    //  .stream()
    //  .map(o -> (String) o)
    //  //        .filter(s -> s.startsWith("schema"))
    //  .collect(Collectors.toMap(s -> s, props::get));
    //    final var valueSerde = new GenericAvroSerde();
    //    valueSerde.configure(srConfig, false);
    var converter = new JsonConverter();
    converter.configure(Map.of("schemas.enable", "false", "converter.type", "value"));
    var valueSerde = new SchemaAndValueSerde(converter);
    final var builder = new StreamsBuilder();
    builder
      .stream("test-input", Consumed.with(Serdes.String(), valueSerde))
      .mapValues((s, schemaAndValue) -> Requirements.requireMapOrNull(schemaAndValue.value(), "testing"))
      .filter((s, map) -> !map.isEmpty())
      .filter((s, map) -> map.get("gender").toString().equals("MALE"))
      .mapValues((s, stringObjectMap) -> new SchemaAndValue(null, stringObjectMap))
      .to("test-output", Produced.with(Serdes.String(), valueSerde));
    final var server = HttpKafkaStreamsServer
      .newBuilder()
      .port(8080)
      .prometheusMetricsEnabled(true)
      .build(builder.build(), props);
    server.startApplicationAndServer();
    //    var stats = new MetricsPrinter(() -> server.kafkaStreams().metrics(),
    //        List.of(
    //            new MetricName("stream-thread-metrics", "process-rate"),
    //            new MetricName("stream-thread-metrics", "process-latency-avg"),
    //            new MetricName("stream-thread-metrics", "process-latency-max"),
    //            new MetricName("stream-thread-metrics", "poll-records-avg"),
    //            new MetricName("stream-thread-metrics", "poll-records-max"),
    //            new MetricName("consumer-fetch-manager-metrics", "records-lag-avg")
    //        ),
    //        10_000);
    //    stats.start();
  }
}
