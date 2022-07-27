package poc.stateless;

import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.stream.Collectors;
import kafka.streams.rest.armeria.HttpKafkaStreamsServer;
import org.apache.avro.util.Utf8;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

public class StatelessInOut {

  public static void main(String[] args) throws IOException, InterruptedException {
    final var props = new Properties();
    props.load(Files.newInputStream(Path.of("streams.properties")));
    final var valueSerde = new GenericAvroSerde();
    final var srConfig = props
      .keySet()
      .stream()
      .map(o -> (String) o)
      //        .filter(s -> s.startsWith("schema"))
      .collect(Collectors.toMap(s -> s, props::get));
    valueSerde.configure(srConfig, false);
    final var builder = new StreamsBuilder();
    builder
      .stream("jeqo-test-v1", Consumed.with(Serdes.String(), valueSerde))
      .filter((key, value) -> ((Utf8) value.get("ip")).toString().startsWith("1"))
      .to("jeqo-test-output-v1", Produced.with(Serdes.String(), valueSerde));
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
