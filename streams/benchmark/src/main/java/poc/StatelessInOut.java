package poc;

import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import poc.metrics.StatsMetrics;
import poc.metrics.StatsMetrics.MetricName;

public class StatelessInOut {

  public static void main(String[] args) throws IOException, InterruptedException {
    final var props = new Properties();
    props.load(Files.newInputStream(Path.of("streams.properties")));
    final var valueSerde = new GenericAvroSerde();
    final var srConfig = props.keySet().stream().map(o-> (String)o)
//        .filter(s -> s.startsWith("schema"))
        .collect(Collectors.toMap(s -> s, props::get));
    valueSerde.configure(srConfig, false);
    final var builder = new StreamsBuilder();
    builder.stream("jeqo-test-v1", Consumed.with(Serdes.String(), valueSerde))
        .to("jeqo-test-output-v1", Produced.with(Serdes.String(), valueSerde));
//    HttpKafkaStreamsServer.newBuilder()
//        .port(8080)
//        .prometheusMetricsEnabled(true)
//        .build(builder.build(), props)
//        .startApplicationAndServer();
    final var ks = new KafkaStreams(builder.build(), props);
    Runtime.getRuntime().addShutdownHook(new Thread(ks::close));
    ks.start();
    var stats = new StatsMetrics(ks,
        List.of(
            new MetricName("process-rate", "stream-thread-metrics"),
            new MetricName("records-lag-avg", "consumer-fetch-manager-metrics")
        ),
        5_000);
    stats.start();
  }
}
