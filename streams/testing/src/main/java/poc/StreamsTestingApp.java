package poc;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;

/**
 *
 */
public class StreamsTestingApp {

  public static void main(String[] args) throws IOException {
    final Properties config = loadConfig();
    System.out.println(config);

    //    final var topology = buildSimpleFromTo();
    final var topology = buildSessionWindowFromMultipleTopics();

    System.out.println(topology.describe());
    final var kafkaStreams = new KafkaStreams(topology, config);
    Runtime.getRuntime().addShutdownHook(new Thread(() -> kafkaStreams.close(Duration.ofMinutes(1))));
    kafkaStreams.start();
  }

  static Properties loadConfig() throws IOException {
    final var config = new Properties();
    config.load(Files.newBufferedReader(Path.of("client.properties")));
    return config;
  }

  static Topology buildSimpleFromTo() {
    final var builder = new StreamsBuilder();

    builder
      .stream("input", Consumed.with(Serdes.String(), Serdes.String()))
      .to("output", Produced.with(Serdes.String(), Serdes.String()));

    return builder.build();
  }

  /*
    $KAFKA_HOME/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic input_1 --property "parse.key=true" --property "key.separator=:"
    $KAFKA_HOME/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic input_2 --property "parse.key=true" --property "key.separator=:"
    $KAFKA_HOME/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic input_3 --property "parse.key=true" --property "key.separator=:"
   */
  static Topology buildSessionWindowFromMultipleTopics() {
    final var builder = new StreamsBuilder();

    final TransformerSupplier<String, String, KeyValue<String, String>> transformerSupplier = () ->
      new Transformer<>() {
        ProcessorContext context;

        @Override
        public void init(ProcessorContext context) {
          this.context = context;
        }

        @Override
        public KeyValue<String, String> transform(String key, String value) {
          if (!value.isBlank()) {
            System.out.println("Extracting key based on topic name: " + context.topic());
            final var k = value.split(",")[0];
            return KeyValue.pair(k, value);
          } else {
            return null;
          }
        }

        @Override
        public void close() {}
      };

    builder
      .stream(List.of("input_1", "input_2", "input_3"), Consumed.with(Serdes.String(), Serdes.String()))
      .transform(transformerSupplier)
      .filter((key, value) -> Objects.nonNull(value))
      .repartition(Repartitioned.with(Serdes.String(), Serdes.String()))
      .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
      .windowedBy(SessionWindows.with(Duration.ofMinutes(1)).grace(Duration.ofSeconds(30)))
      .aggregate(
        () -> "",
        (key, v, v1) -> v + "&" + v1,
        (k, a, a1) -> a + "_" + a1,
        Materialized.with(Serdes.String(), Serdes.String())
      )
      .toStream((key, value) -> key.toString())
      .to("output", Produced.with(Serdes.String(), Serdes.String()));

    return builder.build();
  }
}
