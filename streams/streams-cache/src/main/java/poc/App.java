package poc;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

public class App {

  /**
   * <code>
   * kafka-topics --bootstrap-server localhost:9092 --create --topic input --partitions 1
   * </code>
   * <p>
   * <code>
   * kafka-topics --bootstrap-server localhost:9092 --create --topic output --partitions 1
   * </code>
   * <p>
   * <code>
   * kafka-console-producer --bootstrap-server localhost:9092 --topic topic-name --property
   * "parse.key=true" --property "key.separator=:"
   * </code>
   *
   */
  public static void main(String[] args) throws IOException {
    final var streamsProps = new Properties();
    try (final var inputStream = new FileInputStream("src/main/resources/streams.properties")) {
      streamsProps.load(inputStream);
    }

    final var b = new StreamsBuilder();
    // as simple as possible, pass-through
    b
      .stream("input", Consumed.with(Serdes.String(), Serdes.String()))
      .to("output", Produced.with(Serdes.String(), Serdes.String()));

    // basic stream operations (stateless)
    //    b.stream("input", Consumed.with(Serdes.String(), Serdes.String()))
    //        .filter((key, value) -> value.startsWith("a") || value.startsWith("b"))
    //        .mapValues((readOnlyKey, value) -> value.toUpperCase())
    //        .peek((key, value) -> System.out.println(key + "=>" + value))
    //        .to("output", Produced.with(Serdes.String(), Serdes.String()));

    // ktable
    //    b.table("input", Consumed.with(Serdes.String(), Serdes.String()))
    //        .filter((key, value) -> value.startsWith("a") || value.startsWith("b"))
    //        .mapValues((readOnlyKey, value) -> value.toUpperCase(),
    //            Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("ktable-store")
    //                .withKeySerde(Serdes.String())
    //                .withValueSerde(Serdes.String()))
    //        .toStream()
    //        .peek((key, value) -> System.out.println(key + "=>" + value))
    //        .to("output");

    final var topology = b.build();

    System.out.println(topology.describe());

    final var ks = new KafkaStreams(topology, streamsProps);
    ks.start();
  }
}
