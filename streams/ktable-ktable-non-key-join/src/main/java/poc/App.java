package poc;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

public class App {

  public static void main(String[] args) throws IOException {
    final Properties config = loadConfig();
    System.out.println(config);

    final var topology = sameEntityTopology();

    System.out.println(topology.describe());
    final var kafkaStreams = new KafkaStreams(topology, config);
    Runtime.getRuntime()
        .addShutdownHook(new Thread(() -> kafkaStreams.close(Duration.ofMinutes(1))));
    kafkaStreams.start();
  }

  static Properties loadConfig() throws IOException {
    final var config = new Properties();
    config.load(Files.newBufferedReader(Path.of("client.properties")));
    return config;
  }

  /*
    $KAFKA_HOME/bin/kafka-console-producer.sh --bootstrap-server localhost:29092 --topic parent_v1 --property "parse.key=true" --property "key.separator=:"
    p1:parent1
    p1:newparent1
    $KAFKA_HOME/bin/kafka-console-producer.sh --bootstrap-server localhost:29092 --topic child_v1 --property "parse.key=true" --property "key.separator=:"
    c1:p1
    c2:p1
    c3:p1
    $KAFKA_HOME/bin/kafka-console-consumer.sh --bootstrap-server localhost:29092 --topic fkjoin_v1 --property "print.key=true" --property "key.separator=:" --from-beginning
   */
  static Topology buildTopology() {
    final var builder = new StreamsBuilder();

    var parentTable =
        builder.table("parent_v1", Consumed.with(Serdes.String(), Serdes.String()));
    var childTable =
        builder.table("child_v1", Consumed.with(Serdes.String(), Serdes.String()));

    childTable
        .join(parentTable, s -> s, (v1, v2) -> v1 + v2)
        .toStream()
        .to("fkjoin_v1", Produced.with(Serdes.String(), Serdes.String()));

    return builder.build();
  }

  static Topology sameEntityTopology() {
    final var builder = new StreamsBuilder();
    final var items = builder.table("items", Consumed.with(Serdes.String(), new ItemSerde()));

    items.leftJoin(items,
            Item::parent,
            (item, parent) -> item.addAttrs(parent.attributes()))
        .toStream()
        .to("joined", Produced.with(Serdes.String(), new ItemSerde()));
    return builder.build();
  }

}
