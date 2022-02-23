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

    final var topology = buildTopology();

    System.out.println(topology.describe());
//    final var kafkaStreams = new KafkaStreams(topology, config);
//    Runtime.getRuntime()
//        .addShutdownHook(new Thread(() -> kafkaStreams.close(Duration.ofMinutes(1))));
//    kafkaStreams.start();
  }

  static Properties loadConfig() throws IOException {
    final var config = new Properties();
    config.load(Files.newBufferedReader(Path.of("client.properties")));
    return config;
  }

  static Topology buildTopology() {
    final var builder = new StreamsBuilder();
    final var items = builder.table("items", Consumed.with(Serdes.String(), new ItemSerde()));

    items.leftJoin(items,
            Item::parent,
            (item, parent) -> item.addAttrs(parent.attributes()))
        .toStream()
        .to("joined", Produced.with(Serdes.String(), new ItemSerde()));
    return builder.build();
  }

  static Topology inout() {

    final var builder = new StreamsBuilder();
    builder.stream("")
        // meaningful part
        .to("");
    return builder.build();
  }

}
