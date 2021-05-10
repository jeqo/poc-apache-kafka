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

/**
 *
 */
public class StreamsTestingApp {

  public static void main(String[] args) throws IOException {
    System.out.println("Hello World!");
    final Properties config = loadConfig();
    System.out.println(config);

    final var topology = buildSimpleFromTo();

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

    builder.stream("input", Consumed.with(Serdes.String(), Serdes.String()))
        .to("output", Produced.with(Serdes.String(), Serdes.String()));

    return builder.build();
  }
}
