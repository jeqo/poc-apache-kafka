package poc;

import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import ksql.pageviews;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

/**
 * Streaming application that reduces pageviews input stream into the sum of viewtime per userId.
 */
public class App {

  public static void main(String[] args) throws IOException {
    var config = Config.load();

    var builder = new StreamsBuilder();

    var inputValueSerde = new SpecificAvroSerde<pageviews>();
    inputValueSerde.configure(config.streamsConfigAsMap(), false);

    builder.stream(config.inputTopic, Consumed.with(Serdes.String(), inputValueSerde))
        .map((key, p) -> KeyValue.pair(p.getUserid(), p.getViewtime()))
        .groupByKey()
        .reduce(Long::sum)
        .toStream()
        .to(config.outputTopic, Produced.with(Serdes.String(), Serdes.Long()));

    var topology = builder.build();

    var kafkaStreams = new KafkaStreams(topology, config.streamsConfig);
    Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    kafkaStreams.start();
  }

  public static class Config {

    final String inputTopic;
    final String outputTopic;
    final Properties streamsConfig;

    public Config(String inputTopic, String outputTopic, Properties streamsConfig) {
      this.inputTopic = inputTopic;
      this.outputTopic = outputTopic;
      this.streamsConfig = streamsConfig;
    }

    Map<String, ?> streamsConfigAsMap() {
      Map<String, Object> map = new LinkedHashMap<>();
      streamsConfig.forEach((o, o2) -> map.put((String) o, o2));
      return map;
    }

    static Config load() throws IOException {
      var inputTopic = "pageviews";
      var inputTopicParam = System.getenv("TOPIC_INPUT");
      if (inputTopicParam != null) {
        inputTopic = inputTopicParam;
      }

      var outputTopic = "pageviews-sum-per-userid";
      var outputTopicParam = System.getenv("TOPIC_OUTPUT");
      if (outputTopicParam != null) {
        outputTopic = outputTopicParam;
      }

      var config = new Properties();
      config.load(Files.newInputStream(Path.of("CONFIG_PATH")));

      return new Config(inputTopic, outputTopic, config);
    }

  }
}
