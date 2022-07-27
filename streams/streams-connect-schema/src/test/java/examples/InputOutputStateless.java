package examples;

import java.util.Properties;
import kafka.streams.connect.data.SchemaAndValueSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

public class InputOutputStateless {

  public static void main(String[] args) {
    final var converter = new JsonConverter();

    final var inputSerde = new SchemaAndValueSerde(converter);
    final var outputSerde = new SchemaAndValueSerde(converter);

    final var b = new StreamsBuilder();
    b
      .stream("input", Consumed.with(Serdes.String(), inputSerde))
      // .filter((s, schemaAndValue) -> schemaAndValue.value())
      .to("output", Produced.with(Serdes.String(), outputSerde));

    final var topology = b.build();
    final var config = new Properties();

    try (var ks = new KafkaStreams(topology, config)) {
      ks.start();
    }
  }
}
