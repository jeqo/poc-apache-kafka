package poc.stateful;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Repartitioned;

public class StatelessRepartition {

  public static void main(String[] args) {
    var builder = new StreamsBuilder();
    builder.stream("input", Consumed.with(Serdes.String(), Serdes.String()).withName("poll-input"))
        .map((key, value) -> {
          if (key == null) {
            // Change the key
            return KeyValue.pair(value, value);
          } else {
            return KeyValue.pair(key, value);
          }
        }, Named.as("map-key-when-null"))
        .repartition(Repartitioned.with(Serdes.String(), Serdes.String()).withName("input"))
        .mapValues(value -> "Processed: " + value)
        .to("output", Produced.with(Serdes.String(), Serdes.String()));

    var topology = builder.build();

    System.out.println(topology.describe());

    new StatelessRepartition().other();
  }

  void other() {
    var builder = new StreamsBuilder();
    final var input = builder.stream("input",
        Consumed.with(Serdes.String(), Serdes.String()).withName("poll-input"));

    input.filter((key, value) -> key == null)
        .selectKey((key, value) -> value) //set key from value.
        .to("input", Produced.with(Serdes.String(), Serdes.String()));

    input
        .filter((key, value) -> key != null)
        .mapValues(value -> "Processed: " + value)
        .to("output", Produced.with(Serdes.String(), Serdes.String()));

    var topology = builder.build();

    System.out.println(topology.describe());
  }
}
