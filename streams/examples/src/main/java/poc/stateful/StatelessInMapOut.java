package poc.stateful;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;

public class StatelessInMapOut {
  Topology build() {
    final var builder = new StreamsBuilder();
    return builder.build();
  }
}
