package poc;

import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Record;

public class App {

  public static void main(String[] args) {
    var props = loadProperties();
    System.out.println("Config: \n" + props);
    var topology = topology();
    System.out.println("Topology: \n" + topology.describe().toString());
    var streams = new KafkaStreams(topology, props);
    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    streams.start();
  }

  private static Topology topology() {
    final var builder = new StreamsBuilder();
    final var input = builder.stream("input", Consumed.with(Serdes.String(), Serdes.String()));
    input.processValues(() -> new ContextualProcessor<String, String, String, String>() {
          @Override
          public void process(Record<String, String> record) {
            context().forward(record.withValue("Hello " + record.value()));
          }
        }, Named.as("test"))
        .to("output", Produced.with(Serdes.String(), Serdes.String()));

    input.toTable(Materialized.with(Serdes.String(), Serdes.String()))
        .processValues(() -> new ContextualProcessor<String, String, String, String>() {
          @Override
          public void process(Record<String, String> record) {
            context().forward(record.withValue("Table " + record.value()));
          }
        })
        .toStream()
        .to("output-table", Produced.with(Serdes.String(), Serdes.String()));
    return builder.build();
  }

  static Properties loadProperties() {
    final var props = new Properties();
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "new-trasnform");
    return props;
  }
}
