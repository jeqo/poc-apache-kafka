package poc;

import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
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
    final var input = builder.stream("words", Consumed.with(Serdes.String(), Serdes.String()));
    input
      .processValues(
        () ->
          new FixedKeyProcessor<String, String, String>() {
            FixedKeyProcessorContext<String, String> context;

            @Override
            public void init(FixedKeyProcessorContext<String, String> context) {
              this.context = context;
            }

            @Override
            public void process(FixedKeyRecord<String, String> record) {
              for (var s : record.value().split(",")) {
                context.forward(record.withValue("Hello " + s));
              }
            }
          },
        Named.as("test")
      )
      .to("output", Produced.with(Serdes.String(), Serdes.String()));
    input
      .process(
        () ->
          new ContextualProcessor<String, String, String, String>() {
            @Override
            public void process(Record<String, String> record) {
              System.out.println(record.value());
              context().forward(record);
            }
          },
        Named.as("test-other")
      )
      .to("other", Produced.with(Serdes.String(), Serdes.String()));
    return builder.build();
  }

  static Properties loadProperties() {
    final var props = new Properties();
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "new-process");
    return props;
  }
}
