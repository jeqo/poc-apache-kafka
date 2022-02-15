package poc;

import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.stream.Collectors;
import kafka.streams.rest.armeria.HttpKafkaStreamsServer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

public class StatelessInOut {

  public static void main(String[] args) throws IOException {
    final var props = new Properties();
    props.load(Files.newInputStream(Path.of("streams.properties")));
    final var valueSerde = new GenericAvroSerde();
    final var srConfig = props.keySet().stream().map(o-> (String)o)
//        .filter(s -> s.startsWith("schema"))
        .collect(Collectors.toMap(s -> s, props::get));
    valueSerde.configure(srConfig, false);
    final var builder = new StreamsBuilder();
    builder.stream("jeqo-test-v1", Consumed.with(Serdes.String(), valueSerde))
        .to("jeqo-test-output-v1", Produced.with(Serdes.String(), valueSerde));
    HttpKafkaStreamsServer.newBuilder()
        .build(builder.build(), props)
        .startApplicationAndServer();
  }
}
