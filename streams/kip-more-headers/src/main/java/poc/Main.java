package poc;

import static org.apache.kafka.streams.StreamsConfig.APPLICATION_ID_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.STATE_DIR_CONFIG;

import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.RecordSerde;
import org.apache.kafka.streams.kstream.Repartitioned;

public class Main {

  public static void main(String[] args) {
    var builder = new StreamsBuilder();
    var input = builder.stream("input", Consumed.with(Serdes.String(), Serdes.String())).mapValueToRecord().split();
    input
      .branch(
        (key, value) -> value.topic().equals("input"),
        Branched.withConsumer(b1 -> {
          b1
            .filter((key, value) -> value.headers().hasWithName("a"))
            .filter((key, value) -> ("a").equals(value.headers().lastWithName("a").valueAsUtf8()))
            .setRecordHeaders((k, v) -> v.headers().addUtf8("a", "b").retainLatest())
            .mapValues((k, v) -> v.value())
            .to("output", Produced.with(Serdes.String(), Serdes.String()));

          b1
            .groupByKey()
            .reduce(
              (value1, value2) -> {
                value1.headers().forEach(header -> value2.headers().add(header));
                return value2;
              },
              Materialized.with(Serdes.String(), new RecordSerde<>(Serdes.String(), Serdes.String()))
            );

          b1
            .groupByKey()
            .count()
            .toStream()
            .mapValueToRecord()
            .repartition(Repartitioned.with(Serdes.String(), new RecordSerde<>(Serdes.String(), Serdes.Long())))
            .foreach((key, value) -> System.out.println(key + " => " + value));
        })
      )
      .noDefaultBranch();

    var configs = new Properties();
    configs.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    configs.put(APPLICATION_ID_CONFIG, "kip-headers_1");
    configs.put(STATE_DIR_CONFIG, "target/kafka-streams");
    var kafkaStreams = new KafkaStreams(builder.build(), configs);
    Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
    kafkaStreams.start();
  }
}
