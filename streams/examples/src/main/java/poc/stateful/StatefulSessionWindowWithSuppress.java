package poc.stateful;

import java.io.IOException;
import java.time.Duration;
import kafka.context.KafkaContexts;
import kafka.producer.ProgressControlInterceptor;
import kafka.streams.rest.armeria.HttpKafkaStreamsServer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.Suppressed;
import org.apache.kafka.streams.kstream.Suppressed.BufferConfig;
import poc.data.Transaction;
import poc.data.TransactionSerde;

public class StatefulSessionWindowWithSuppress {

  final Serde<String> keySerde = Serdes.String();
  final Serde<Transaction> valueSerde = new TransactionSerde();
  final Serde<Long> outputValueSerde = Serdes.Long();

  final String inputTopic;
  final String outputTopic;

  public StatefulSessionWindowWithSuppress(String inputTopic, String outputTopic) {
    this.inputTopic = inputTopic;
    this.outputTopic = outputTopic;
  }

  public Topology topology() {
    final var b = new StreamsBuilder();

    b.stream(inputTopic, Consumed.with(keySerde, valueSerde))
        .selectKey((s, transaction) -> transaction.userId())
        .repartition(Repartitioned.with(keySerde, valueSerde))
        .groupByKey()
        .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofSeconds(30)))
        .count()
        .suppress(Suppressed.untilWindowCloses(BufferConfig.unbounded()))
        .toStream()
        .selectKey(
            (w, aLong) -> "%s@<%s,%s>".formatted(w.key(), w.window().start(), w.window().endTime()))
        .to(outputTopic, Produced.with(keySerde, outputValueSerde));

    return b.build();
  }

  public static void main(String[] args) throws IOException {
    final var kafka = KafkaContexts.load().get("local");
    final var props = kafka.properties();

    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "ks1");
    props.put(
        StreamsConfig.producerPrefix(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG),
        ProgressControlInterceptor.class.getName());
    props.put("progress.control.start.ms", 60000);
    props.put("progress.control.topics.include", "ks1-KSTREAM-REPARTITION-0000000002-repartition");

    final var app = new StatefulSessionWindowWithSuppress("input", "output");

    final var server =
        HttpKafkaStreamsServer.newBuilder()
            .port(8080)
            .prometheusMetricsEnabled(true)
            .build(app.topology(), props);
    server.startApplicationAndServer();
  }
}
