package poc.stateful;

import java.io.IOException;
import java.time.Duration;
import kafka.context.KafkaContexts;
import kafka.streams.rest.armeria.HttpKafkaStreamsServer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.Stores;
import poc.data.Transaction;
import poc.data.TransactionSerde;

public class StatefulRequestSessiontoreReplyEnrich {

  final Serde<String> keySerde = Serdes.String();
  final Serde<Transaction> valueSerde = new TransactionSerde();
  final String storeName = "transactions";
  final Duration retention = Duration.ofHours(1);

  final String requestTopic;
  final String requestBackendTopic;
  final String responseBackendTopic;
  final String responseTopic;

  public StatefulRequestSessiontoreReplyEnrich(
    String requestTopic,
    String requestBackendTopic,
    String responseBackendTopic,
    String responseTopic
  ) {
    this.requestTopic = requestTopic;
    this.requestBackendTopic = requestBackendTopic;
    this.responseBackendTopic = responseBackendTopic;
    this.responseTopic = responseTopic;
  }

  public Topology topology() {
    final var b = new StreamsBuilder();
    b.addStateStore(
      Stores.sessionStoreBuilder(Stores.inMemorySessionStore(storeName, retention), keySerde, valueSerde)
    );

    b
      .stream(requestTopic, Consumed.with(keySerde, valueSerde).withName("consume-transactions"))
      .peek((key, value) -> {})
      .transformValues( // .processValues(
        () ->
          new ValueTransformerWithKey<String, Transaction, Transaction>() {
            ProcessorContext context;
            SessionStore<String, Transaction> store;

            @Override
            public void init(ProcessorContext context) {
              this.context = context;
              store = context.getStateStore(storeName);
            }

            @Override
            public Transaction transform(String readOnlyKey, Transaction value) { // kip-820: process(record) {
              store.put(new Windowed<>(readOnlyKey, new SessionWindow(context.timestamp(), Long.MAX_VALUE)), value); //kip-820: record.timestamp()
              // context().forward(record)
              return value;
            }

            @Override
            public void close() {} // nothing to close
          },
        Named.as("cache-request"),
        storeName
      )
      .to(requestBackendTopic, Produced.with(keySerde, valueSerde).withName("send-to-backend"));

    b
      .stream(responseBackendTopic, Consumed.with(keySerde, Serdes.String()))
      .transformValues(
        () ->
          new ValueTransformerWithKey<String, String, Transaction>() {
            ProcessorContext context;
            SessionStore<String, Transaction> store;

            @Override
            public void init(ProcessorContext context) {
              this.context = context;
              context.schedule(Duration.ofMinutes(1), PunctuationType.WALL_CLOCK_TIME, timestamp -> {});
              store = context.getStateStore(storeName);
            }

            @Override
            public Transaction transform(String readOnlyKey, String value) {
              try (var iter = store.backwardFetch(readOnlyKey)) {
                if (iter.hasNext()) {
                  return iter.next().value; // kip-820: record.withValue(iter.next().value);
                } else {
                  return null;
                }
              }
            }

            @Override
            public void close() {} // nothing to close
          },
        storeName
      )
      .to(responseTopic, Produced.with(keySerde, valueSerde));
    return b.build();
  }

  public static void main(String[] args) throws IOException {
    final var kafka = KafkaContexts.load().get("local");
    final var props = kafka.properties();

    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "ks1");
    props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);

    final var app = new StatefulRequestSessiontoreReplyEnrich(
      "request",
      "request-backend",
      "response-backend",
      "response"
    );

    final var server = HttpKafkaStreamsServer
      .newBuilder()
      .port(8080)
      .prometheusMetricsEnabled(true)
      .addServiceForWindowStore(app.storeName)
      .build(app.topology(), props);
    server.startApplicationAndServer();
    server
      .kafkaStreams()
      .setUncaughtExceptionHandler(
        new StreamsUncaughtExceptionHandler() {
          @Override
          public StreamThreadExceptionResponse handle(Throwable exception) {
            return null;
          }
        }
      );
    //    var ks = new KafkaStreams(app.topology(), props);
    //    Runtime.getRuntime().addShutdownHook(new Thread(ks::close));
    //    ks.start();
    //    ks.streamsMetadataForStore(app.storeName)
    //        .stream().map(streamsMetadata -> streamsMetadata.hostInfo().)
  }

  static class DeadLetterTopicStreamsUncaughtExceptionHandler implements StreamsUncaughtExceptionHandler {

    final Producer<String, String> producer;

    DeadLetterTopicStreamsUncaughtExceptionHandler(Producer<String, String> producer) {
      this.producer = producer;
    }

    @Override
    public StreamThreadExceptionResponse handle(Throwable exception) {
      return StreamThreadExceptionResponse.REPLACE_THREAD;
    }
  }
}
