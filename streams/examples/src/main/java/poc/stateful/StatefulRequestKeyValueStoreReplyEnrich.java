package poc.stateful;

import java.io.IOException;
import java.time.Duration;
import kafka.context.KafkaContexts;
import kafka.streams.rest.armeria.HttpKafkaStreamsServer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import poc.data.Transaction;
import poc.data.TransactionSerde;

public class StatefulRequestKeyValueStoreReplyEnrich {

  final Serde<String> keySerde = Serdes.String();
  final Serde<Transaction> valueSerde = new TransactionSerde();
  final String storeName = "transactions";

  final String requestTopic;
  final String requestBackendTopic;
  final String responseBackendTopic;
  final String responseTopic;

  public StatefulRequestKeyValueStoreReplyEnrich(
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
      Stores.keyValueStoreBuilder(
        Stores.inMemoryKeyValueStore(storeName),
        keySerde,
        valueSerde
      )
    );

    b
      .stream(requestTopic, Consumed.with(keySerde, valueSerde))
      .transformValues(
        () ->
          new ValueTransformerWithKey<String, Transaction, Transaction>() {
            ProcessorContext context;
            KeyValueStore<String, Transaction> store;

            @Override
            public void init(ProcessorContext context) {
              this.context = context;
              store = context.getStateStore(storeName);
            }

            @Override
            public Transaction transform(String readOnlyKey, Transaction value) { // kip-820: process(record) {
              store.put(readOnlyKey, value); //kip-820: record.timestamp()
              // context().forward(record)
              return value;
            }

            @Override
            public void close() {} // nothing to close
          },
        storeName
      )
      .to(requestBackendTopic, Produced.with(keySerde, valueSerde));

    b
      .stream(responseBackendTopic, Consumed.with(keySerde, Serdes.String()))
      .transformValues(
        () ->
          new ValueTransformerWithKey<String, String, Transaction>() {
            ProcessorContext context;
            KeyValueStore<String, Transaction> store;

            @Override
            public void init(ProcessorContext context) {
              this.context = context;
              context.schedule(Duration.ofMinutes(1), PunctuationType.WALL_CLOCK_TIME, timestamp -> {});
              store = context.getStateStore(storeName);
            }

            @Override
            public Transaction transform(String readOnlyKey, String value) {
              var transaction = store.delete(readOnlyKey);
              if (transaction == null) {
               // rehydrate store with transaction
              }
              // use transaction for enrichment
              return transaction;
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
    props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);

    final var app = new StatefulRequestKeyValueStoreReplyEnrich(
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
    //    var ks = new KafkaStreams(app.topology(), props);
    //    Runtime.getRuntime().addShutdownHook(new Thread(ks::close));
    //    ks.start();
    //    ks.streamsMetadataForStore(app.storeName)
    //        .stream().map(streamsMetadata -> streamsMetadata.hostInfo().)
  }
}
