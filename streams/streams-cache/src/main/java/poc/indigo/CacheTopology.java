package poc.indigo;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.streams.serdes.json.KafkaJsonSchemaSerde;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.extension.annotations.WithSpan;
import java.time.Duration;
import java.util.function.Supplier;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;
import poc.jsonschema2json.Customer;
import poc.jsonschema2json.Product;
import poc.jsonschema2json.Signal;

/**
 * Cache topologies implemented here prepare the cache state locally. See {@link StreamingCache} to
 * validate how cache state is accessed from outside the Kafka Streams topology.
 * <p/>
 * See <a href="https://docs.confluent.io/platform/current/streams/developer-guide/interactive-queries.html">Kafka
 * Streams Interactive Queries documentation</a>.
 */
public class CacheTopology implements Supplier<Topology> {

  static final ObjectMapper jsonMapper = new ObjectMapper();

  KafkaJsonSchemaSerde<Object> valueSerde;
  KafkaJsonSchemaSerde<Signal> outputValueSerde;

  public CacheTopology(KafkaJsonSchemaSerde<Object> valueSerde, KafkaJsonSchemaSerde<Signal> outputValueSerde) {
    this.valueSerde = valueSerde;
    this.outputValueSerde = outputValueSerde;
  }

  @Override
  public Topology get() {
    var b = new StreamsBuilder();
    windowedCache(b);
    return b.build();
  }

  void windowedCache(StreamsBuilder builder) {
    final var storeName = "window-cache";
    final var storeBuilder = Stores.windowStoreBuilder(
      //        Stores.persistentWindowStore(storeName, Duration.ofMinutes(10), Duration.ofMinutes(1),
      //            false),
      Stores.inMemoryWindowStore(storeName, Duration.ofMinutes(10), Duration.ofMinutes(1), false),
      Serdes.String(),
      valueSerde
    );
    builder.addStateStore(storeBuilder);

    builder
      .stream("all-events", Consumed.with(Serdes.String(), valueSerde).withName("consume-event"))
      .transformValues(
        () ->
          new ValueTransformerWithKey<>() {
            ProcessorContext context;
            WindowStore<String, Object> store;

            @Override
            public void init(ProcessorContext context) {
              this.context = context;
              this.store = context.getStateStore(storeName);
            }

            @Override
            @WithSpan // Custom trace of the transform method
            public Object transform(String key, Object value) {
              Span.current().setAttribute("msg.key", key); // add new attr.

              try {
                store.put(key, value, context.timestamp());
              } catch (Exception e) {
                e.printStackTrace();
              }
              return value;
            }

            @Override
            public void close() {}
          },
        Named.as("cache"),
        storeName
      )
      .mapValues(
        (readOnlyKey, value) -> {
          // TODO not working yet
          var jsonNode = jsonMapper.valueToTree(value);
          var id = "";
          try {
            switch (jsonNode.get("event_type").textValue()) {
              case "customer_event":
                id = (jsonMapper.treeToValue(jsonNode, Customer.class)).getCustomerId();
                break;
              case "product_event":
                id = (jsonMapper.treeToValue(jsonNode, Product.class)).getProductId();
            }
          } catch (JsonProcessingException e) {
            e.printStackTrace();
          }
          // end TODO
          final var signal = new Signal();
          signal.setId(id);
          signal.setRef("/window-cache/" + readOnlyKey);
          return signal;
        },
        Named.as("map-to-signal")
      )
      .to("signal", Produced.with(Serdes.String(), outputValueSerde).withName("produce-signal"));
  }
}
