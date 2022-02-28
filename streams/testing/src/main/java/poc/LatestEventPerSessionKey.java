package poc;

import java.time.Duration;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.Stores;

public class LatestEventPerSessionKey {

  public static void main(String[] args) {
    var t = build();
    System.out.println(t.describe());
  }

  public static Topology build() {
    var s = new StreamsBuilder();

    s.addStateStore(
        Stores.sessionStoreBuilder(Stores.persistentSessionStore("sessions", Duration.ofDays(7)),
            Serdes.String(), Serdes.String()));

    // https://github.com/apache/kafka/blob/1e0916580f16b99b911b0ed36e9740dcaeef520e/streams/src/main/java/org/apache/kafka/streams/kstream/internals/KStreamSessionWindowAggregate.java
    final TransformerSupplier<Object, Object, KeyValue<Object, Object>> session = () -> new Transformer<>() {
      SessionStore<Object, Object> store;
      ProcessorContext context;

      @Override
      public void init(ProcessorContext context) {
        this.context = context;
        store = context.getStateStore("session");
      }

      @Override
      public KeyValue<Object, Object> transform(Object key, Object value) {
        try (var iter = store.fetch(key)) {
          if (iter.hasNext()) {
            // (maybe) test if the context.timestamp is higher than the latest record
            store.remove(iter.next().key);
          }
        }
        store.put(
            // choose a different key
            new Windowed<>(
                key,
                new SessionWindow(context.timestamp(), context.timestamp())),
            value
        );

        // later
        // store.fetch(record.key());
        return KeyValue.pair(key, value);
      }

      @Override
      public void close() {
      }
    };

    var stream = s.stream("input");
    stream.transform(session);

    s.stream("input", Consumed.with(Serdes.ByteArray(), Serdes.ByteBuffer()))
        .groupByKey()
        .windowedBy(SessionWindows.ofInactivityGapWithNoGrace(Duration.ofMinutes(1)))
        .reduce((value1, value2) -> value2, Materialized.as("session"));
    return s.build();
  }

}
