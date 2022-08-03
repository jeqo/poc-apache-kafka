package poc.stateless;

import java.util.function.Consumer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.kstream.ValueTransformerWithKeySupplier;
import org.apache.kafka.streams.processor.ProcessorContext;

public class StatelessSplitBranches {

  Topology build() {
    var b = new StreamsBuilder();

//    var s = b.stream("abc");
//    s.mapValues().to();
//    s.mapValues().to();

    var branches = b.stream("t1", Consumed.with(Serdes.String(), Serdes.String()))
      .split()
      .branch(
        (key, value) -> value.startsWith("a"),
        Branched.as("A")
      )
      .branch(
        (key, value) -> value.startsWith("b"),
        Branched.as("B")
      )
      .branch((key, value) -> value.startsWith("C"),
        Branched.withConsumer(ks -> new SubProcess().accept(ks)))
      .defaultBranch();

    branches.forEach((name, ks) -> finalizeProcess(ks));

    return b.build();
  }

  static class StorePayment implements ValueTransformerWithKeySupplier<String, String, String> {

    @Override
    public ValueTransformerWithKey<String, String, String> get() {
      return new ValueTransformerWithKey<>() {
        @Override
        public void init(ProcessorContext context) {

        }

        @Override
        public String transform(String readOnlyKey, String value) {
          return null;
        }

        @Override
        public void close() {

        }
      };
    }
  }

  static class SubProcess implements Consumer<KStream<String, String>> {

    @Override
    public void accept(KStream<String, String> ks) {
      // to mq
      var s1 = ks
        .mapValues((key, value) -> value.split(" "));
      // to status
      s1.to("mq");
      ks.mapValues(s -> s).to("status");
      //.through("test")
      //s1.repartition("appid-name-repartition");
    }
  }

  public static void main(String[] args) {
    var app = new StatelessSplitBranches();
    var tp = app.build();
    System.out.println(tp.describe());
  }

  private void finalizeProcess(KStream<String, String> ks) {
    ks.to("abc", Produced.with(Serdes.String(), Serdes.String()));
  }
}
