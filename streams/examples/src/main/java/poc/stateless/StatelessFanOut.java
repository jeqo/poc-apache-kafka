package poc.stateless;

import java.util.Arrays;
import java.util.List;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;

public class StatelessFanOut {

  public static void main(String[] args) {
    var b = new StreamsBuilder();

    // a:v1
    b.stream("input", Consumed.with(Serdes.String(), Serdes.String()))
        // 1 record -> * records
        .flatMapValues(StatelessFanOut::getStrings)
        // a:v1-topic1
        // a:v1-topic2
        // a:v1-topic3
        .to((key, value, recordContext) -> 
            new String(recordContext.headers().lastHeader("topic").value()));

    System.out.println(b.build().describe());
  }

  private static List<String> getStrings(String value) {
    return Arrays.asList(value.split(","));
  }

}
