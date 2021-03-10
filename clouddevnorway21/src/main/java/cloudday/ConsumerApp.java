package cloudday;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class ConsumerApp {

  public static void main(String[] args) throws IOException {
    var cfg = Config.common();
    cfg.put(ConsumerConfig.GROUP_ID_CONFIG, "mygroup");

    try (var c = new KafkaConsumer<>(cfg, new StringDeserializer(), new StringDeserializer())) {
      c.subscribe(List.of("hello-world"));

      while (!Thread.interrupted()) {
        var records = c.poll(Duration.ofSeconds(1));
        if (!records.isEmpty()) {
          // business logic
          records.forEach(System.out::println);

          c.commitAsync();
        }
      }
    }
  }
}
