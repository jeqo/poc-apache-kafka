package kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class ControlProducerExample {
  public static void main(String[] args) throws ExecutionException, InterruptedException {
    final var config =
        Map.<String, Object>of(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
            ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, ProgressControlInterceptor.class.getName(),
            ProducerConfig.ACKS_CONFIG, "1");
    try (final var producer =
        new KafkaProducer<>(config, new StringSerializer(), new StringSerializer())) {
      producer.send(new ProducerRecord<>("input", "k1", "v1")).get();
      System.out.println("wait!");
      Thread.sleep(Duration.ofMinutes(1).toMillis());
    }
  }
}
