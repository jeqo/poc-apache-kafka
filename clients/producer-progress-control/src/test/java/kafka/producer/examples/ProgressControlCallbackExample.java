package kafka.producer.examples;

import kafka.producer.ProgressControlCallback;
import kafka.producer.ProgressControlConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class ProgressControlCallbackExample {
  public static void main(String[] args) throws ExecutionException, InterruptedException {
    final var config =
        Map.<String, Object>of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    try (final var producer =
        new KafkaProducer<>(config, new StringSerializer(), new StringSerializer())) {
      var callback =
          new ProgressControlCallback<>(producer, ProgressControlConfig.newBuilder().build());
      producer.send(new ProducerRecord<>("input", "k1", "v1"), callback).get();
      System.out.println("wait!");
      Thread.sleep(Duration.ofMinutes(1).toMillis());
    }
  }
}
