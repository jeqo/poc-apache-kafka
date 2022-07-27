package poc.consumer;

import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroDeserializer;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import poc.Topics;
import poc.avro.User;

public class AvroConsumer {

  public static void main(String[] args) {
    // Prepare connection
    final var props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "avro-consumer-v1");
    final var valueDeserializer = new SpecificAvroDeserializer<User>();
    final var srProps = Map.of(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
    valueDeserializer.configure(srProps, false);
    final var consumer = new KafkaConsumer<>(props, new StringDeserializer(), valueDeserializer);
    // Subscribe and poll
    consumer.subscribe(List.of(Topics.POC_FORWARD.name()));
    while (!Thread.interrupted()) {
      final var records = consumer.poll(Duration.ofSeconds(5));
      for (final var record : records) {
        final var value = record.value();
        final var username = value.getUsername();
        final var createdAt = value.getCreateAt();
        //        final var country = value.getCountry();
        //        final var city = value.getCity();

        System.out.printf(
          "Record received from %s-%s@%s: {username=%s, createdAt=%s" +
          //                + ", at %s:%s"
          "}%n",
          record.topic(),
          record.partition(),
          record.offset(),
          username,
          createdAt
          //            , country, city
        );
      }
      consumer.commitAsync();
    }
  }
}
