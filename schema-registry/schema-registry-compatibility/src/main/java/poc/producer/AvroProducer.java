package poc.producer;

import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerializer;
import java.time.Instant;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import poc.Topics;
import poc.avro.User;

public class AvroProducer {

  public static void main(String[] args) {
    // Prepare connection
    final var props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "avro-producer");
    final var valueSerializer = new SpecificAvroSerializer<User>();
    final var srProps = Map.of(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
    valueSerializer.configure(srProps, false);
    final var producer = new KafkaProducer<>(props, new StringSerializer(), valueSerializer);
    // Produce Avro records
    final var value = User
      .newBuilder()
      .setUsername("Jorge")
      .setCreateAt(Instant.now())
      .setCountry("UK")
      //        .setCity("London")
      .build();
    final var key = value.getUsername();
    final var record = new ProducerRecord<>(Topics.POC_BACKWARD.name(), key, value);
    // Send
    producer.send(
      record,
      (metadata, exception) -> {
        if (exception == null) {
          System.out.printf("Record acked: %s%n", metadata);
        } else {
          System.err.printf("Exception when sending record: %s%n", exception.getMessage());
          exception.printStackTrace();
        }
      }
    );
    // Flush
    producer.flush();
    // Close
    producer.close();
  }
}
