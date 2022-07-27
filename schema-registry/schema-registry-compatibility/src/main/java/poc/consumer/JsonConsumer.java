package poc.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import poc.Topics;

public class JsonConsumer {

  public static void main(String[] args) throws JsonProcessingException {
    final var jsonMapper = new ObjectMapper();
    // Prepare connection
    final var props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "json-consumer-v1");
    final var consumer = new KafkaConsumer<>(props, new StringDeserializer(), new StringDeserializer());
    // Subscribe and poll
    consumer.subscribe(List.of(Topics.POC_YOLO.name()));
    while (!Thread.interrupted()) {
      final var records = consumer.poll(Duration.ofSeconds(5));
      for (final var record : records) {
        final var value = record.value();
        final var valueJson = jsonMapper.readTree(value);
        final var username = valueJson.get("username").asText();
        //        final var createdAtText = valueJson.get("created_at").asText();
        //        final var createdAt = LocalDateTime.parse(createdAtText, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
        // or was it unix-time
        final var createdAtLong = valueJson.get("created_at").asText();
        final var createdAt = Instant.ofEpochMilli(Long.parseLong(createdAtLong));
        System.out.printf(
          "Record received from %s-%s@%s: {username=%s, createdAt=%s}%n",
          record.topic(),
          record.partition(),
          record.offset(),
          username,
          createdAt
        );
      }
      consumer.commitAsync();
    }
  }
}
