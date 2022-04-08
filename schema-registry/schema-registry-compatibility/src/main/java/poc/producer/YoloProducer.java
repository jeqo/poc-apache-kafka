package poc.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import poc.Topics;

public class YoloProducer {

  public static void main(String[] args) throws JsonProcessingException {
    // Prepare connection
    final var props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "yolo-producer");
    final var producer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());
    // Prepare message
    final var jsonMapper = new ObjectMapper();
    final var valueJson = jsonMapper.createObjectNode()
        .put("username", "Jorge")
        .put("created_at", LocalDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME));
    // or is it unix-time better?
//         .put("created_at", System.currentTimeMillis());
    final var value = jsonMapper.writeValueAsString(valueJson);
    final var record = new ProducerRecord<>(Topics.POC_BACKWARD.name(),
        valueJson.get("username").asText(), value);
    // Send
    producer.send(record, (metadata, exception) -> {
      if (exception == null) {
        System.out.printf("Record acked: %s%n", metadata);
      } else {
        System.err.printf("Exception when sending record: %s%n", exception.getMessage());
        exception.printStackTrace();
      }
    });
    // Flush
    producer.flush();
    // Close
    producer.close();
  }
}
