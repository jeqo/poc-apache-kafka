package poc;

import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import poc.ItemSerde.ItemSerializer;

public class AppDriver {

  public static void main(String[] args) {
    final Properties props = loadProperties();
    final var producer = new KafkaProducer<>(props, new StringSerializer(), new ItemSerializer());
    producer.send(new ProducerRecord<>("items", "k1", new Item("k1", null, "k1", Map.of("a", "v2"))));
    producer.send(new ProducerRecord<>("items", "k1.1", new Item("k1.1", "k1", "k1.1", Map.of())));
    producer.send(new ProducerRecord<>("items", "k2", new Item("k2", null, "k2", Map.of())));
    producer.close();
  }

  private static Properties loadProperties() {
    final var props = new Properties();
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    return props;
  }
}
