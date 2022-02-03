package poc;

import java.util.Properties;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class AppDriver {

  public static void main(String[] args) {
    final Properties props = loadProperties();
    final var producer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());
    producer.send(new ProducerRecord<>("input", "k1", "v1"));
    producer.send(new ProducerRecord<>("input", "k2", "v2"));
    producer.send(new ProducerRecord<>("input", "k3", "v3"));
    producer.close();
  }

  private static Properties loadProperties() {
    final var props = new Properties();
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    return props;
  }
}
