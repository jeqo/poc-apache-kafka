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
    producer.send(new ProducerRecord<>("words", "k1", "test,abc,def"));
    producer.send(new ProducerRecord<>("words", "k2", "v2,v3"));
    producer.send(new ProducerRecord<>("words", "k3", "a3,a4"));
    producer.close();
  }

  private static Properties loadProperties() {
    final var props = new Properties();
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    return props;
  }
}
