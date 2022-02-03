package poc;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class AppPrep {

  public static void main(String[] args) throws ExecutionException, InterruptedException {
    final Properties props = loadProperties();
    final var admin = AdminClient.create(props);
    admin.createTopics(List.of(
        new NewTopic("input", 1, (short) 1),
        new NewTopic("output", 1, (short) 1),
        new NewTopic("output-table", 1, (short) 1)
    )).all().get();
    admin.close();
  }

  private static Properties loadProperties() {
    final var props = new Properties();
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    return props;
  }
}
