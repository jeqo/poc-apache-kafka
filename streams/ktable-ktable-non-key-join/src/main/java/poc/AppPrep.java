package poc;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;

public class AppPrep {

  public static void main(String[] args) throws ExecutionException, InterruptedException {
    final Properties props = loadProperties();
    final var admin = AdminClient.create(props);
    admin.createTopics(List.of(
        new NewTopic("items", 1, (short) 1),
        new NewTopic("joined", 1, (short) 1)
    )).all().get();
    admin.close();
  }

  private static Properties loadProperties() {
    final var props = new Properties();
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    return props;
  }
}
