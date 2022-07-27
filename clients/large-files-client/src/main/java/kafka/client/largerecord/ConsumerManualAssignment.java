package kafka.client.largerecord;

import java.io.FileInputStream;
import java.io.IOException;
import java.time.Duration;
import java.util.Properties;
import java.util.Set;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * Manual assignment skips any rebalancing policy and
 * allow consumers to poll directly from specific partitions/offsets
 */
public class ConsumerManualAssignment {

  public static void main(String[] args) {
    var consumer = new KafkaConsumer<>(props(), new StringDeserializer(), new StringDeserializer());
    final var topicPartition = new TopicPartition("test1", 0);
    consumer.assign(Set.of(topicPartition));
    consumer.seek(topicPartition, 0);
    while (!Thread.interrupted()) {
      for (var record : consumer.poll(Duration.ofSeconds(1))) {
        System.out.println(record);
      }
    }
  }

  static Properties props() {
    var props = new Properties();
    try (
      final var inputStream = new FileInputStream("clients/large-files-client/src/main/resources/consumer.properties")
    ) {
      props.load(inputStream);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return props;
  }
}
