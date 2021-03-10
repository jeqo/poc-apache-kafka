package cloudday;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.ConfigResource.Type;

public class AdminApp {

  public static void main(String[] args)
      throws ExecutionException, InterruptedException, IOException {
    var config = Config.common();

    var admin = AdminClient.create(config);

    // Check existing topics
    var existingTopics = admin.listTopics().names().get();
    System.out.printf("Topics: %s%n", existingTopics);

    // Define your new topic
    var topicName = "topic5";
    var partitions = 6;
    var replicationFactor = (short) 3;

    if (!existingTopics.contains(topicName)) {
      // Create topic
      var newTopic = new NewTopic(topicName, partitions, replicationFactor)
          //.configs(Map.of("min.insync.replicas", "1"))
          ;
      admin.createTopics(
          List.of(newTopic),
          new CreateTopicsOptions().validateOnly(false))
          .all().get();

    } else {
      System.out.printf("Topic %s already exists%n%n", topicName);
    }

    // Describe topic
    var descriptions = admin.describeTopics(List.of(topicName))
        .all().get();

    var topicDescription = descriptions.get(topicName);
    topicDescription.partitions().forEach(topicPartitionInfo -> {
      System.out.printf("Partition: %s%n", topicPartitionInfo.partition());

      System.out.println("Replicas:");
      System.out.println(topicPartitionInfo.replicas()
          .stream().map(Node::toString)
          .collect(Collectors.joining("\n")));

      System.out.printf("Replica Leader: %s%n", topicPartitionInfo.leader());

      System.out.println("In-Sync Replicas:");
      System.out.println(topicPartitionInfo.isr()
          .stream().map(Node::toString)
          .collect(Collectors.joining("\n")));
    });

    // Describe topic configuration
    var configResource = new ConfigResource(Type.TOPIC, topicName);
    var resourceConfigs =
        admin.describeConfigs(List.of(configResource)).values().get(configResource).get();
    System.out.println("Topic configs: ");
    System.out.println(
        resourceConfigs.entries()
            .stream().map(e -> e.name() + "=" + e.value())
            .collect(Collectors.joining("\n")));
  }
}
