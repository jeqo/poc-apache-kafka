package kafka;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.TopicPartition;

public class ListTopics {

  public static void main(String[] args)
      throws IOException, ExecutionException, InterruptedException {
    var config = new Config(List.of("test"), Optional.of("jeqo-"));
    var clientConfig = new Properties();
    clientConfig.load(Files.newInputStream(Path.of("client.properties")));
    var adminClient = AdminClient.create(clientConfig);
    var listing = adminClient.listTopics(new ListTopicsOptions()).listings().get();
    var list = listing.stream()
        .map(TopicListing::name)
        .filter(l -> config.topics().contains(l) || config.prefix().map(l::startsWith)
            .orElse(true))
        .toList();
    System.out.println(list);
    var described = adminClient.describeTopics(list).allTopicNames().get();
    var tpsByTopic = new LinkedHashMap<String, List<TopicPartition>>();
    var startOffsetRequest = new LinkedHashMap<TopicPartition, OffsetSpec>();
    var endOffsetRequest = new LinkedHashMap<TopicPartition, OffsetSpec>();
    for (String topic : list) {
      var description = described.get(topic);
      var tps = description.partitions().stream().map(tpi -> new TopicPartition(topic, tpi.partition()))
          .sorted(Comparator.comparingInt(TopicPartition::partition))
          .toList();
      tps.forEach(topicPartition -> {
        startOffsetRequest.put(topicPartition, OffsetSpec.earliest());
        endOffsetRequest.put(topicPartition, OffsetSpec.latest());
      });
      tpsByTopic.put(topic, tps);
    }
    var startOffsets = adminClient.listOffsets(startOffsetRequest).all().get();
    var endOffsets = adminClient.listOffsets(endOffsetRequest).all().get();
    for (String topic: list) {
      System.out.printf("Topic: %s%n", topic);
      for (var tp : tpsByTopic.get(topic)) {
        System.out.printf("\tPartition %s: %s - %s%n", tp.partition(), startOffsets.get(tp).offset(), endOffsets.get(tp).offset());
      }
    }
  }

  record Config(
      List<String> topics,
      Optional<String> prefix
  ) {

  }
}
