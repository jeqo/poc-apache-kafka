package kafka.topics;

import java.nio.file.Files;
import java.nio.file.Path;
import java.text.NumberFormat;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import picocli.CommandLine;

@CommandLine.Command(
        name = "kafka-list-topics",
        version = "0.1.0-SNAPSHOT"
)
public class ListTopics implements Callable<Integer> {

  public static void main(String[] args) {
    int exitCode = new CommandLine(new ListTopics()).execute(args);
    System.exit(exitCode);
  }

  @CommandLine.Option(names = {"-t", "--topics"}, description = "list of topic names to include")
  List<String> topics = new ArrayList<>();

  @CommandLine.Option(names = {"-p", "--prefix"}, description = "Topic name prefix")
  Optional<String> prefix = Optional.empty();

  @CommandLine.Option(
          names = {"-c", "--config"},
          description = "Client configuration properties file." +
                  "Must include connection to Kafka and Schema Registry",
          required = true)
  Path configPath;

  @Override
  public Integer call() throws Exception {
    var config = new Config(topics, prefix);
    var clientConfig = new Properties();
    clientConfig.load(Files.newInputStream(configPath));
    var adminClient = AdminClient.create(clientConfig);
    List<String> list = listOfTopics(config, adminClient);
    System.out.println("List of topics:");
    System.out.println(list);
    var described = adminClient.describeTopics(list).allTopicNames().get();
    var configs = adminClient.describeConfigs(list.stream().map(t -> new ConfigResource(ConfigResource.Type.TOPIC, t)).toList()).all().get();
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
    var numberFormat = NumberFormat.getInstance();
    for (String topic: list) {
      System.out.printf("Topic: %s%n", topic);
      System.out.println("Configs: " + configs.get(new ConfigResource(ConfigResource.Type.TOPIC, topic)));
      for (var tp : tpsByTopic.get(topic)) {
        System.out.printf("\tPartition %s => offsets[%s-%s]%n",
                tp.partition(),
                numberFormat.format(startOffsets.get(tp).offset()),
                numberFormat.format(endOffsets.get(tp).offset()));
      }
    }
    return 0;
  }

  private List<String> listOfTopics(Config config, AdminClient adminClient) throws InterruptedException, ExecutionException {
    var listing = adminClient.listTopics(new ListTopicsOptions()).listings().get();
    var list = listing.stream()
            .map(TopicListing::name)
            .filter(l -> config.topics().contains(l) || config.prefix().map(l::startsWith)
                    .orElse(true))
            .toList();
    return list.isEmpty() ?  listing.stream().map(TopicListing::name).toList() : list;
  }

  record Config(
      List<String> topics,
      Optional<String> prefix
  ) {

  }
}
