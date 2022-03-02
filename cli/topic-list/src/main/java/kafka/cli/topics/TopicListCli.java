package kafka.cli.topics;

import static java.lang.System.out;

import java.io.IOException;
import java.util.stream.Collectors;
import kafka.cli.topics.TopicListCli.VersionProviderWithConfigProvider;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import picocli.CommandLine;

import java.nio.file.Files;
import java.nio.file.Path;
import java.text.NumberFormat;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import picocli.CommandLine.IVersionProvider;

@CommandLine.Command(
    name = "ktopiclist",
    versionProvider = VersionProviderWithConfigProvider.class,
    mixinStandardHelpOptions = true
)
public class TopicListCli implements Callable<Integer> {

    public static void main(String[] args) {
        int exitCode = new CommandLine(new TopicListCli()).execute(args);
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
        try (var adminClient = AdminClient.create(clientConfig)) {
            List<String> list = listOfTopics(config, adminClient);
            out.println("List of topics:");
            out.println(list);
            out.println();
            var described = adminClient.describeTopics(list).allTopicNames().get();
            var configs = adminClient.describeConfigs(
                    list.stream().map(t -> new ConfigResource(ConfigResource.Type.TOPIC, t)).toList())
                .all()
                .get();
            var tpsByTopic = new LinkedHashMap<String, List<TopicPartition>>();
            var startOffsetRequest = new LinkedHashMap<TopicPartition, OffsetSpec>();
            var endOffsetRequest = new LinkedHashMap<TopicPartition, OffsetSpec>();
            for (String topic : list) {
                var description = described.get(topic);
                var tps = description.partitions().stream()
                    .map(tpi -> new TopicPartition(topic, tpi.partition()))
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
            final var length = String.valueOf(Long.MAX_VALUE).length();
            for (String topic : list) {
                var description = described.get(topic);
                var tps = description.partitions().stream()
                    .collect(Collectors.toMap(TopicPartitionInfo::partition, tpi -> tpi));
                out.printf("Topic: %s (UID: %s)%n", topic, description.topicId());
                out.println(
                    " Configs: " + configs.get(
                        new ConfigResource(ConfigResource.Type.TOPIC, topic)));
                for (var tp : tpsByTopic.get(topic)) {
                    var tpi = tps.get(tp.partition());
                    out.println(" Partitions:");
                    out.printf("  %s: [Leader: %s] [ISR: %s] %n",
                        tp.partition(),
                        tpi.leader().id(),
                        tpi.isr().stream()
                            .map(node -> node.hasRack() ?
                                node.id() + " (" + node.host() + "@rack:" + node.rack() + ")" :
                                node.id() + " (" + node.host() + ")")
                            .toList());
                    out.printf("   offsets: [ %s - %s ]%n",
                        String.format("%1$" + length + "s",
                            numberFormat.format(startOffsets.get(tp).offset())).replace(' ', '_'),
                        String.format("%1$" + length + "s",
                            numberFormat.format(endOffsets.get(tp).offset())).replace(' ', '_'));
                }
                out.println();
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
        return list.isEmpty() ? listing.stream().map(TopicListing::name).toList() : list;
    }

    record Config(
        List<String> topics,
        Optional<String> prefix
    ) {

    }

    static class VersionProviderWithConfigProvider implements IVersionProvider {

        @Override
        public String[] getVersion() throws IOException {
            final var url = VersionProviderWithConfigProvider.class.getClassLoader().getResource("cli.properties");
            if (url == null) {
                return new String[]{"No cli.properties file found in the classpath."};
            }
            final var properties = new Properties();
            properties.load(url.openStream());
            return new String[]{
                properties.getProperty("appName") + " version " + properties.getProperty(
                    "appVersion") + "", "Built: " + properties.getProperty("appBuildTime"),
            };
        }
    }
}
