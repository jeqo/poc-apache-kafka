package kafka.cli.topics.list;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Callable;
import kafka.cli.topics.list.Cli.VersionProviderWithConfigProvider;
import org.apache.kafka.clients.admin.AdminClient;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.IVersionProvider;
import picocli.CommandLine.Option;

import static java.lang.System.out;

@Command(
    name = "ktopiclist",
    descriptionHeading = "Kafka CLI - Topic list",
    description = "List Kafka topics with metadata, partitions, replica placement, configuration, and offsets at once.",
    versionProvider = VersionProviderWithConfigProvider.class,
    mixinStandardHelpOptions = true
)
public class Cli implements Callable<Integer> {

  public static void main(String[] args) {
    int exitCode = new CommandLine(new Cli()).execute(args);
    System.exit(exitCode);
  }

  @Option(names = {"-t", "--topics"}, description = "list of topic names to include")
  List<String> topics = new ArrayList<>();

  @Option(names = {"-p", "--prefix"}, description = "Topic name prefix")
  Optional<String> prefix = Optional.empty();

  @Option(
      names = {"-c", "--config"},
      description = "Client configuration properties file." +
          "Must include connection to Kafka and Schema Registry",
      required = true)
  Path configPath;

  @Option(names = {"--pretty"}, defaultValue = "false", description = "Print pretty/formatted JSON")
  boolean pretty;

  @Override
  public Integer call() throws Exception {
    final var opts = new Opts(topics, prefix);

    final var clientConfig = new Properties();
    clientConfig.load(Files.newInputStream(configPath));

    try (var adminClient = AdminClient.create(clientConfig)) {
      final var helper = new Helper(adminClient);
      final var output = helper.run(opts);
      out.println(output.toJson(pretty));
    }
    return 0;
  }

  record Opts(
      List<String> topics,
      Optional<String> prefix
  ) {

    public boolean match(String name) {
      return topics.contains(name) || prefix.map(name::startsWith).orElse(true);
    }
  }

  static class VersionProviderWithConfigProvider implements IVersionProvider {

    @Override
    public String[] getVersion() throws IOException {
      final var url = VersionProviderWithConfigProvider.class.getClassLoader()
          .getResource("cli.properties");
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
