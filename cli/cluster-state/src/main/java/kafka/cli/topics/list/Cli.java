package kafka.cli.topics.list;

import static java.lang.System.err;
import static java.lang.System.out;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Callable;
import kafka.context.KafkaContexts;
import kafka.cli.topics.list.Cli.VersionProviderWithConfigProvider;
import org.apache.kafka.clients.admin.AdminClient;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.IVersionProvider;
import picocli.CommandLine.Option;

@Command(
    name = "kfk-cluster-state",
    descriptionHeading = "Kafka CLI - Topic list",
    description =
        "List Kafka topics with metadata, partitions, replica placement, configuration, and offsets at once.",
    versionProvider = VersionProviderWithConfigProvider.class,
    mixinStandardHelpOptions = true)
public class Cli implements Callable<Integer> {

  public static void main(String[] args) {
    int exitCode = new CommandLine(new Cli()).execute(args);
    System.exit(exitCode);
  }

  @Option(
      names = {"-t", "--topics"},
      description = "list of topic names to include")
  List<String> topics = new ArrayList<>();

  @Option(
      names = {"-p", "--prefix"},
      description = "Topic name prefix")
  Optional<String> prefix = Optional.empty();

  @ArgGroup(multiplicity = "1")
  PropertiesOption propertiesOption;

  @Option(
      names = {"--pretty"},
      defaultValue = "false",
      description = "Print pretty/formatted JSON")
  boolean pretty;

  @Override
  public Integer call() throws Exception {
    final var opts = new Opts(topics, prefix);

    final var clientConfig = propertiesOption.load();

    try (var adminClient = AdminClient.create(clientConfig)) {
      final var helper = new Helper(adminClient);
      final var output = helper.run(opts);
      out.println(output.toJson(pretty));
    }
    return 0;
  }

  record Opts(List<String> topics, Optional<String> prefix) {

    public boolean match(String name) {
      return topics.contains(name) || prefix.map(name::startsWith).orElse(true);
    }
  }


  static class PropertiesOption {

    @CommandLine.Option(
        names = {"-c", "--config"},
        description =
            "Client configuration properties file."
                + "Must include connection to Kafka")
    Optional<Path> configPath;

    @ArgGroup(exclusive = false)
    ContextOption contextOption;

    public Properties load() {
      return configPath.map(path -> {
            try {
              final var p = new Properties();
              p.load(Files.newInputStream(path));
              return p;
            } catch (Exception e) {
              throw new IllegalArgumentException(
                  "ERROR: properties file at %s is failing to load".formatted(path));
            }
          })
          .orElseGet(() -> {
            try {
              return contextOption.load();
            } catch (IOException e) {
              throw new IllegalArgumentException("ERROR: loading contexts");
            }
          });
    }
  }

  static class ContextOption {

    @Option(
        names = "--kafka",
        description = "Kafka context name",
        required = true
    )
    String kafkaContextName;

    public Properties load() throws IOException {
      final var kafkas = KafkaContexts.load();
      final var props = new Properties();
      if (kafkas.has(kafkaContextName)) {
        final var kafka = kafkas.get(kafkaContextName);
        final var kafkaProps = kafka.properties();
        props.putAll(kafkaProps);

        return props;
      } else {
        err.printf("ERROR: Kafka context `%s` not found. Check that context already exist.%n",
            kafkaContextName);
        return null;
      }
    }
  }

  static class VersionProviderWithConfigProvider implements IVersionProvider {

    @Override
    public String[] getVersion() throws IOException {
      final var url =
          VersionProviderWithConfigProvider.class.getClassLoader().getResource("cli.properties");
      if (url == null) {
        return new String[] {"No cli.properties file found in the classpath."};
      }
      final var properties = new Properties();
      properties.load(url.openStream());
      return new String[] {
        properties.getProperty("appName") + " version " + properties.getProperty("appVersion") + "",
        "Built: " + properties.getProperty("appBuildTime"),
      };
    }
  }
}
