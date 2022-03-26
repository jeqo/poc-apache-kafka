package kafka.cli.context;

import static java.lang.System.err;
import static java.lang.System.out;
import static kafka.cli.context.Cli.*;
import static kafka.cli.context.Helper.*;

import java.io.IOException;
import java.net.Authenticator;
import java.net.PasswordAuthentication;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.file.Files;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Callable;
import kafka.cli.context.KafkaContexts.KafkaContext;
import kafka.cli.context.SchemaRegistryContexts.SchemaRegistryAuth;
import kafka.cli.context.SchemaRegistryContexts.SchemaRegistryCluster;
import kafka.cli.context.SchemaRegistryContexts.SchemaRegistryContext;
import kafka.cli.context.SchemaRegistryContexts.UsernamePasswordAuth;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

@CommandLine.Command(
    name = KFK_CTX_CMD,
    versionProvider = Cli.VersionProviderWithConfigProvider.class,
    mixinStandardHelpOptions = true,
    subcommands = {
      CreateCommand.class,
      DeleteCommand.class,
      TestCommand.class,
      PropertiesCommand.class,
      KCatCommand.class,
      SchemaRegistryContextsCommand.class
    },
    description = "Manage Kafka connection as contexts.")
public class Cli implements Callable<Integer> {

  public static final String KFK_CTX_CMD = "kfk-ctx";

  @Option(names = {"-v", "--verbose"})
  boolean verbose;

  public static void main(String[] args) {
    int exitCode = new CommandLine(new Cli()).execute(args);
    System.exit(exitCode);
  }

  @Override
  public Integer call() throws Exception {
    var contexts = KafkaContexts.from(Files.readAllBytes(kafkaContextConfig()));
    if (verbose) {
      out.println(contexts.namesAndBootstrapServers());
    } else {
      out.println(contexts.names());
    }
    return 0;
  }

  @CommandLine.Command(
      name = "create",
      description = "Register a Kafka context. Destination: ~/.kafka/kafka.json")
  static class CreateCommand implements Callable<Integer> {

    @CommandLine.Parameters(index = "0", description = "Kafka context name. e.g. `local`")
    String name;

    @CommandLine.Parameters(index = "1", description = "Bootstrap servers. e.g. `localhost:9092`")
    String bootstrapServers;

    @CommandLine.Option(
        names = "--auth",
        description =
            "Authentication method (default: ${DEFAULT-VALUE}). Valid values: ${COMPLETION-CANDIDATES}",
        required = true,
        defaultValue = "PLAINTEXT")
    KafkaContexts.KafkaAuth.AuthType authType;

    @ArgGroup(exclusive = false)
    UsernamePasswordOptions usernamePasswordOptions;

    @Override
    public Integer call() throws Exception {
      final var contexts = KafkaContexts.from(Files.readAllBytes(kafkaContextConfig()));

      final KafkaContexts.KafkaAuth auth =
          switch (authType) {
            case SASL_PLAIN -> new KafkaContexts.UsernamePasswordAuth(
                authType,
                usernamePasswordOptions.username,
                passwordHelper().encrypt(usernamePasswordOptions.password));
            default -> new KafkaContexts.NoAuth();
          };
      final var ctx =
          new KafkaContext(name, new KafkaContexts.KafkaCluster(bootstrapServers, auth));

      contexts.add(ctx);
      save(contexts);

      out.printf(
          "Kafka context `%s` with bootstrap-servers [%s] is saved.",
          ctx.name(), ctx.cluster().bootstrapServers());
      return 0;
    }
  }

  @CommandLine.Command(
      name = "delete",
      description = "Removes context. Destination: ~/.kafka/kafka.json")
  static class DeleteCommand implements Callable<Integer> {

    @CommandLine.Parameters(index = "0", description = "Kafka context name. e.g. `local`")
    String name;

    @Override
    public Integer call() throws Exception {
      var contexts = KafkaContexts.from(Files.readAllBytes(kafkaContextConfig()));

      if (contexts.has(name)) {
        final var ctx = contexts.get(name);
        contexts.remove(name);
        save(contexts);

        out.printf(
            "Kafka context `%s` with bootstrap servers: [%s] is deleted.%n",
            ctx.name(), ctx.cluster().bootstrapServers());
        return 0;
      } else {
        out.printf("Kafka context `%s` is not registered.%n", name);
        return 1;
      }
    }
  }

  @CommandLine.Command(
      name = "properties",
      description = "Get properties configurations for contexts")
  static class PropertiesCommand implements Callable<Integer> {

    @CommandLine.Parameters(index = "0", description = "Kafka context name")
    String name;

    @Option(
        names = {"--schema-registry", "-sr"},
        description = "Schema Registry context name")
    Optional<String> schemeRegistryContext;

    @Override
    public Integer call() throws Exception {
      final var contexts = KafkaContexts.from(Files.readAllBytes(kafkaContextConfig()));
      if (contexts.has(name)) {
        final var ctx = contexts.get(name);
        final var props = ctx.properties(passwordHelper());
        props.store(out, "Kafka client properties generated by " + KFK_CTX_CMD);

        if (schemeRegistryContext.isPresent()) {
          final var srContexts =
              SchemaRegistryContexts.from(Files.readAllBytes(schemaRegistryContextConfig()));
          if (srContexts.has(schemeRegistryContext.get())) {
            final var srCtx = srContexts.get(schemeRegistryContext.get());
            final var srProps = srCtx.properties(passwordHelper());

            srProps.store(out, "Schema Registry client properties generated by " + KFK_CTX_CMD);
          } else {
            System.err.printf(
                "WARN: Schema Registry context %s does not exist. "
                    + "Schema Registry connection properties will not be included",
                schemeRegistryContext.get());
          }
        }
      } else {
        err.printf("Kafka context `%s` is not found%n", name);
        return 1;
      }
      return 0;
    }
  }

  @CommandLine.Command(name = "test", description = "Test cluster contexts")
  static class TestCommand implements Callable<Integer> {

    @CommandLine.Parameters(index = "0", description = "Kafka context name")
    String name;

    @Option(
        names = {"--schema-registry", "-sr"},
        description = "Schema Registry context name")
    Optional<String> schemeRegistryContext;

    @Override
    public Integer call() throws Exception {
      final var contexts = KafkaContexts.from(Files.readAllBytes(kafkaContextConfig()));
      if (contexts.has(name)) {
        final var ctx = contexts.get(name);
        final var props = ctx.properties(passwordHelper());

        final var bootstrapServers = props.get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG);
        try (final var admin = AdminClient.create(props)) {
          final var clusterId = admin.describeCluster().clusterId().get();
          System.err.printf(
              "Connection to Kafka `%s` [%s] (id=%s) succeed%n", name, bootstrapServers, clusterId);
          admin
              .describeCluster()
              .nodes()
              .get()
              .forEach(node -> System.err.println("Node: " + node));
        } catch (Exception e) {
          System.err.printf("Connection to Kafka `%s` [%s] failed%n", name, bootstrapServers);
          e.printStackTrace();
          return 1;
        }

        if (schemeRegistryContext.isPresent()) {
          final var srContexts =
              SchemaRegistryContexts.from(Files.readAllBytes(schemaRegistryContextConfig()));
          final var sr = schemeRegistryContext.get();
          if (srContexts.has(sr)) {
            final var srCtx = srContexts.get(sr);

            final var auth = srCtx.cluster().auth();
            final var httpClient =
                switch (auth.type()) {
                  case BASIC_AUTH -> HttpClient.newBuilder()
                      .authenticator(
                          new Authenticator() {
                            @Override
                            protected PasswordAuthentication getPasswordAuthentication() {
                              final var basicAuth = (UsernamePasswordAuth) auth;
                              return new PasswordAuthentication(
                                  basicAuth.username(),
                                  passwordHelper().decrypt(basicAuth.password()).toCharArray());
                            }
                          })
                      .build();
                  case NO_AUTH -> HttpClient.newHttpClient();
                };
            final var urls = srCtx.cluster().urls();
            final var response =
                httpClient.send(
                    HttpRequest.newBuilder().uri(URI.create(urls)).GET().build(),
                    BodyHandlers.discarding());
            if (response.statusCode() == 200) {
              System.err.printf("Connection to Schema Registry `%s` [%s] succeed%n", sr, urls);
            } else {
              System.err.printf(
                  "Connection to Schema Registry `%s` URL(s): [%s] failed%n", sr, urls);
              return 1;
            }
          } else {
            System.err.printf(
                "WARN: Schema Registry context %s does not exist. "
                    + "Schema Registry connection properties will not be tested",
                sr);
          }
        }
      } else {
        err.printf("Kafka context `%s` is not found%n", name);
        return 1;
      }
      return 0;
    }
  }

  @CommandLine.Command(name = "kcat", description = "kcat command with properties from context")
  static class KCatCommand implements Callable<Integer> {

    @CommandLine.Parameters(index = "0", description = "Context name")
    String name;

    @Option(
        names = {"--schema-registry", "-sr"},
        description = "Schema Registry context name")
    Optional<String> schemeRegistryContext;

    @Override
    public Integer call() throws Exception {
      final var contexts = KafkaContexts.from(Files.readAllBytes(kafkaContextConfig()));
      final var ctx = contexts.get(name);
      final var kcat = ctx.kcat(passwordHelper());

      if (schemeRegistryContext.isPresent()) {
        final var srContexts =
            SchemaRegistryContexts.from(Files.readAllBytes(schemaRegistryContextConfig()));
        if (srContexts.has(schemeRegistryContext.get())) {
          final var srCtx = srContexts.get(schemeRegistryContext.get());
          final var srProps = srCtx.kcat(passwordHelper());

          out.println(kcat + srProps);
        } else {
          System.err.printf(
              "WARN: Schema Registry context %s does not exist. Schema Registry connection properties will not be included",
              schemeRegistryContext.get());
        }
      } else {
        out.println(kcat);
      }
      return 0;
    }
  }

  @CommandLine.Command(
      name = "sr",
      subcommands = {
        SchemaRegistryContextsCommand.CreateCommand.class,
        SchemaRegistryContextsCommand.DeleteCommand.class
      },
      description = "Manage Schema Registry connection properties as contexts.")
  static class SchemaRegistryContextsCommand implements Callable<Integer> {
    @Option(names = {"-v", "--verbose"})
    boolean verbose;

    @Override
    public Integer call() throws Exception {
      var contexts = SchemaRegistryContexts.from(Files.readAllBytes(schemaRegistryContextConfig()));
      if (verbose) {
        out.println(contexts.namesAndUrls());
      } else {
        out.println(contexts.names());
      }
      return 0;
    }

    @CommandLine.Command(
        name = "create",
        description = "Register context. Destination: ~/.kafka/schema-registry.json")
    static class CreateCommand implements Callable<Integer> {

      @CommandLine.Parameters(index = "0", description = "Context name. e.g. `local`")
      String name;

      @CommandLine.Parameters(
          index = "1",
          description = "Schema Registry URLs. e.g. `http://localhost:8081`")
      String urls;

      @CommandLine.Option(
          names = "--auth",
          description =
              "Authentication type (default: ${DEFAULT-VALUE}). Valid values: ${COMPLETION-CANDIDATES}",
          required = true,
          defaultValue = "PLAINTEXT")
      SchemaRegistryAuth.AuthType authType;

      @ArgGroup(exclusive = false)
      UsernamePasswordOptions usernamePasswordOptions;

      @Override
      public Integer call() throws Exception {
        var contexts =
            SchemaRegistryContexts.from(Files.readAllBytes(schemaRegistryContextConfig()));

        final SchemaRegistryAuth auth =
            switch (authType) {
              case BASIC_AUTH -> new SchemaRegistryContexts.UsernamePasswordAuth(
                  authType,
                  usernamePasswordOptions.username,
                  passwordHelper().encrypt(usernamePasswordOptions.password));
              default -> new SchemaRegistryContexts.NoAuth();
            };
        final var ctx = new SchemaRegistryContext(name, new SchemaRegistryCluster(urls, auth));

        contexts.add(ctx);
        save(contexts);

        out.printf(
            "Schema Registry context `%s` with URL(s): [%s] is saved.",
            ctx.name(), ctx.cluster().urls());
        return 0;
      }
    }

    @CommandLine.Command(
        name = "delete",
        description = "Removes context. Destination: ~/.kafka/schema-registry.json")
    static class DeleteCommand implements Callable<Integer> {

      @CommandLine.Parameters(index = "0", description = "Context name. e.g. `local`")
      String name;

      @Override
      public Integer call() throws Exception {
        final var contexts =
            SchemaRegistryContexts.from(Files.readAllBytes(schemaRegistryContextConfig()));

        if (contexts.has(name)) {
          final var ctx = contexts.get(name);
          contexts.remove(name);
          save(contexts);

          out.printf(
              "Schema Registry context `%s` with URL(s): [%s] is deleted.%n",
              ctx.name(), ctx.cluster().urls());
          return 0;
        } else {
          out.printf("Schema Registry Context %s is not registered.%n", name);
          return 1;
        }
      }
    }
  }

  static class UsernamePasswordOptions {
    @CommandLine.Option(
        names = {"--username", "-u"},
        description = "Username authentication")
    String username;

    @CommandLine.Option(
        names = {"--password", "-p"},
        description = "Password authentication",
        arity = "0..1",
        interactive = true)
    String password;
  }

  static class VersionProviderWithConfigProvider implements CommandLine.IVersionProvider {

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
