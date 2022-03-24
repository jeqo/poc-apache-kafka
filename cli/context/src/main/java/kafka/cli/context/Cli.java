package kafka.cli.context;

import java.net.Authenticator;
import java.net.PasswordAuthentication;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.Optional;
import kafka.cli.context.Cli.CreateCommand;
import kafka.cli.context.Cli.KCatCommand;
import kafka.cli.context.Cli.PropertiesCommand;
import kafka.cli.context.Cli.TestCommand;
import kafka.cli.context.Cli.SchemaRegistryContextsCommand;
import kafka.cli.context.KafkaContexts.KafkaContext;
import kafka.cli.context.SchemaRegistryContexts.SchemaRegistryAuth;
import kafka.cli.context.SchemaRegistryContexts.SchemaRegistryCluster;
import kafka.cli.context.SchemaRegistryContexts.SchemaRegistryContext;
import kafka.cli.context.SchemaRegistryContexts.UsernamePasswordAuth;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import picocli.CommandLine;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.concurrent.Callable;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Option;

@CommandLine.Command(name = "kfk-ctx", versionProvider = Cli.VersionProviderWithConfigProvider.class, mixinStandardHelpOptions = true, subcommands = {
        CreateCommand.class,
        TestCommand.class,
        PropertiesCommand.class,
        KCatCommand.class,
        SchemaRegistryContextsCommand.class }, descriptionHeading = "Kafka CLI - Context", description = "Manage Kafka connection properties as contexts.")
public class Cli implements Callable<Integer> {

    public static void main(String[] args) {
        int exitCode = new CommandLine(new Cli()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() throws Exception {
        var contexts = KafkaContexts.from(Files.readAllBytes(kafkaContextConfig()));
        System.out.println(contexts.names());
        return 0;
    }

    static void save(KafkaContexts contexts) throws IOException {
        Files.write(kafkaContextConfig(), contexts.serialize());
    }

    static void save(SchemaRegistryContexts contexts) throws IOException {
        Files.write(schemaRegistryContextConfig(), contexts.serialize());
    }

    static Path kafkaContextConfig() throws IOException {
        final Path home = baseDir();

        final var context = home.resolve("kafka.json");
        if (!Files.isRegularFile(context)) {
            System.err.println("Kafka Content configuration file doesn't exist, creating one...");
            Files.write(context, KafkaContexts.empty());
        }

        return context;
    }

    static Path schemaRegistryContextConfig() throws IOException {
        final Path home = baseDir();

        final var context = home.resolve("schema-registry.json");
        if (!Files.isRegularFile(context)) {
            System.err.println("Schema Registry Content configuration file doesn't exist, creating one...");
            Files.write(context, KafkaContexts.empty());
        }

        return context;
    }

    private static Path baseDir() throws IOException {
        final var homePath = System.getProperty("user.home");
        if (homePath.isBlank()) {
            throw new IllegalStateException("Can't find user's home. ${HOME} is empty");
        }

        final var home = Path.of(homePath, ".kafka");
        if (!Files.isDirectory(home)) {
            System.err.println("Kafka Context directory doesn't exist, creating one...");
            Files.createDirectories(home);
        }
        return home;
    }

    @CommandLine.Command(name = "create", description = "Register context. Destination: ~/.kafka/kafka.json")
    static class CreateCommand implements Callable<Integer> {

        @CommandLine.Parameters(index = "0", description = "Context name. e.g. `local`")
        String name;
        @CommandLine.Parameters(index = "1", description = "Kafka bootstrap servers. e.g. `localhost:9092`")
        String bootstrapServers;

        @CommandLine.Option(names = "--auth", description = "Authentication type (default: ${DEFAULT-VALUE}). Valid values: ${COMPLETION-CANDIDATES}", required = true, defaultValue = "PLAINTEXT")
        KafkaContexts.KafkaAuth.AuthType authType;

        @ArgGroup(exclusive = false)
        UsernamePasswordOptions usernamePasswordOptions;

        @Override
        public Integer call() throws Exception {
            var contexts = KafkaContexts.from(Files.readAllBytes(kafkaContextConfig()));

            final KafkaContexts.KafkaAuth auth = switch (authType) {
                case SASL_PLAIN -> new KafkaContexts.UsernamePasswordAuth(
                        authType,
                        usernamePasswordOptions.username,
                        passwordHelper().encrypt(usernamePasswordOptions.password));
                default -> new KafkaContexts.NoAuth();
            };
            final var ctx = new KafkaContext(name, new KafkaContexts.KafkaCluster(bootstrapServers, auth));

            contexts.add(ctx);
            save(contexts);

            System.out.printf("Context %s with bootstrap-servers %s saved.", ctx.name(),
                    ctx.cluster().bootstrapServers());
            return 0;
        }
    }

    static PasswordHelper passwordHelper() {
        try {
            final var saltPath = baseDir().resolve(".salt");
            if (!Files.exists(saltPath)) {
                final var salt = PasswordHelper.generateKey();
                Files.writeString(saltPath, salt);
                return new PasswordHelper(salt);
            }
            else {
                final var salt = Files.readString(saltPath);
                return new PasswordHelper(salt);
            }
        }
        catch (IOException e) {
            throw new IllegalStateException("Password helper not loading", e);
        }
    }

    @CommandLine.Command(name = "properties", description = "Get properties configuration for context")
    static class PropertiesCommand implements Callable<Integer> {

        @CommandLine.Parameters(index = "0", description = "Context name")
        String name;

        @Option(names = { "--schema-registry", "-sr" }, description = "Schema Registry context name")
        Optional<String> schemeRegistryContext;

        @Override
        public Integer call() throws Exception {
            final var contexts = KafkaContexts.from(Files.readAllBytes(kafkaContextConfig()));
            final var ctx = contexts.get(name);
            final var props = ctx.properties(passwordHelper());
            props.store(System.out, "Kafka client properties generated by kfk-ctx");

            if (schemeRegistryContext.isPresent()) {
                final var srContexts = SchemaRegistryContexts.from(Files.readAllBytes(schemaRegistryContextConfig()));
                if (srContexts.has(schemeRegistryContext.get())) {
                    final var srCtx = srContexts.get(schemeRegistryContext.get());
                    final var srProps = srCtx.properties(passwordHelper());

                    srProps.store(System.out, "Schema Registry client properties generated by kfk-ctx");
                }
                else {
                    System.err.printf("WARN: Schema Registry context %s does not exist. Schema Registry connection properties will not be included",
                            schemeRegistryContext.get());
                }
            }
            return 0;
        }
    }

    @CommandLine.Command(name = "test", description = "Test contexts")
    static class TestCommand implements Callable<Integer> {

        @CommandLine.Parameters(index = "0", description = "Context name")
        String name;

        @Option(names = { "--schema-registry", "-sr" }, description = "Schema Registry context name")
        Optional<String> schemeRegistryContext;

        @Override
        public Integer call() throws Exception {
            final var contexts = KafkaContexts.from(Files.readAllBytes(kafkaContextConfig()));
            final var ctx = contexts.get(name);
            final var props = ctx.properties(passwordHelper());

            try (final var admin = AdminClient.create(props)) {
                final var clusterId = admin.describeCluster().clusterId().get();
                System.err.printf("Connection to cluster %s (id=%s) succeed%n", props.get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG), clusterId);
                admin.describeCluster().nodes().get().forEach(node -> System.err.println("Node: " + node));
            }
            catch (Exception e) {
                System.err.println("Connection to cluster failed");
                e.printStackTrace();
                return 1;
            }

            if (schemeRegistryContext.isPresent()) {
                final var srContexts = SchemaRegistryContexts.from(Files.readAllBytes(schemaRegistryContextConfig()));
                if (srContexts.has(schemeRegistryContext.get())) {
                    final var srCtx = srContexts.get(schemeRegistryContext.get());

                    final var auth = srCtx.cluster().auth();
                    final var httpClient = switch (auth.type()) {
                        case BASIC_AUTH -> HttpClient.newBuilder()
                                .authenticator(new Authenticator() {
                                    @Override
                                    protected PasswordAuthentication getPasswordAuthentication() {
                                        final var basicAuth = (UsernamePasswordAuth) auth;
                                        return new PasswordAuthentication(basicAuth.username(), passwordHelper().decrypt(basicAuth.password()).toCharArray());
                                    }
                                })
                                .build();
                        case NO_AUTH -> HttpClient.newHttpClient();
                    };
                    final var urls = srCtx.cluster().urls();
                    final var response = httpClient.send(HttpRequest.newBuilder()
                            .uri(URI.create(urls))
                            .GET()
                            .build(),
                            BodyHandlers.discarding());
                    if (response.statusCode() == 200) {
                        System.err.printf("Connection to schema registry cluster %s succeed%n", urls);
                    }
                    else {
                        System.err.printf("Connection to schema registry cluster %s failed%n", urls);
                        return 1;
                    }
                }
                else {
                    System.err.printf("WARN: Schema Registry context %s does not exist. Schema Registry connection properties will not be tested",
                            schemeRegistryContext.get());
                }
            }
            return 0;
        }
    }

    @CommandLine.Command(name = "kcat", description = "kcat command with properties from context")
    static class KCatCommand implements Callable<Integer> {

        @CommandLine.Parameters(index = "0", description = "Context name")
        String name;

        @Option(names = { "--schema-registry", "-sr" }, description = "Schema Registry context name")
        Optional<String> schemeRegistryContext;

        @Override
        public Integer call() throws Exception {
            final var contexts = KafkaContexts.from(Files.readAllBytes(kafkaContextConfig()));
            final var ctx = contexts.get(name);
            final var kcat = ctx.kcat(passwordHelper());

            if (schemeRegistryContext.isPresent()) {
                final var srContexts = SchemaRegistryContexts.from(Files.readAllBytes(schemaRegistryContextConfig()));
                if (srContexts.has(schemeRegistryContext.get())) {
                    final var srCtx = srContexts.get(schemeRegistryContext.get());
                    final var srProps = srCtx.kcat(passwordHelper());

                    System.out.println(kcat + srProps);
                }
                else {
                    System.err.printf("WARN: Schema Registry context %s does not exist. Schema Registry connection properties will not be included",
                            schemeRegistryContext.get());
                }
            }
            else {
                System.out.println(kcat);
            }
            return 0;
        }
    }

    @CommandLine.Command(name = "sr", subcommands = {
            SchemaRegistryContextsCommand.Create.class }, description = "Manage Schema Registry connection properties as contexts.")
    static class SchemaRegistryContextsCommand implements Callable<Integer> {
        @Override
        public Integer call() throws Exception {
            var contexts = SchemaRegistryContexts.from(Files.readAllBytes(schemaRegistryContextConfig()));
            System.out.println(contexts.names());
            return 0;
        }

        @CommandLine.Command(name = "create", description = "Register context. Destination: ~/.kafka/schema-registry.json")
        static class Create implements Callable<Integer> {

            @CommandLine.Parameters(index = "0", description = "Context name. e.g. `local`")
            String name;
            @CommandLine.Parameters(index = "1", description = "Schema Registry URLs. e.g. `http://localhost:8081`")
            String urls;

            @CommandLine.Option(names = "--auth", description = "Authentication type (default: ${DEFAULT-VALUE}). Valid values: ${COMPLETION-CANDIDATES}", required = true, defaultValue = "PLAINTEXT")
            SchemaRegistryAuth.AuthType authType;

            @ArgGroup(exclusive = false)
            UsernamePasswordOptions usernamePasswordOptions;

            @Override
            public Integer call() throws Exception {
                var contexts = SchemaRegistryContexts.from(Files.readAllBytes(schemaRegistryContextConfig()));

                final SchemaRegistryAuth auth;
                switch (authType) {
                    case BASIC_AUTH -> auth = new SchemaRegistryContexts.UsernamePasswordAuth(authType, usernamePasswordOptions.username,
                            passwordHelper().encrypt(usernamePasswordOptions.password));
                    default -> auth = new SchemaRegistryContexts.NoAuth();
                }
                final var ctx = new SchemaRegistryContext(name, new SchemaRegistryCluster(urls, auth));

                contexts.add(ctx);
                save(contexts);

                System.out.printf("Context %s with URLs %s saved.", ctx.name(),
                        ctx.cluster().urls());
                return 0;
            }
        }
    }

    static class UsernamePasswordOptions {
        @CommandLine.Option(names = { "--username", "-u" }, description = "Username authentication")
        String username;
        @CommandLine.Option(names = { "--password", "-p" }, description = "Password authentication", arity = "0..1", interactive = true)
        String password;
    }

    static class VersionProviderWithConfigProvider implements CommandLine.IVersionProvider {

        @Override
        public String[] getVersion() throws IOException {
            final var url = VersionProviderWithConfigProvider.class.getClassLoader().getResource("cli.properties");
            if (url == null) {
                return new String[]{ "No cli.properties file found in the classpath." };
            }
            final var properties = new Properties();
            properties.load(url.openStream());
            return new String[]{
                    properties.getProperty("appName") + " version " + properties.getProperty("appVersion") + "",
                    "Built: " + properties.getProperty("appBuildTime"), };
        }
    }
}
