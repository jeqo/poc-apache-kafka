package kafka.cli.quotas;

import static java.lang.System.err;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Callable;
import kafka.cli.quotas.Cli.CreateCommand;
import kafka.cli.quotas.Quotas.ConnectionCreationRate;
import kafka.cli.quotas.Quotas.Constraint;
import kafka.cli.quotas.Quotas.KafkaClient;
import kafka.cli.quotas.Quotas.KafkaClientEntity;
import kafka.cli.quotas.Quotas.NetworkBandwidth;
import kafka.cli.quotas.Quotas.Quota;
import kafka.cli.quotas.Quotas.RequestRate;
import kafka.context.KafkaContexts;
import org.apache.kafka.clients.admin.AdminClient;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "kfk-quotas", subcommands = {CreateCommand.class})
public class Cli implements Callable<Integer> {

    @ArgGroup(multiplicity = "1")
    PropertiesOption propertiesOption;

    public static void main(String[] args) {
        int exitCode = new CommandLine(new Cli()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() throws Exception {
        final var props = propertiesOption.load();
        final var kafkaAdmin = AdminClient.create(props);
        final var quotaManager = new QuotaManager(kafkaAdmin);

        final var quotas = quotaManager.all();
        System.out.println(quotas.toJson());
        return 0;
    }

    @Command(name = "create")
    static class CreateCommand implements Callable<Integer> {

        @ArgGroup(multiplicity = "1")
        PropertiesOption propertiesOption;

        @Option(names = {"--user-default"}, description = "Default to all users")
        boolean userDefault;
        @Option(names = {"--user"}, description = "Application's User Principal")
        Optional<String> user;
        @Option(names = {"--client-id-default"}, description = "Default to all client IDs")
        boolean clientIdDefault;
        @Option(names = {"--client-id"}, description = "Application's Client ID")
        Optional<String> clientId;
        @Option(names = {"--ip-default"}, description = "Default to all IPs")
        boolean ipDefault;
        @Option(names = {"--ip"}, description = "Application's IP")
        Optional<String> ip;

        @Option(names = {"--write"}, description = "Write bandwidth")
        Optional<Long> writeBandwidth;
        @Option(names = {"--read"}, description = "Read bandwidth")
        Optional<Long> readBandwidth;
        @Option(names = {"--request-rate"}, description = "Request rate")
        Optional<Long> requestRate;
        @Option(names = {"--connection-rate"}, description = "Connection creation rate")
        Optional<Long> connectionRate;

        @Override
        public Integer call() throws Exception {
            final var props = propertiesOption.load();
            final var kafkaAdmin = AdminClient.create(props);
            final var quotaManager = new QuotaManager(kafkaAdmin);
            final var quota = new Quota(
                new KafkaClient(
                    new KafkaClientEntity(userDefault, user),
                    new KafkaClientEntity(clientIdDefault, clientId),
                    new KafkaClientEntity(ipDefault, ip)
                ),
                new Constraint(
                    writeBandwidth.map(NetworkBandwidth::new),
                    readBandwidth.map(NetworkBandwidth::new),
                    requestRate.map(RequestRate::new),
                    connectionRate.map(ConnectionCreationRate::new)
                )
            );
            quotaManager.create(quota);
            return 0;
        }
    }


    static class PropertiesOption {

        @CommandLine.Option(
            names = {"-c", "--config"},
            description =
                "Client configuration properties file."
                    + "Must include connection to Kafka and Schema Registry")
        Optional<Path> configPath;

        @ArgGroup(exclusive = false)
        ContextOption contextOption;

        public Properties load() {
            return configPath
                .map(
                    path -> {
                        try {
                            final var p = new Properties();
                            p.load(Files.newInputStream(path));
                            return p;
                        } catch (Exception e) {
                            throw new IllegalArgumentException(
                                "ERROR: properties file at %s is failing to load".formatted(path));
                        }
                    })
                .orElseGet(
                    () -> {
                        try {
                            return contextOption.load();
                        } catch (IOException e) {
                            throw new IllegalArgumentException("ERROR: loading contexts");
                        }
                    });
        }
    }

    static class ContextOption {

        @Option(names = "--kafka", description = "Kafka context name", required = true)
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
                err.printf(
                    "ERROR: Kafka context `%s` not found. Check that context already exist.%n",
                    kafkaContextName);
                return null;
            }
        }
    }

}
