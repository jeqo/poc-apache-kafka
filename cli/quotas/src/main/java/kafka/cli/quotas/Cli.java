package kafka.cli.quotas;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.concurrent.Callable;
import org.apache.kafka.clients.admin.AdminClient;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "kfk-quotas")
public class Cli implements Callable<Integer> {

    @Option(names = {"-c", "--config"}, description = "Kafka configuration properties", required = true)
    Path config;

    public static void main(String[] args) {
        int exitCode = new CommandLine(new Cli()).execute(args);
        System.exit(exitCode);
    }

    @Override public Integer call() throws Exception {
        final var props = new Properties();
        props.load(Files.newInputStream(config));

        final var kafkaAdmin = AdminClient.create(props);
        final var quotaManager = new QuotaManager(kafkaAdmin);

        final var quotas = quotaManager.all();
        System.out.println(quotas.toJson());
        return 0;
    }
}
