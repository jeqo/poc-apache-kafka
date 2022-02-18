package kafka.producer.performance.datagen;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;
import picocli.CommandLine;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;


@CommandLine.Command(
        name = "producer-perf-datagen",
        version = "0.1.0-SNAPSHOT",
        description = "",
        subcommands = {
                Cli.Run.class,
                Cli.ListQuickstarts.class,
        }
)
public class Cli implements Callable<Integer> {

    @Override
    public Integer call() {
        return 0;
    }

    @CommandLine.Command(
            name = "run",
            description = "run performance tests"
    )
    static class Run implements Callable<Integer> {
        @CommandLine.Option(
                names = {"-t", "--topic"},
                description = "target Kafka topic name",
                required = true)
        String topicName;
        @CommandLine.Option(
                names = {"-q", "--quickstart"},
                description = "Quickstart name. For list of available quickstarts, run `quickstarts` subcommand",
                required = true)
        Quickstart quickstart;
        @CommandLine.Option(
                names = {"-n", "--num-records"},
                description = "Number of records to produce",
                required = true)
        long numRecords;
        @CommandLine.Option(
                names = {"-k", "--throughput"},
                description = "Number of target records per second to produce",
                defaultValue = "-1")
        long throughput = -1L;

        @CommandLine.Option(
                names = {"-c", "--config"},
                description = "Client configuration properties file." +
                        "Must include connection to Kafka and Schema Registry",
                required = true)
        Path configPath;

        int reportingInterval = 5_000;
        boolean shouldPrintMetrics = false;

        boolean transactionEnabled = false;
        long transactionDuration = 100L;

        @Override
        public Integer call() throws Exception {
            var producerConfig = new Properties();
            producerConfig.load(Files.newInputStream(configPath));
            var keySerializer = new StringSerializer();
            var valueSerializer = new KafkaAvroSerializer();
            valueSerializer.configure(
                    producerConfig.keySet().stream()
                            .collect(Collectors.toMap(String::valueOf, producerConfig::get)),
                    false);
            var producer = new KafkaProducer<>(producerConfig, keySerializer, valueSerializer);
            final var records = numRecords;
            final var targetThroughput = throughput;
            var pp = new ProducerPerformance(
                    new ProducerPerformance.Config(
                            records,
                            topicName,
                            transactionEnabled,
                            transactionDuration,
                            shouldPrintMetrics),
                    producer,
                    new PayloadGenerator(new PayloadGenerator.Config(
                            Optional.empty(),
                            Optional.of(quickstart),
                            Optional.empty(),
                            Optional.empty(),
                            records)),
                    new ThroughputThrottler(System.currentTimeMillis(), targetThroughput),
                    new Stats(records, reportingInterval)
            );
            pp.start();
            return 0;


        }
    }

    @CommandLine.Command(
            name = "quickstarts",
            description = "Lists available quickstarts"
    )
    static class ListQuickstarts implements Callable<Integer> {

        @Override
        public Integer call() {
            System.out.println("Available quickstart:");
            for (Quickstart q : Quickstart.values()) {
                System.out.printf("\t%s%n", q.name());
            }
            return 0;
        }
    }
}
