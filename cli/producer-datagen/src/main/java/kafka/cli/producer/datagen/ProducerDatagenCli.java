package kafka.cli.producer.datagen;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.io.IOException;
import kafka.cli.producer.datagen.PayloadGenerator.Format;
import kafka.cli.producer.datagen.ProducerDatagenCli.VersionProviderWithConfigProvider;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import picocli.CommandLine;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import picocli.CommandLine.IVersionProvider;
import picocli.CommandLine.Option;

@CommandLine.Command(
    name = "kproducerdatagen",
    versionProvider = VersionProviderWithConfigProvider.class,
    mixinStandardHelpOptions = true,
    description = "Kafka Performance Producer with Data generation",
    subcommands = {
        ProducerDatagenCli.Run.class,
        ProducerDatagenCli.Interval.class,
        ProducerDatagenCli.ProduceOnce.class,
        ProducerDatagenCli.ListQuickstarts.class,
    })
public class ProducerDatagenCli implements Callable<Integer> {

    public static void main(String[] args) {
        int exitCode = new CommandLine(new ProducerDatagenCli()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() {
        CommandLine.usage(this, System.out);
        return 0;
    }

    @CommandLine.Command(name = "perf", description = "run performance tests")
    static class Run implements Callable<Integer> {

        @CommandLine.Option(names = {"-t",
            "--topic"}, description = "target Kafka topic name", required = true)
        String topicName;
        @CommandLine.Option(names = {"-q",
            "--quickstart"}, description = "Quickstart name. For list of available quickstarts, run `quickstarts` subcommand", required = true)
        Quickstart quickstart;
        @CommandLine.Option(names = {"-n",
            "--num-records"}, description = "Number of records to produce", required = true)
        long numRecords;
        @CommandLine.Option(names = {"-k",
            "--throughput"}, description = "Number of target records per second to produce", defaultValue = "-1")
        long throughput = -1L;

        @CommandLine.Option(names = {"-c", "--config"}, description =
            "Client configuration properties file."
                + "Must include connection to Kafka and Schema Registry", required = true)
        Path configPath;

        @Option(names = {"-f",
            "--format"}, description = "Record value format", defaultValue = "JSON")
        Format format;

        int reportingInterval = 5_000;
        boolean shouldPrintMetrics = false;

        boolean transactionEnabled = false;
        long transactionDuration = 100L;

        @Override
        public Integer call() throws Exception {
            var producerConfig = new Properties();
            producerConfig.load(Files.newInputStream(configPath));
            var keySerializer = new StringSerializer();
            Serializer<Object> valueSerializer;
            if (format.equals(Format.AVRO)) {
                valueSerializer = new KafkaAvroSerializer();
                valueSerializer.configure(producerConfig.keySet().stream()
                    .collect(Collectors.toMap(String::valueOf, producerConfig::get)), false);
            } else {
                valueSerializer = (Serializer) new StringSerializer();
            }
            try (var producer = new KafkaProducer<>(producerConfig, keySerializer,
                valueSerializer)) {
                final var records = numRecords;
                final var targetThroughput = throughput;
                var pp = new PerformanceRun(
                    new PerformanceRun.Config(records, topicName, transactionEnabled,
                        transactionDuration, shouldPrintMetrics), producer, new PayloadGenerator(
                    new PayloadGenerator.Config(Optional.empty(), Optional.of(quickstart),
                        Optional.empty(),
                        Optional.empty(), records, format)),
                    new ThroughputThrottler(System.currentTimeMillis(), targetThroughput),
                    new Stats(records, reportingInterval));
                pp.start();
            }
            return 0;
        }
    }

    @CommandLine.Command(name = "interval", description = "run producer with interval")
    static class Interval implements Callable<Integer> {

        @CommandLine.Option(names = {"-t",
            "--topic"}, description = "target Kafka topic name", required = true)
        String topicName;
        @CommandLine.Option(names = {"-q",
            "--quickstart"}, description = "Quickstart name. For list of available quickstarts, run `quickstarts` subcommand", required = true)
        Quickstart quickstart;
        @CommandLine.Option(names = {"-n",
            "--num-records"}, description = "Number of records to produce", required = true)
        long numRecords;
        @CommandLine.Option(names = {"-i",
            "--interval"}, description = "Maximum interval between producer send", defaultValue = "5000")
        long interval;

        @CommandLine.Option(names = {"-c", "--config"}, description =
            "Client configuration properties file."
                + "Must include connection to Kafka and Schema Registry", required = true)
        Path configPath;

        @Option(names = {"-f",
            "--format"}, description = "Record value format", defaultValue = "JSON")
        Format format;

        int reportingInterval = 5_000;

        @Override
        public Integer call() throws Exception {
            var producerConfig = new Properties();
            producerConfig.load(Files.newInputStream(configPath));
            var keySerializer = new StringSerializer();
            Serializer<Object> valueSerializer;
            if (format.equals(Format.AVRO)) {
                valueSerializer = new KafkaAvroSerializer();
                valueSerializer.configure(producerConfig.keySet().stream()
                    .collect(Collectors.toMap(String::valueOf, producerConfig::get)), false);
            } else {
                valueSerializer = (Serializer) new StringSerializer();
            }
            try (var producer = new KafkaProducer<>(producerConfig, keySerializer,
                valueSerializer)) {
                final var maxRecords = numRecords;
                final var maxInterval = interval;
                var pp = new IntervalRun(
                    new IntervalRun.Config(topicName, maxRecords, maxInterval), producer,
                    new PayloadGenerator(
                        new PayloadGenerator.Config(Optional.empty(), Optional.of(quickstart),
                            Optional.empty(),
                            Optional.empty(), maxRecords, format)),
                    new Stats(maxRecords, reportingInterval));
                pp.start();
            }
            return 0;
        }
    }

    @CommandLine.Command(name = "once", description = "produce once")
    static class ProduceOnce implements Callable<Integer> {

        @CommandLine.Option(names = {"-t",
            "--topic"}, description = "target Kafka topic name", required = true)
        String topicName;
        @CommandLine.Option(names = {"-q",
            "--quickstart"}, description = "Quickstart name. For list of available quickstarts, run `quickstarts` subcommand", required = true)
        Quickstart quickstart;

        @CommandLine.Option(names = {"-c", "--config"}, description =
            "Client configuration properties file."
                + "Must include connection to Kafka and Schema Registry", required = true)
        Path configPath;

        @Option(names = {"-f",
            "--format"}, description = "Record value format", defaultValue = "JSON")
        Format format;

        @Override
        public Integer call() throws Exception {
            var producerConfig = new Properties();
            producerConfig.load(Files.newInputStream(configPath));
            var keySerializer = new StringSerializer();
            Serializer<Object> valueSerializer;
            if (format.equals(Format.AVRO)) {
                valueSerializer = new KafkaAvroSerializer();
                valueSerializer.configure(producerConfig.keySet().stream()
                    .collect(Collectors.toMap(String::valueOf, producerConfig::get)), false);
            } else {
                valueSerializer = (Serializer) new StringSerializer();
            }
            try (var producer = new KafkaProducer<>(producerConfig, keySerializer,
                valueSerializer)) {
                var pg = new PayloadGenerator(
                    new PayloadGenerator.Config(Optional.empty(), Optional.of(quickstart),
                        Optional.empty(),
                        Optional.empty(), 10, format));
                var record = pg.get();
                Object value;
                if (format.equals(Format.JSON)) {
                    value = pg.toJson(record);
                } else {
                    value = record;
                }
                var meta = producer.send(new ProducerRecord<>(topicName, pg.key(record), value))
                    .get();
                System.out.println("Record sent. " + meta);
            }
            return 0;
        }
    }

    @CommandLine.Command(name = "quickstarts", description = "Lists available quickstarts")
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
