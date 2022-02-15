package kafka.datagen;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.Properties;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;

public class App {
    public static void main(String[] args) throws IOException {
        var producerConfig = new Properties();
        producerConfig.load(Files.newInputStream(Path.of("client.properties")));
        var producer = new KafkaProducer<String, GenericRecord>(producerConfig);
        final var records = 10_000_000;
        final var targetThroughput = 1_000;
        var pp = new ProducerPerformance(
            new ProducerPerformance.Config(records, "jeqo-test-v1", false, 100, false),
            producer,
            new PayloadGenerator(new PayloadGenerator.Config(
                Optional.empty(),
                Optional.of(Quickstart.CLICKSTREAM),
                Optional.empty(),
                Optional.empty(),
                records)),
            new ThroughputThrottler(System.currentTimeMillis(), targetThroughput),
            new Stats(records, 5000)
        );
        pp.start();
    }
}
