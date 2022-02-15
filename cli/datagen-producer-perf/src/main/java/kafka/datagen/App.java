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
        var pp = new ProducerPerformance(
            new ProducerPerformance.Config(1000000, "jeqo-test-v1", false, 100, false),
            producer,
            new PayloadGenerator(new PayloadGenerator.Config(
                Optional.empty(),
                Optional.of(Quickstart.CLICKSTREAM),
                Optional.empty(),
                Optional.empty(),
                1000000)),
            new ThroughputThrottler(System.currentTimeMillis(), 1000),
            new Stats(1000000, 5000)
        );
        pp.start();
    }
}
