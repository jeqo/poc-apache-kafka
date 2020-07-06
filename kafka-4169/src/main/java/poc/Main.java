package poc;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BATCH_SIZE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.COMPRESSION_TYPE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.LINGER_MS_CONFIG;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.record.CompressionRatioEstimator;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.serialization.ByteArraySerializer;

public class Main {

    public static final String TOPIC_NAME = "t1";
    public static final Random RANDOM = ThreadLocalRandom.current();

    final Producer<byte[], byte[]> producer;

    public Main(Producer<byte[], byte[]> producer) {
        this.producer = producer;
    }

    public static void main(String[] args) throws IOException {
        final var compressionType = CompressionType.SNAPPY;
        final var serializer = new ByteArraySerializer();
        final var config = new Properties();
        config.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(COMPRESSION_TYPE_CONFIG, compressionType.name);
        final var producer = new KafkaProducer<>(config, serializer, serializer);
        final var main = new Main(producer);
        final var msg = Files.readAllBytes(Paths.get("src/main/resources/random.txt"));
        IntStream.rangeClosed(0, 100).forEach(i -> main.sendMessage(msg));
        producer.flush();
        System.out.println(msg.length);
        System.out.println(CompressionRatioEstimator.estimation(TOPIC_NAME, compressionType));
        main.sendMessage(msg);
        producer.flush();
        System.out.println(msg.length);
        System.out.println(CompressionRatioEstimator.estimation(TOPIC_NAME, compressionType));
        final var dmsg = Files.readAllBytes(Paths.get("src/main/resources/double-random.txt"));
        main.sendMessage(dmsg);
        producer.close();
        System.out.println(dmsg.length);
        System.out.println(CompressionRatioEstimator.estimation(TOPIC_NAME, compressionType));
    }

    private static void printMetrics(KafkaProducer<byte[], byte[]> producer) {
        producer.metrics().forEach((name, metric) -> {
            if (name.name().contains("compression-rate-avg"))
                System.out.println(name.name() + "->" + metric.metricValue());
        });
    }

    void sendMessage(byte[] msg) {
        final var record = new ProducerRecord<>(TOPIC_NAME, (byte[]) null, msg);
        producer.send(record, (metadata, exception) -> {
            if (exception != null) exception.printStackTrace();
        });
    }
}
