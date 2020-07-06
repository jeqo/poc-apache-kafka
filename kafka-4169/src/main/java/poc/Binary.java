package poc;

import static org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BATCH_SIZE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.COMPRESSION_TYPE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.LINGER_MS_CONFIG;

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

public class Binary {

    public static final String TOPIC_NAME = "t1";
    public static final Random RANDOM = ThreadLocalRandom.current();

    final Producer<byte[], byte[]> producer;

    public Binary(Producer<byte[], byte[]> producer) {
        this.producer = producer;
    }

    public static void main(String[] args) {
        final var compressionType = CompressionType.ZSTD;
        final var serializer = new ByteArraySerializer();
        final var config = new Properties();
        config.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(COMPRESSION_TYPE_CONFIG, compressionType.name);
        config.put(BATCH_SIZE_CONFIG, 1000000);
        config.put(LINGER_MS_CONFIG, 100);
        final var producer = new KafkaProducer<>(config, serializer, serializer);
        final var main = new Binary(producer);
        IntStream.range(0, 1000000).forEach(i -> main.sendMessage(RANDOM.nextInt(10)));
        producer.flush();
        System.out.println(CompressionRatioEstimator.estimation(TOPIC_NAME, compressionType));
        IntStream.range(0, 100).forEach(i -> main.sendMessage(900000));
        producer.flush();
        System.out.println(CompressionRatioEstimator.estimation(TOPIC_NAME, compressionType));
        main.sendMessage(1050000);
        producer.close();
        System.out.println(CompressionRatioEstimator.estimation(TOPIC_NAME, compressionType));
    }

    private static void printMetrics(KafkaProducer<byte[], byte[]> producer) {
        producer.metrics().forEach((name, metric) -> {
            if (name.name().contains("compression-rate-avg"))
                System.out.println(name.name() + "->" + metric.metricValue());
        });
    }

    void sendMessage(int msgSize) {
        final var value = new byte[msgSize];
        RANDOM.nextBytes(value);
        final var record = new ProducerRecord<>(TOPIC_NAME, (byte[]) null, value);
        producer.send(record, (metadata, exception) -> {
            if (exception != null) exception.printStackTrace();
        });
    }
}
