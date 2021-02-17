package poc;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class PocInput {
    public static void main(String[] args) {
        var prodConfigs = new Properties();
        prodConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        var producer = new KafkaProducer<>(prodConfigs, new StringSerializer(), new StringSerializer());
        var record = new ProducerRecord<>("input", "k2", "v1.0");
        record.headers().add("a", "a".getBytes());
        producer.send(record);
        producer.close();
    }
}
