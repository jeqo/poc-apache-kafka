package poc;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class PocOutput {
    public static void main(String[] args) {
        var consConfigs = new Properties();
        consConfigs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, "g1");
        consConfigs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        var consumer = new KafkaConsumer<>(consConfigs, new StringDeserializer(), new StringDeserializer());
        consumer.subscribe(List.of("output"));
        var records = consumer.poll(Duration.ofSeconds(5));
        for (var record: records) {
            System.out.println(record);
        }
        consumer.close();
    }
}
