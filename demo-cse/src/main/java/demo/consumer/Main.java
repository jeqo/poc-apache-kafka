package demo.consumer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;

public class Main {

  public static void main(String[] args) {
    var bootstrapServers = System.getenv("KAFKA_BOOTSTRAP_SERVERS");
    var topic = System.getenv("KAFKA_TOPIC");
    var username = System.getenv("KAFKA_USERNAME");
    var password = System.getenv("KAFKA_PASSWORD");
    var groupId = System.getenv("KAFKA_GROUP_ID");

    if (bootstrapServers == null) {
      System.err.println("KAFKA_BOOTSTRAP_SERVERS not found!");
      System.exit(1);
    }
    if (topic == null) {
      System.err.println("KAFKA_TOPIC not found!");
      System.exit(1);
    }
    if (groupId == null) {
      System.err.println("KAFKA_GROUP_ID not found!");
      System.exit(1);
    }
    if (username == null || password == null) {
      System.err.println("Username/Password not found!");
      System.exit(1);
    }

    var configs = new Properties();
    configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    configs.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    configs.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");

    configs.put(SaslConfigs.SASL_JAAS_CONFIG,
        String.format("""
            org.apache.kafka.common.security.plain.PlainLoginModule
             required username="%s"
             password="%s";
            """, username, password));
    configs.put(SaslConfigs.SASL_MECHANISM, "PLAIN");

    configs.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "https");

    var consumer = new KafkaConsumer<>(configs, new StringDeserializer(), new StringDeserializer());
    consumer.subscribe(List.of(topic));
    while (!Thread.interrupted()) {
      var records = consumer.poll(Duration.ofSeconds(1));
      for (var record : records) {
        System.out.printf("""
            Record %s-%s-%s: %s
            """,
            record.topic(),
            record.partition(),
            record.offset(),
            record.value());
      }
      consumer.commitAsync();
    }
  }
}
