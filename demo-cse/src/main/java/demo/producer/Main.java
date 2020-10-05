package demo.producer;

import java.time.Duration;
import java.util.Properties;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.serialization.StringSerializer;

public class Main {

  public static void main(String[] args) throws InterruptedException {
    var bootstrapServers = System.getenv("KAFKA_BOOTSTRAP_SERVERS");
    var topic = System.getenv("KAFKA_TOPIC");
    var username = System.getenv("KAFKA_USERNAME");
    var password = System.getenv("KAFKA_PASSWORD");

    if (bootstrapServers == null) {
      System.err.println("KAFKA_BOOTSTRAP_SERVERS not found!");
      System.exit(1);
    }
    if (topic == null) {
      System.err.println("KAFKA_TOPIC not found!");
      System.exit(1);
    }
    if (username == null || password == null) {
      System.err.println("Username/Password not found!");
      System.exit(1);
    }

    var configs = new Properties();
    configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    configs.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");

    configs.put(SaslConfigs.SASL_JAAS_CONFIG,
        String.format("""
            org.apache.kafka.common.security.plain.PlainLoginModule
             required username="%s"
             password="%s";
            """, username, password));
    configs.put(SaslConfigs.SASL_MECHANISM, "PLAIN");

    configs.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "https");

    var producer = new KafkaProducer<>(configs, new StringSerializer(), new StringSerializer());

    while (!Thread.interrupted()) {
      Thread.sleep(Duration.ofSeconds(5).toMillis());
      var record = new ProducerRecord<String, String>(topic, """
          { "hello": "world" }
          """);
      producer.send(record, (metadata, exception) -> {
        if (exception != null) {
          System.err.println("Error sending message");
          exception.printStackTrace();
        }
      });
    }
  }
}
