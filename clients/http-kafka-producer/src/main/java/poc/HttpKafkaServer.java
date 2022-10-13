package poc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.server.annotation.Post;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;

public class HttpKafkaServer {

  public static void main(String[] args) throws IOException {
    final var config = new Properties();
    config.load(Files.newInputStream(Path.of("kafka.properties")));
    final var producer = new KafkaProducer<>(
        config,
        new ByteArraySerializer(),
        new ByteArraySerializer()
       );
    final var service = new HttpKafkaService(producer, config.getProperty("topic"));
    final ServerBuilder sb = Server.builder()
        .annotatedService(service)
        .http(8080);
    var server = sb.build();
    server.start().join();
  }

  static class HttpKafkaService {
    final ObjectMapper mapper = new ObjectMapper();
    final Producer<byte[], byte[]> producer;
    final String topic;

    HttpKafkaService(Producer<byte[], byte[]> producer, String topic) {
      this.producer = producer;
      this.topic = topic;
    }

    @Post("/send")
    public HttpResponse send(byte[] body) {
      try {
        final var meta = producer.send(new ProducerRecord<>(topic, null, body)).get();
        final var responseBody = mapper.createObjectNode()
            .put("topic", meta.topic())
            .put("partition", meta.partition())
            .put("offset", meta.offset())
            .put("timestamp", meta.timestamp());
        return HttpResponse.of(mapper.writeValueAsString(responseBody));
      } catch (InterruptedException | ExecutionException | JsonProcessingException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
