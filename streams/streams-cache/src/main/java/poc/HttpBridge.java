package poc;

import io.javalin.Javalin;
import io.javalin.http.Context;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.extension.annotations.WithSpan;
import java.io.FileInputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import poc.jsonschema2json.Customer;
import poc.jsonschema2json.Product;

public class HttpBridge {

  static String topic = "all-events";

  final Producer<String, Object> producer;

  public HttpBridge(Producer<String, Object> producer) {
    this.producer = producer;
  }

  public static void main(String[] args) throws IOException {
    var producerConfig = new Properties();
    try (final var inputStream = new FileInputStream("src/main/resources/producer.properties")) {
      producerConfig.load(inputStream);
    }
    //    final var srClient = new CachedSchemaRegistryClient(
    //        producerConfig.getProperty("schema.registry.url"),
    //        10_000);
    //    final var valueSerializer = new KafkaJsonSchemaSerializer<>(srClient);
    //    final var config = new LinkedHashMap<String, Object>();
    //    producerConfig.forEach((o, o2) -> config.put(o.toString(), o2));
    //
    //    valueSerializer.configure(config, false);
    //    var producer = new KafkaProducer<>(producerConfig
    //        , new StringSerializer(), valueSerializer
    //    );
    var producer = new KafkaProducer<String, Object>(producerConfig);

    var httpBridge = new HttpBridge(producer);

    var app = Javalin.create().get("/test-1/:id", httpBridge::test1).get("/test-2/:id", httpBridge::test2);
    Runtime
      .getRuntime()
      .addShutdownHook(
        new Thread(() -> {
          app.stop();
          producer.close();
        })
      );
    app.start(8080);
  }

  // Custom Tracing Span
  @WithSpan
  public void test1(Context ctx) {
    try {
      // Adding a custom attribute to the span
      Span span = Span.current();
      final var s = Instant.now().toString();
      span.setAttribute("custom-id", s);
      // End adding custom attr.

      final var id = ctx.pathParam("id");

      final var event = new Customer();
      event.setCustomerId("123");

      final var record = new ProducerRecord<>(topic, id, (Object) event);
      producer.send(record).get();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  // Custom Tracing Span
  @WithSpan
  public void test2(Context ctx) {
    try {
      // Adding a custom attribute to the span
      Span span = Span.current();
      final var s = Instant.now().toString();
      span.setAttribute("custom-id", s);
      // End adding custom attr.

      final var id = ctx.pathParam("id");

      final var event = new Product();
      event.setProductId("123");

      final var record = new ProducerRecord<>(topic, id, (Object) event);
      producer.send(record).get();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
