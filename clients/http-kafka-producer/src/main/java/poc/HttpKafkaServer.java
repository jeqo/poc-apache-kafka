package poc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linecorp.armeria.common.HttpHeaders;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.common.metric.MeterIdPrefixFunction;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.ServiceRequestContext;
import com.linecorp.armeria.server.annotation.Description;
import com.linecorp.armeria.server.annotation.Post;
import com.linecorp.armeria.server.docs.DocService;
import com.linecorp.armeria.server.metric.MetricCollectingService;
import io.github.mweirauch.micrometer.jvm.extras.ProcessMemoryMetrics;
import io.github.mweirauch.micrometer.jvm.extras.ProcessThreadMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmInfoMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.kafka.KafkaClientMetrics;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;

public class HttpKafkaServer {

  public static void main(String[] args) throws IOException {
    // load config
    final var config = new Properties();
    config.load(Files.newInputStream(Path.of("kafka.properties")));
    // create producer
    final var producer = new KafkaProducer<>(config, new ByteArraySerializer(), new ByteArraySerializer());
    // prepare metrics registry
    final var prometheusRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    prometheusRegistry.config().commonTags("application", "kafka-http-producer");
    new KafkaClientMetrics(producer).bindTo(prometheusRegistry);
    new JvmInfoMetrics().bindTo(prometheusRegistry);
    new JvmThreadMetrics().bindTo(prometheusRegistry);
    new JvmMemoryMetrics().bindTo(prometheusRegistry);
    new JvmGcMetrics().bindTo(prometheusRegistry);
    new ProcessMemoryMetrics().bindTo(prometheusRegistry);
    new ProcessThreadMetrics().bindTo(prometheusRegistry);
    // create service
    final var service = new HttpKafkaService(producer, config.getProperty("topic"));
    // prepare and start server
    Server
      .builder()
      .annotatedService(
        service,
        MetricCollectingService.builder(MeterIdPrefixFunction.ofDefault("kafkahttp")).newDecorator()
      )
      .service("/metrics", (ctx, req) -> HttpResponse.of(MediaType.PLAIN_TEXT_UTF_8, prometheusRegistry.scrape()))
      .serviceUnder("/", DocService.builder().build())
      .http(8080)
      .meterRegistry(prometheusRegistry)
      .blockingTaskExecutor(800)
      .build()
      .start()
      .join();
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
    public HttpResponse send(@Description("Payload") byte[] body, HttpHeaders httpHeaders, ServiceRequestContext ctx) {
      try {
        final var key = httpHeaders.get("key");
        final byte[] keyBytes = key != null ? key.getBytes(StandardCharsets.UTF_8) : null;
        final var record = new ProducerRecord<>(topic, keyBytes, body);
        final var f = new CompletableFuture<HttpResponse>();
        // to use proper executor service for blocking operations as `get()` is waiting for ack.
        // more here: https://armeria.dev/docs/server-annotated-service/#specifying-a-blocking-task-executor
        ctx
          .blockingTaskExecutor()
          .execute(() ->
            producer.send(
              record,
              (meta, e) -> {
                if (e != null) {
                  f.complete(HttpResponse.ofFailure(e));
                } else {
                  final var responseBody = mapper
                    .createObjectNode()
                    .put("topic", meta.topic())
                    .put("partition", meta.partition())
                    .put("offset", meta.offset())
                    .put("timestamp", meta.timestamp());
                  f.complete(HttpResponse.ofJson(HttpStatus.CREATED, responseBody));
                }
              }
            )
          );
        return HttpResponse.from(f);
      } catch (Exception e) {
        return HttpResponse.ofFailure(e);
      }
    }
  }
}
