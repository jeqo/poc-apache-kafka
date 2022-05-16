package kafka.producer;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// sleep when no progress?
// restart when new partitions are added?
public class ProgressController<K, V> implements Runnable, Closeable {

  static final Logger LOG = LoggerFactory.getLogger(ProgressController.class);

  final Producer<K, V> producer;
  final ProgressControlConfig config;
  final Map<TopicPartition, Control> progress;

  volatile boolean running;

  public ProgressController(Producer<K, V> producer, ProgressControlConfig config) {
    this.producer = producer;
    this.config = config;
    this.progress = new ConcurrentHashMap<>();
  }

  public static <K, V> ProgressController<K,V> create(Map<String,?> configs) {
    final var producerConfig = new Properties(configs.size());
    producerConfig.putAll(configs);
    if (!producerConfig.containsKey("key.serializer"))
      producerConfig.put("key.serializer", ByteArraySerializer.class);
    if (!producerConfig.containsKey("value.serializer"))
      producerConfig.put("value.serializer", ByteArraySerializer.class);
    producerConfig.put("client.id", "progress-control-interceptor");
    producerConfig.remove("interceptor.classes");

    final var internalProducer = new KafkaProducer<K, V>(producerConfig);
    final var config = ProgressControlConfig.load(configs);
    return new ProgressController<>(internalProducer, config);
  }

  @Override
  public void run() {
    if (running) {
      return;
    }
    this.running = true;
    control();
  }

  void control() {
    while (running) {
      progress.forEach(
          (tp, control) -> {
            long current = System.currentTimeMillis();
            if (eval(tp, control, current)) {
              sendControl(tp, current);
            }
          });
    }
  }

  boolean eval(TopicPartition tp, Control control, long current) {
    long diff = current - control.started();
    var iteration = control.iteration();
    if (diff > threshold(iteration)) {
      if (config.onlyOnce() || diff >= config.end()) {
        progress.remove(tp);
      } else {
        progress.put(tp, control.increment(current));
      }
      return true;
    }
    return false;
  }

  private double threshold(long iteration) {
    if (iteration == 0) {
      return config.start();
    }
    return config.backoffExponential()
            ? config.start() + (Math.pow(2, iteration) * config.backoff())
            : config.start() + config.backoff();
  }

  public void addTopicPartition(String topic, int partition, long timestamp) {
    final var tp = new TopicPartition(topic, partition);
    addTopicPartition(tp, timestamp);
  }

  public void addTopicPartition(TopicPartition tp, long ts) {
    this.progress.put(tp, Control.create(ts));
  }

  void sendControl(TopicPartition tp, long current) {
    var record = new ProducerRecord<K, V>(tp.topic(), tp.partition(), null, null);
    record.headers().add("control", String.valueOf(current).getBytes(StandardCharsets.UTF_8));
    producer.send(record);
  }

  @Override
  public synchronized void close() throws IOException {
    this.running = false;
    this.producer.close();
  }

  public void addTopicPartition(final RecordMetadata metadata) {
    addTopicPartition(metadata.topic(), metadata.partition(), metadata.timestamp());
  }

  record Control(long started, long latest, long iteration) {

    public static Control create(long timestamp) {
      return new Control(timestamp, timestamp, 0);
    }

    public Control increment(long timestamp) {
      return new Control(started, timestamp, this.iteration + 1);
    }
  }
}
