package kafka.cli.producer.datagen;

import java.io.IOException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class IntervalRun {

  final Config config;
  final KafkaProducer<String, Object> producer;
  final PayloadGenerator payloadGenerator;
  final Stats stats;

  public IntervalRun(
      Config config,
      KafkaProducer<String, Object> producer,
      PayloadGenerator payloadGenerator, Stats stats) {
    this.config = config;
    this.producer = producer;
    this.payloadGenerator = payloadGenerator;
    this.stats = stats;
  }

  byte[] sample;

  public void start() throws IOException {
    sample = payloadGenerator.sample();
    loop(0);
  }

  void loop(int count) {
    if (config.maxInterval() > 0) {
      try {
        Thread.sleep((long) (config.maxInterval() * Math.random()));
      } catch (InterruptedException e) {
        Thread.interrupted();
      }
    }

    if (config.maxRecords() > 0 && count >= config.maxRecords()) {
      stats.printTotal();

      producer.close();
      return;
    }

    runOnce();

    count++;
    loop(count);
  }

  void runOnce() {
    var payload = payloadGenerator.get();
    var key = payloadGenerator.key(payload);

    var sendStartMs = System.currentTimeMillis();
    var cb = stats.nextCompletion(sendStartMs, sample.length, stats);
    var record = new ProducerRecord<String, Object>(config.topicName(), key, payload);

    producer.send(record, cb);

    long seed = payloadGenerator.random.nextLong();
    payloadGenerator.random.setSeed(seed);
  }

  public record Config(
      String topicName,
      long maxRecords,
      long maxInterval
  ) {
  }
}
