package kafka.datagen;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.Properties;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ProducerPerformance {

  final Config config;
  final KafkaProducer<String, GenericRecord> producer;
  final PayloadGenerator payloadGenerator;
  final ThroughputThrottler throttler;
  final Stats stats;

  public ProducerPerformance(
      final Config config,
      final KafkaProducer<String, GenericRecord> producer,
      final PayloadGenerator payloadGenerator,
      final ThroughputThrottler throughputThrottler,
      final Stats stats
  ) {
    this.config = config;
    this.producer = producer;
    this.payloadGenerator = payloadGenerator;
    this.throttler = throughputThrottler;
    this.stats = stats;
  }

  void start() throws IOException {

    GenericRecord payload;
    String key;
    ProducerRecord<String, GenericRecord> record;

    int currentTransactionSize = 0;
    long transactionStartTime = 0;

    var sample = payloadGenerator.sample();

    for (long i = 0; i < config.records(); i++) {
      payload = payloadGenerator.get();
      key = payloadGenerator.key(payload);

      if (config.transactionsEnabled() && currentTransactionSize == 0) {
        producer.beginTransaction();
        transactionStartTime = System.currentTimeMillis();
      }

      record = new ProducerRecord<>(config.topicName(), key, payload);

      long sendStartMs = System.currentTimeMillis();
      Callback cb = stats.nextCompletion(sendStartMs, sample.length, stats);
      producer.send(record, cb);

      currentTransactionSize++;
      if (config.transactionsEnabled() && config.transactionDurationMs() <= (sendStartMs - transactionStartTime)) {
        producer.commitTransaction();
        currentTransactionSize = 0;
      }

       if (throttler.shouldThrottle(i, sendStartMs)) {
         throttler.throttle();
       }
    }

    if (config.transactionsEnabled() && currentTransactionSize != 0)
      producer.commitTransaction();

    if (!config.shouldPrintMetrics()) {
      producer.close();

      /* print final results */
      stats.printTotal();
    } else {
      // Make sure all messages are sent before printing out the stats and the metrics
      // We need to do this in a different branch for now since tests/kafkatest/sanity_checks/test_performance_services.py
      // expects this class to work with older versions of the client jar that don't support flush().
      producer.flush();

      /* print final results */
      stats.printTotal();

      /* print out metrics */
      ToolsUtils.printMetrics(producer.metrics());
      producer.close();
    }
  }

  record Config(
      long records,
      String topicName,
      boolean transactionsEnabled,
      long transactionDurationMs,
      boolean shouldPrintMetrics
  ) {

  }

  public static void main(String[] args) throws IOException {
    var producerConfig = new Properties();
    producerConfig.load(Files.newInputStream(Path.of("client.properties")));
    var producer = new KafkaProducer<String, GenericRecord>(producerConfig);
    final var records = 1_000_000;
    final var targetThroughput = 10_000;
    var pp = new ProducerPerformance(
        new Config(records, "jeqo-test-v1", false, 100, false),
        producer,
        new PayloadGenerator(new PayloadGenerator.Config(
            Optional.empty(),
            Optional.of(Quickstart.CLICKSTREAM),
            Optional.empty(),
            Optional.empty(),
            records)),
        new ThroughputThrottler(System.currentTimeMillis(), targetThroughput),
        new Stats(records, 5000)
    );
    pp.start();
  }
}
