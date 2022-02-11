package kafka.datagen;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ProducerPerformance {
  final Config config;
  final KafkaProducer<byte[], byte[]> producer;
  final PayloadGenerator payloadGenerator;
  final ThroughputThrottler throttler;
  final Stats stats;

  public ProducerPerformance(Config config,
      KafkaProducer<byte[], byte[]> producer,
      PayloadGenerator payloadGenerator,
      ThroughputThrottler throughputThrottler,
      Stats stats) {
    this.config = config;
    this.producer = producer;
    this.payloadGenerator = payloadGenerator;
    this.throttler = throughputThrottler;
    this.stats = stats;
  }

  void start() {

    byte[] payload;
    ProducerRecord<byte[], byte[]> record;

    int currentTransactionSize = 0;
    long transactionStartTime = 0;
    for (long i = 0; i < config.records(); i++) {
      payload = payloadGenerator.get();

      if (config.transactionsEnabled() && currentTransactionSize == 0) {
        producer.beginTransaction();
        transactionStartTime = System.currentTimeMillis();
      }

      record = new ProducerRecord<>(config.topicName(), payload);

      long sendStartMs = System.currentTimeMillis();
      Callback cb = stats.nextCompletion(sendStartMs, payload.length, stats);
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
      boolean shouldPrintMetrics) {

  }
}
