package kafka.producer;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import kafka.producer.ProgressController.Config;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;

public class ProgressControlInterceptor<K, V> implements ProducerInterceptor<K, V> {

  ProgressController<K, V> progressController;
  volatile boolean closeCalled;

  @Override
  public void configure(Map<String, ?> configs) {
    if (progressController == null) {
      synchronized (this) {
        final var producerConfig = new Properties(configs.size());
        producerConfig.putAll(configs);
        if (!producerConfig.containsKey("key.serializer"))
          producerConfig.put("key.serializer", ByteArraySerializer.class);
        if (!producerConfig.containsKey("value.serializer"))
          producerConfig.put("value.serializer", ByteArraySerializer.class);
        final var internalProducer = new KafkaProducer<K, V>(producerConfig);
        final var config = Config.newBuilder();
        this.progressController = new ProgressController<>(internalProducer, config.build());
        Thread controlThread = new Thread(this.progressController);
        controlThread.start();
      }
    }
  }

  @Override
  public ProducerRecord<K, V> onSend(ProducerRecord<K, V> record) {
    if (closeCalled) return record;
    progressController.addTopicPartition(record);
    return record;
  }

  @Override
  public void onAcknowledgement(RecordMetadata metadata, Exception exception) {}

  @Override
  public synchronized void close() {
    if (closeCalled) {
      return;
    }
    if (progressController != null) {
      try {
        progressController.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    closeCalled = true;
  }
}
