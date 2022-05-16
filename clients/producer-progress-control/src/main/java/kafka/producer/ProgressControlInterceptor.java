package kafka.producer;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;

public class ProgressControlInterceptor<K, V> implements ProducerInterceptor<K, V> {

  Thread controlThread;
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
        producerConfig.put("client.id", "progress-control-interceptor");
        producerConfig.remove("interceptor.classes");

        final var internalProducer = new KafkaProducer<K, V>(producerConfig);
        final var config = ProgressControlConfig.load(configs);
        this.progressController = new ProgressController<>(internalProducer, config);

        controlThread = new Thread(this.progressController);
        controlThread.start();
      }
    }
  }

  @Override
  public ProducerRecord<K, V> onSend(ProducerRecord<K, V> record) {
    return record;
  }

  @Override
  public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
    if (exception == null) {
      if (!closeCalled) progressController.addTopicPartition(metadata);
    }
  }

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
