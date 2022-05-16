package kafka.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * Get metadata to restart clock. Keep clock per topic-partition. Compare clock with control
 * configuration.
 */
public class ProgressControlCallback<K, V> implements Callback {

  final Producer<K, V> producer;
  final ProgressControlConfig config;

  Thread controlThread;
  ProgressController<?, ?> progressController;
  volatile boolean closeCalled;

  public ProgressControlCallback(Producer<K, V> producer, ProgressControlConfig config) {
    this.producer = producer;
    this.config = config;
  }

  @Override
  public void onCompletion(RecordMetadata metadata, Exception exception) {
    if (progressController == null) {
      synchronized (this) {
        this.progressController = new ProgressController<>(producer, config);

        controlThread = new Thread(this.progressController);
        controlThread.start();
      }
    }

    if (exception == null) {
      if (!closeCalled) progressController.addTopicPartition(metadata);
    }
  }
}
