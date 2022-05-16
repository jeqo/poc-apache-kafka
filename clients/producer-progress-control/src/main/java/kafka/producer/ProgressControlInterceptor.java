package kafka.producer;

import java.io.IOException;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class ProgressControlInterceptor<K, V> implements ProducerInterceptor<K, V> {

  Thread controlThread;
  ProgressController<K, V> progressController;
  volatile boolean closeCalled;

  @Override
  public void configure(Map<String, ?> configs) {
    if (progressController == null) {
      synchronized (this) {
        this.progressController = ProgressController.create(configs);

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
