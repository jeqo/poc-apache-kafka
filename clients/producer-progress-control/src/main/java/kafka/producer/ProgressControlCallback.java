package kafka.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * Get metadata to restart clock. Keep clock per topic-partition. Compare clock with control
 * configuration.
 */
public class ProgressControlCallback implements Callback {

  @Override
  public void onCompletion(RecordMetadata metadata, Exception exception) {}
}
