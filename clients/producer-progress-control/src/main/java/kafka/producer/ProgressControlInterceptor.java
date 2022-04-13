package kafka.producer;

import java.util.Map;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class ProgressControlInterceptor<K, V> implements ProducerInterceptor<K, V> {

  @Override
  public ProducerRecord<K, V> onSend(ProducerRecord<K, V> record) {
    return null;
  }

  @Override
  public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

  }

  @Override
  public void close() {

  }

  @Override
  public void configure(Map<String, ?> configs) {

  }
}
