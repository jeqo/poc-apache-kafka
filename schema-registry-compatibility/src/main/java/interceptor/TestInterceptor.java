package interceptor;

import java.util.Map;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class TestInterceptor<K, V> implements ProducerInterceptor<K, V> {

  CloudEventHeaders cloudEventHeaders;

  @Override
  public void configure(Map<String, ?> configs) {

  }

  @Override
  public ProducerRecord<K, V> onSend(ProducerRecord<K, V> record) {
    if (cloudEventHeaders.validate(record.headers()))
      return record;
    else {
      System.out.println("ERROR!!");
      return null;
    }
  }

  @Override
  public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

  }

  @Override
  public void close() {

  }
}
