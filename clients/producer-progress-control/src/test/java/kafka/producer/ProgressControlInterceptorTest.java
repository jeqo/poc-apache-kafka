package kafka.producer;

import static org.junit.jupiter.api.Assertions.*;

import java.util.HashMap;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

class ProgressControlInterceptorTest {

  @Test
  void testAll() {
    final var interceptor = new ProgressControlInterceptor<>();
    final var configs = new HashMap<String, String>();
    configs.put("bootstrap.servers", "localhost:19092");
    interceptor.configure(configs);
    final var record = new ProducerRecord<Object, Object>("t1", 0, "k1", "v1");
    interceptor.onSend(record);
    interceptor.close();
  }
}
