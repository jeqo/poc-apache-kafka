package kafka.producer;

import java.io.IOException;
import kafka.producer.ProgressController.Config;
import kafka.producer.ProgressController.Control;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProgressControllerTest {

  @Test
  void test() {
    final var config = Config.newBuilder().withStart(Duration.ofSeconds(1)).build();
    try(final var controller = new ProgressController(config)) {
      final var tp = new TopicPartition("t1", 0);
      final var eval = controller.eval(tp, Control.create(0), 10);
      assertFalse(eval);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  void test1() {
    final var config = Config.newBuilder().withStart(Duration.ofSeconds(1)).build();
    try(final var controller = new ProgressController(config)) {
      final var tp = new TopicPartition("t1", 0);
      controller.addTopicPartition(tp, 0);
      final var eval = controller.eval(tp, Control.create(0), Duration.ofSeconds(2).toMillis());
      assertTrue(eval);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
