package kafka.producer;

import kafka.producer.ProgressController.Config;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertFalse;

class ProgressControllerTest {

  @Test
  void test() {
    final var config = Config.newBuilder().withStart(Duration.ofSeconds(1)).build();

    assertFalse(config.shouldSendControl(100, 0));
  }
}
