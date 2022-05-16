package kafka.producer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.time.Duration;
import java.util.Properties;
import kafka.producer.ProgressController.Control;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

class ProgressControllerTest {
  final Producer<String, String> producer;

  ProgressControllerTest() {
    final var properties = new Properties();
    properties.put("bootstrap.servers", "localhost:19092");
    producer = new KafkaProducer<>(properties, new StringSerializer(), new StringSerializer());
  }

  @Test
  void shouldEvalFalse_whenDiffIsLessThanStartPeriod() {
    final var config = ProgressControlConfig.newBuilder().withStart(Duration.ofSeconds(1)).build();
    try (final var controller = new ProgressController<>(producer, config)) {
      final var tp = new TopicPartition("t1", 0);
      final var eval = controller.eval(tp, Control.create(0), 10);
      assertFalse(eval);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  void shouldEvalTrue_whenDiffIsHigherThanStartPeriod() {
    final var config = ProgressControlConfig.newBuilder().withStart(Duration.ofSeconds(1)).build();
    try (final var controller = new ProgressController<>(producer, config)) {
      final var tp = new TopicPartition("t1", 0);
      controller.addTopicPartition(tp, 0);
      final var current = Duration.ofSeconds(2).toMillis();
      final var eval = controller.eval(tp, Control.create(0), current);
      assertTrue(eval);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  void shouldRemovePartition_whenEvalTrueAndNoBackoff() {
    final var config = ProgressControlConfig.newBuilder().withStart(Duration.ofSeconds(1)).build();
    try (final var controller = new ProgressController<>(producer, config)) {
      final var tp = new TopicPartition("t1", 0);
      controller.addTopicPartition(tp, 0);
      final var current = Duration.ofSeconds(2).toMillis();
      final var eval = controller.eval(tp, Control.create(0), current);
      assertTrue(eval);
      assertTrue(controller.progress.isEmpty());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  void shouldIncreaseIteration_whenEvalTrueAndNoBackoff() {
    final var config =
        ProgressControlConfig.newBuilder()
            .withStart(Duration.ofSeconds(1))
            .withEnd(Duration.ofSeconds(10), Duration.ofSeconds(1), false)
            .build();
    try (final var controller = new ProgressController<>(producer, config)) {
      final var tp = new TopicPartition("t1", 0);
      controller.addTopicPartition(tp, 0);
      final var current = Duration.ofSeconds(2).toMillis();
      final var eval = controller.eval(tp, Control.create(0), current);
      assertTrue(eval);
      assertFalse(controller.progress.isEmpty());
      assertEquals(1, controller.progress.get(tp).iteration());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  void shouldEvalFalse_when() {
    final var config =
        ProgressControlConfig.newBuilder()
            .withStart(Duration.ofSeconds(1))
            .withEnd(Duration.ofSeconds(10), Duration.ofSeconds(2), false)
            .build();
    try (final var controller = new ProgressController<>(producer, config)) {
      final var tp = new TopicPartition("t1", 0);
      controller.addTopicPartition(tp, 0);
      final var current = Duration.ofSeconds(1).toMillis();
      final var eval = controller.eval(tp, new Control(0, current, 1), current);
      assertFalse(eval);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
