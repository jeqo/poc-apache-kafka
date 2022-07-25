package poc.stateful;

import java.time.Duration;
import java.time.Instant;
import org.apache.kafka.streams.TopologyTestDriver;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import poc.data.Transaction;

class StatefulSessionWindowWithSuppressTest {
  @Test
  void testTopology() {
    var inputTopicName = "input";
    var outputTopicName = "output";

    var app = new StatefulSessionWindowWithSuppress(inputTopicName, outputTopicName);

    try (final var driver = new TopologyTestDriver(app.topology(), Instant.ofEpochMilli(0))) {
      // Given
      // - with topics
      var inputTopic = driver.createInputTopic(
          "input",
          app.keySerde.serializer(),
          app.valueSerde.serializer()
      );
      var outputTopic = driver.createOutputTopic(
          "output",
          app.keySerde.deserializer(),
          app.outputValueSerde.deserializer()
      );

      // When
      inputTopic.pipeInput("k1", new Transaction(1111, 101, "u1", 201, 30), 0);

      // ... wait for next messages
      driver.advanceWallClockTime(Duration.ofSeconds(30));
//      inputTopic.pipeInput("k1", new Transaction(1111, 101, "u1", 201, 30), 30_001);
      inputTopic.pipeInput("", null, 31_000);

      var kv1 = outputTopic.readKeyValue();

      // Then
      Assertions.assertThat(kv1.value)
          .isEqualTo(1L);
      Assertions.assertThat(kv1.key)
          .startsWith("u1@<").endsWith(">");
    }
  }
}