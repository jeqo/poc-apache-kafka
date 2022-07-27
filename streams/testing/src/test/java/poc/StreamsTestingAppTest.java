package poc;

import static poc.StreamsTestingApp.buildSimpleFromTo;
import static poc.StreamsTestingApp.loadConfig;

import java.io.IOException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.TopologyTestDriver;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Unit test for simple App.
 */
class StreamsTestingAppTest {

  @Test
  void testApp() throws IOException {
    try (final var driver = new TopologyTestDriver(buildSimpleFromTo(), loadConfig())) {
      // Given
      // - with topics
      var inputTopic = driver.createInputTopic("input", new StringSerializer(), new StringSerializer());
      var outputTopic = driver.createOutputTopic("output", new StringDeserializer(), new StringDeserializer());

      // When
      inputTopic.pipeInput("k1", "v1");
      var kv1 = outputTopic.readKeyValue();

      // Then
      Assertions.assertThat(kv1).extracting("key", "value").contains("k1", "v1");
    }
  }
}
