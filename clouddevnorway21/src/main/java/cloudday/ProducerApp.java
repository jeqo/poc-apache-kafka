package cloudday;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerApp {

  public static void main(String[] args)
      throws ExecutionException, InterruptedException, IOException {
    var config = Config.common();
    var keySerializer = new StringSerializer();
    var valueSerializer = new StringSerializer();

    // Kafka Producers need to know the serialization format to store events in a topic.
    var producer = new KafkaProducer<>(config, keySerializer, valueSerializer);

    ProducerRecord<String, String> record = new ProducerRecord<>(
        "hello-world",
        "from:jorge, to:arturo",
        "all good");

    var future = producer.send(record, (metadata, exception) -> {
      // handle callback if (exception != null) ...
    });
    var metadata = future.get(); // get a response back
    System.out.println(metadata);
    producer.flush(); // or flush manually
    producer.close(); // or close producer
  }
}
