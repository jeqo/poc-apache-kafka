package poc;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ProducerWithGeneric {

  public static void main(String[] args) throws ExecutionException, InterruptedException {
    final var configs = new Properties();
    configs.put("bootstrap.servers", "localhost:9092");
    configs.put("key.serializer", "io.confluent.kafka.streams.serdes.avro.GenericAvroSerializer");
    configs.put("value.serializer", "io.confluent.kafka.streams.serdes.avro.GenericAvroSerializer");
    configs.put("schema.registry.url", "http://localhost:8081");

    final var producer = new KafkaProducer<GenericRecord, GenericRecord>(configs);

    final var keySchema = SchemaBuilder
      .record("Key")
      .namespace("poc.avro")
      .fields()
      .name("val1")
      .type()
      .stringType()
      .noDefault()
      .endRecord();
    final var key = new GenericData.Record(keySchema);
    key.put("val1", "1");
    final var valueSchema = SchemaBuilder
      .record("Value")
      .namespace("poc.avro")
      .fields()
      .name("val1")
      .type()
      .stringType()
      .noDefault()
      .endRecord();
    final var value = new GenericData.Record(valueSchema);
    value.put("val1", "1");

    final var record = new ProducerRecord<GenericRecord, GenericRecord>("t1", key, value);
    producer.send(record).get();
  }
}
