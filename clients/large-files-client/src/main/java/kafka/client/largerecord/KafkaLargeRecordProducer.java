package kafka.client.largerecord;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;

public class KafkaLargeRecordProducer<K, V> implements Producer<K, V> {

  private final Producer<byte[], Bytes> producer;
  private final Serializer<K> keySerializer;
  private final Serializer<V> valueSerializer;
  private final int batchSize;

  public KafkaLargeRecordProducer(
    Producer<byte[], Bytes> producer,
    Serializer<K> keySerializer,
    Serializer<V> valueSerializer,
    int batchSize
  ) {
    this.producer = producer;
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
    this.batchSize = batchSize;
  }

  @Override
  public void initTransactions() {
    producer.initTransactions();
  }

  @Override
  public void beginTransaction() throws ProducerFencedException {
    producer.beginTransaction();
  }

  @Override
  public void commitTransaction() throws ProducerFencedException {
    producer.commitTransaction();
  }

  @Override
  public void abortTransaction() throws ProducerFencedException {
    producer.abortTransaction();
  }

  @Override
  public Future<RecordMetadata> send(ProducerRecord<K, V> record) {
    return send(record, null);
  }

  @Override
  public Future<RecordMetadata> send(ProducerRecord<K, V> record, Callback callback) {
    producer.beginTransaction();
    final byte[] serializedKey = keySerializer.serialize(record.topic(), record.key());
    final byte[] serialized = valueSerializer.serialize(record.topic(), record.value());
    final ByteBuffer buffer = ByteBuffer.wrap(serialized);
    buffer.position(0);
    Future<RecordMetadata> metadataFuture = null;
    while (buffer.hasRemaining()) {
      ProducerRecord<byte[], Bytes> bytesRecord = new ProducerRecord<>(
        record.topic(),
        serializedKey,
        Bytes.wrap(serialized)
      );
      metadataFuture = producer.send(bytesRecord);
    }
    producer.commitTransaction();
    return metadataFuture;
  }

  @Override
  public void flush() {
    producer.flush();
  }

  @Override
  public List<PartitionInfo> partitionsFor(String topic) {
    return producer.partitionsFor(topic);
  }

  @Override
  public Map<MetricName, ? extends Metric> metrics() {
    return producer.metrics();
  }

  @Override
  public void close() {
    producer.close();
  }

  @Override
  public void close(Duration timeout) {
    producer.close(timeout);
  }

  @Override
  public void sendOffsetsToTransaction(
    Map<TopicPartition, OffsetAndMetadata> offsets,
    ConsumerGroupMetadata groupMetadata
  ) throws ProducerFencedException {
    producer.sendOffsetsToTransaction(offsets, groupMetadata);
  }

  @Override
  public void sendOffsetsToTransaction(Map<TopicPartition, OffsetAndMetadata> offsets, String consumerGroupId)
    throws ProducerFencedException {
    producer.sendOffsetsToTransaction(offsets, consumerGroupId);
  }
}
