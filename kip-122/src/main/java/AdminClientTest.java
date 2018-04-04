import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ConsumerGroupListing;
import org.apache.kafka.clients.admin.DeleteConsumerGroupsResult;
import org.apache.kafka.clients.admin.DescribeConsumerGroupsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 *
 */
public class AdminClientTest {

  public static void main(String[] args) throws ExecutionException, InterruptedException {
    final Properties producerConfigs = new Properties();
    producerConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    producerConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    producerConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    final Producer<String, String> producer = new KafkaProducer<>(producerConfigs);

    final ProducerRecord<String, String> producerRecord = new ProducerRecord<>("test-kip-222", "test1");
    final RecordMetadata recordMetadata = producer.send(producerRecord).get();
    System.out.println("Record producer=>" + recordMetadata.offset());

    final Properties consumerConfigs = new Properties();
    consumerConfigs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");
    consumerConfigs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerConfigs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    consumerConfigs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

    final Consumer<String, String> consumer = new KafkaConsumer<>(consumerConfigs);
    consumer.subscribe(Collections.singletonList("test-kip-222"));

    final ConsumerRecords<String, String> consumerRecords = consumer.poll(10000L);

    for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
      System.out.println("Record consumer=>" + consumerRecord.offset() + ":" + consumerRecord.value());
    }

    consumer.commitSync();

    final Properties adminConfigs = new Properties();
    adminConfigs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

    final AdminClient adminClient = KafkaAdminClient.create(adminConfigs);

    final ListConsumerGroupsResult consumerGroups = adminClient.listConsumerGroups();
    Collection<KafkaFuture<Collection<ConsumerGroupListing>>> consumerGroupNames = consumerGroups.listings();

    for (KafkaFuture<Collection<ConsumerGroupListing>> name : consumerGroupNames) {
      for (ConsumerGroupListing cs : name.get()) {
        System.out.println("Consumer group=>" + cs);
      }
    }

    final DescribeConsumerGroupsResult describedGroups = adminClient.describeConsumerGroups(Collections.singletonList("group1"));
    final Map<String, KafkaFuture<ConsumerGroupDescription>> groupDescriptionMap = describedGroups.values();

    for (Map.Entry<String, KafkaFuture<ConsumerGroupDescription>> groupDescriptionEntry : groupDescriptionMap.entrySet()) {
      final ConsumerGroupDescription consumerGroupDescription = groupDescriptionEntry.getValue().get();
      System.out.println("Group=>" + groupDescriptionEntry.getKey()
          + ",Description=>" + consumerGroupDescription.partitionAssignor());

      for (MemberDescription memberDescription : consumerGroupDescription.members()) {
        System.out.println("Member description=>" + memberDescription);
      }
    }

    final ListConsumerGroupOffsetsResult groupOffsets = adminClient.listConsumerGroupOffsets("group1");
    final Map<TopicPartition, OffsetAndMetadata> groupOffsetsListing = groupOffsets.partitionsToOffsetAndMetadata().get();

    for (Map.Entry<TopicPartition, OffsetAndMetadata> groupOffsetListing : groupOffsetsListing.entrySet()) {
      System.out.println("Topic:" + groupOffsetListing.getKey().topic() + ",Partition:" + groupOffsetListing.getKey().partition() + ",Offset:" + groupOffsetListing.getValue());
    }

    DeleteConsumerGroupsResult rr = adminClient.deleteConsumerGroups(Collections.singletonList("group1"));
    for (Map.Entry<String, KafkaFuture<Void>> r : rr.values().entrySet()) {
      r.getValue().get();
      System.out.println("CG removed: " + r.getKey());
    }

    for(KafkaFuture<Collection<ConsumerGroupListing>> c : adminClient.listConsumerGroups().listings()) {
      System.out.println(c.get().size() == 0);
    }
  }

}
