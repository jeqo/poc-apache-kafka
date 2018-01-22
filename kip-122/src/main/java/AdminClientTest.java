import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeGroupsResult;
import org.apache.kafka.clients.admin.GroupDescription;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListGroupOffsetsResult;
import org.apache.kafka.clients.admin.ListGroupsResult;
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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 *
 */
public class AdminClientTest {

  public static void main(String[] args) throws ExecutionException, InterruptedException {
    final Properties producerConfigs = new Properties();
    producerConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "docker-vm:9092");
    producerConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    producerConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    final Producer<String, String> producer = new KafkaProducer<>(producerConfigs);

    final ProducerRecord<String, String> producerRecord = new ProducerRecord<>("test-kip-222", "test1");
    final RecordMetadata recordMetadata = producer.send(producerRecord).get();
    System.out.println("Record producer=>" + recordMetadata.offset());

    final Properties consumerConfigs = new Properties();
    consumerConfigs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "docker-vm:9092");
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
    adminConfigs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "docker-vm:9092");

    final AdminClient adminClient = KafkaAdminClient.create(adminConfigs);

    final ListGroupsResult consumerGroups = adminClient.listConsumerGroups();
    final Set<String> consumerGroupNames = consumerGroups.names().get();

    for (String name : consumerGroupNames) {
      System.out.println("Consumer group=>" + name);
    }

    final DescribeGroupsResult describedGroups = adminClient.describeGroups(Collections.singletonList("group1"));
    final Map<String, GroupDescription> groupDescriptionMap = describedGroups.all().get();

    for (Map.Entry<String, GroupDescription> groupDescriptionEntry : groupDescriptionMap.entrySet()) {
      System.out.println("Group=>" + groupDescriptionEntry.getKey()
          + ",Description=>" + groupDescriptionEntry.getValue().protocolType());

      for (MemberDescription memberDescription : groupDescriptionEntry.getValue().members()) {
        System.out.println("Member description=>" + memberDescription);
      }
    }

    final ListGroupOffsetsResult groupOffsets = adminClient.listGroupOffsets("group1");
    final Map<TopicPartition, OffsetAndMetadata> groupOffsetsListing = groupOffsets.namesToListings().get();

    for (Map.Entry<TopicPartition, OffsetAndMetadata> groupOffsetListing : groupOffsetsListing.entrySet()) {
      System.out.println("Topic:" + groupOffsetListing.getKey().topic() + ",Partition:" + groupOffsetListing.getKey().partition() + ",Offset:" + groupOffsetListing.getValue());
    }
  }

}
