package io.github.jeqo.poc;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Hello world!
 */
public class App {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        final Properties producerConfigs = new Properties();
        producerConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        final Producer<String, String> producer = new KafkaProducer<>(producerConfigs);

        final ProducerRecord<String, String> producerRecord = new ProducerRecord<>("test-kip-244-in", "test1");
        producerRecord.headers().add("test-header", "value1".getBytes());
        final RecordMetadata recordMetadata = producer.send(producerRecord).get();
        System.out.println("Record producer=>" + recordMetadata.offset());

        Thread.sleep(2000);

        final Properties streamsConfigs = new Properties();
        streamsConfigs.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-kip-244");
        streamsConfigs.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        streamsConfigs.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        streamsConfigs.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);

        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream("test-kip-244-in").to("test-kip-244");
        final Topology topology = builder.build();
        final KafkaStreams kafkaStreams = new KafkaStreams(topology, streamsConfigs);
        kafkaStreams.start();

        Thread.sleep(2000);

        final Properties consumerConfigs = new Properties();
        consumerConfigs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");
        consumerConfigs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfigs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerConfigs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        final Consumer<String, String> consumer = new KafkaConsumer<>(consumerConfigs);
        consumer.subscribe(Collections.singletonList("test-kip-244"));

        final ConsumerRecords<String, String> consumerRecords = consumer.poll(10000L);

        for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
            System.out.println("Record consumer=>" + consumerRecord.offset() + ":" + consumerRecord.value());
            System.out.println("Record header=>" + new String(consumerRecord.headers().lastHeader("test-header").value()));
        }

        consumer.commitSync();

    }
}
