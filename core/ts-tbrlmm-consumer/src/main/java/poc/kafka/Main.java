package poc.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.server.log.remote.metadata.storage.TopicBasedRemoteLogMetadataManagerConfig;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Properties;
import java.util.Set;

public class Main {
    public static void main(String[] args) throws IOException {
        final var props = new Properties();
        props.load(Files.newBufferedReader(Path.of("client.properties")));
        props.put("group.id", "test__rlmm");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetResetStrategy.EARLIEST.name().toLowerCase());

        final var keyDeserializer = new ByteArrayDeserializer();
        final var valueDeserializer = new RemoteLogMetadataDeserializer();

        try (final var c = new KafkaConsumer<>(props, keyDeserializer, valueDeserializer);
             final var db = new DatabaseWriter("rlmm")) {
            c.subscribe(Set.of(TopicBasedRemoteLogMetadataManagerConfig.REMOTE_LOG_METADATA_TOPIC_NAME));
            while (true) {
                final var rs = c.poll(Duration.ofSeconds(10));

                var completed = true;

                for (final var tp : rs.partitions()) {
                    if (rs.records(tp).isEmpty()) completed = false;
                    else {
                        db.addRecords(rs.records(tp));

                        for (final var r : rs.records(tp)) {
                            final var key = r.key() == null ? "" : new String(r.key()) + " : ";
                            System.out.println(tp + "[" + r.offset() + "] => " + key + r.value());
                        }
                    }
                }

                if (completed) return;
            }
        } catch (IOException | SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
