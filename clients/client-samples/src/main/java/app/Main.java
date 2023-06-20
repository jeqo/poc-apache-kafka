package app;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class Main {
    public static void main(String[] args) {
        try (final var admin = AdminClient.create(Map.of("bootstrap.servers", "localhost:9092"))) {
            final var t1 = new NewTopic("should-not-be-here", 1, (short) 1);
            final var opts = new CreateTopicsOptions().validateOnly(true);
            admin.createTopics(Set.of(t1), opts).all().get();
        } catch (ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
