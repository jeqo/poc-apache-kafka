package kafka.cli.context;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class Helper {
  static void save(KafkaContexts contexts) throws IOException {
    Files.write(kafkaContextConfig(), contexts.serialize());
  }

  static void save(SchemaRegistryContexts contexts) throws IOException {
    Files.write(schemaRegistryContextConfig(), contexts.serialize());
  }

  static Path kafkaContextConfig() throws IOException {
    final Path home = baseDir();

    final var context = home.resolve("kafka.json");
    if (!Files.isRegularFile(context)) {
      System.err.println("Kafka Content configuration file doesn't exist, creating one...");
      Files.write(context, KafkaContexts.empty());
    }

    return context;
  }

  static Path schemaRegistryContextConfig() throws IOException {
    final Path home = baseDir();

    final var context = home.resolve("schema-registry.json");
    if (!Files.isRegularFile(context)) {
      System.err.println(
          "Schema Registry Content configuration file doesn't exist, creating one...");
      Files.write(context, KafkaContexts.empty());
    }

    return context;
  }

  static Path baseDir() throws IOException {
    final var homePath = System.getProperty("user.home");
    if (homePath.isBlank()) {
      throw new IllegalStateException("Can't find user's home. ${HOME} is empty");
    }

    final var home = Path.of(homePath, ".kafka");
    if (!Files.isDirectory(home)) {
      System.err.println("Kafka Context directory doesn't exist, creating one...");
      Files.createDirectories(home);
    }
    return home;
  }

  static PasswordHelper passwordHelper() {
    try {
      final var saltPath = baseDir().resolve(".salt");
      if (!Files.exists(saltPath)) {
        final var salt = PasswordHelper.generateKey();
        Files.writeString(saltPath, salt);
        return new PasswordHelper(salt);
      } else {
        final var salt = Files.readString(saltPath);
        return new PasswordHelper(salt);
      }
    } catch (IOException e) {
      throw new IllegalStateException("Password helper not loading", e);
    }
  }
}
