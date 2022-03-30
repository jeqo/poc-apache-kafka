package kafka.context;

import static kafka.context.ContextHelper.passwordHelper;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.security.plain.internals.PlainSaslServer;

public record KafkaContexts(Map<String, KafkaContext> contextMap) {
  static final ObjectMapper json = new ObjectMapper();

  public static void save(KafkaContexts contexts) throws IOException {
    Files.write(kafkaContextConfig(ContextHelper.baseDir()), contexts.serialize());
  }

  static Path kafkaContextConfig(Path home) throws IOException {
    final var context = home.resolve("kafka.json");
    if (!Files.isRegularFile(context)) {
      System.err.println("Kafka Content configuration file doesn't exist, creating one...");
      Files.write(context, KafkaContexts.empty());
    }

    return context;
  }

  static byte[] empty() throws JsonProcessingException {
    return json.writeValueAsBytes(json.createArrayNode());
  }

  static KafkaContexts load(Path baseDir) throws IOException {
    return from(Files.readAllBytes(kafkaContextConfig(baseDir)));
  }

  public static KafkaContexts load() throws IOException {
    return load(ContextHelper.baseDir());
  }

  static KafkaContexts from(byte[] bytes) throws IOException {
    final var tree = json.readTree(bytes);
    if (!tree.isArray()) throw new IllegalArgumentException("JSON is not an array");

    final var array = (ArrayNode) tree;
    final var contexts = new HashMap<String, KafkaContext>(array.size());
    for (final var node : array) {
      final var context = KafkaContext.parse(node);
      contexts.put(context.name(), context);
    }

    return new KafkaContexts(contexts);
  }

  public String names() throws JsonProcessingException {
    return json.writeValueAsString(contextMap.keySet());
  }

  public byte[] serialize() throws JsonProcessingException {
    final var array = json.createArrayNode();
    for (final var ctx : contextMap.values()) array.add(ctx.toJson());
    return json.writeValueAsBytes(array);
  }

  public void add(KafkaContext ctx) {
    contextMap.put(ctx.name, ctx);
  }

  public KafkaContext get(String name) {
    return contextMap.get(name);
  }

  public boolean has(String name) {
    return contextMap.containsKey(name);
  }

  public void remove(String name) {
    contextMap.remove(name);
  }

  public String namesAndBootstrapServers() throws JsonProcessingException {
    final var node = json.createObjectNode();
    contextMap.forEach((k, v) -> node.put(k, v.cluster().bootstrapServers()));
    return json.writeValueAsString(node);
  }

  public record KafkaContext(String name, KafkaCluster cluster) {

    static KafkaContext parse(JsonNode node) {
      final var name = node.get("name").textValue();
      return new KafkaContext(name, KafkaCluster.parse(node.get("cluster")));
    }

    public JsonNode toJson() {
      final var node = json.createObjectNode().put("name", this.name);
      node.set("cluster", cluster.toJson());
      return node;
    }

    public Properties properties() throws IOException {
      final var props = new Properties();
      props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
      switch (cluster.auth().type()) {
        case SASL_PLAIN -> {
          props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SASL_SSL.name);
          props.put(SaslConfigs.SASL_MECHANISM, PlainSaslServer.PLAIN_MECHANISM);
          var auth = (KafkaContexts.UsernamePasswordAuth) cluster.auth();
          props.setProperty(
              SaslConfigs.SASL_JAAS_CONFIG,
              "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";"
                  .formatted(auth.username(), passwordHelper().decrypt(auth.password())));
        }
        default -> {}
      }
      return props;
    }

    public String kcat() {
      return switch (cluster.auth().type()) {
        case SASL_PLAIN -> """
                        kcat -b %s -X ***REMOVED*** -X sasl.mechanisms=PLAIN \\
                         -X sasl.username=$KAFKA_USERNAME -X sasl.password=$KAFKA_PASSWORD \\
                         -X api.version.request=true\040"""
            .formatted(cluster.bootstrapServers);
        default -> "kcat -b %s ".formatted(cluster.bootstrapServers);
      };
    }

    public String env(boolean includeAuth) {
      return switch (cluster.auth().type()) {
        case SASL_PLAIN -> includeAuth
            ? """
            export KAFKA_BOOTSTRAP_SERVERS=%s
            export KAFKA_USERNAME=%s
            export KAFKA_PASSWORD=%s"""
                .formatted(
                    cluster.bootstrapServers,
                    ((UsernamePasswordAuth) cluster.auth()).username,
                    passwordHelper().decrypt(((UsernamePasswordAuth) cluster.auth()).password))
            : "export KAFKA_BOOTSTRAP_SERVERS=%s".formatted(cluster.bootstrapServers);
        default -> "export KAFKA_BOOTSTRAP_SERVERS=%s".formatted(cluster.bootstrapServers);
      };
    }
  }

  public record KafkaCluster(String bootstrapServers, KafkaAuth auth) {
    static KafkaCluster parse(JsonNode cluster) {
      return new KafkaCluster(
          cluster.get("bootstrapServers").textValue(), KafkaAuth.parse(cluster.get("auth")));
    }

    public JsonNode toJson() {
      final var node = json.createObjectNode().put("bootstrapServers", bootstrapServers);
      node.set("auth", auth.toJson());
      return node;
    }
  }

  public interface KafkaAuth {
    AuthType type();

    static KafkaAuth parse(JsonNode auth) {
      final var type = AuthType.valueOf(auth.get("type").textValue());
      return switch (type) {
        case SASL_PLAIN -> new UsernamePasswordAuth(
            type, auth.get("username").textValue(), auth.get("password").textValue());
        default -> new NoAuth();
      };
    }

    default JsonNode toJson() {
      return json.createObjectNode().put("type", type().name());
    }

    enum AuthType {
      PLAINTEXT,
      SASL_PLAIN
    }
  }

  public record NoAuth() implements KafkaAuth {
    @Override
    public AuthType type() {
      return AuthType.PLAINTEXT;
    }
  }

  public record UsernamePasswordAuth(AuthType authType, String username, String password)
      implements KafkaAuth {

    public static UsernamePasswordAuth build(AuthType authType, String username, String password) {
      return new UsernamePasswordAuth(authType, username, passwordHelper().encrypt(password));
    }

    @Override
    public AuthType type() {
      return authType;
    }

    @Override
    public JsonNode toJson() {
      var node = (ObjectNode) KafkaAuth.super.toJson();
      return node.put("username", username).put("password", password);
    }
  }
}