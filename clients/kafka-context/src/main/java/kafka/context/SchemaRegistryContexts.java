package kafka.context;

import static kafka.context.ContextHelper.passwordHelper;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.net.PasswordAuthentication;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public record SchemaRegistryContexts(Map<String, SchemaRegistryContext> contextMap) {

  static final ObjectMapper json = new ObjectMapper();

  public static SchemaRegistryContexts load(Path baseDir) throws IOException {
    return from(Files.readAllBytes(schemaRegistryContextConfig(baseDir)));
  }

  public static SchemaRegistryContexts load() throws IOException {
    return load(ContextHelper.baseDir());
  }

  public static void save(SchemaRegistryContexts contexts) throws IOException {
    Files.write(schemaRegistryContextConfig(ContextHelper.baseDir()), contexts.serialize());
  }

  static Path schemaRegistryContextConfig(Path home) throws IOException {
    final var context = home.resolve("schema-registry.json");
    if (!Files.isRegularFile(context)) {
      System.err.println(
          "Schema Registry Content configuration file doesn't exist, creating one...");
      Files.write(context, KafkaContexts.empty());
    }

    return context;
  }

  static SchemaRegistryContexts from(byte[] bytes) throws IOException {
    final var tree = json.readTree(bytes);
    if (!tree.isArray()) {
      throw new IllegalArgumentException("JSON is not an array");
    }

    final var array = (ArrayNode) tree;
    final var contexts = new HashMap<String, SchemaRegistryContext>(array.size());
    for (final var node : array) {
      final var context = SchemaRegistryContext.parse(node);
      contexts.put(context.name(), context);
    }

    return new SchemaRegistryContexts(contexts);
  }

  public String names() throws JsonProcessingException {
    return json.writeValueAsString(contextMap.keySet());
  }

  public byte[] serialize() throws JsonProcessingException {
    final var array = json.createArrayNode();
    for (final var ctx : contextMap.values()) array.add(ctx.toJson());
    return json.writeValueAsBytes(array);
  }

  public void add(SchemaRegistryContext ctx) {
    contextMap.put(ctx.name, ctx);
  }

  public SchemaRegistryContext get(String name) {
    return contextMap.get(name);
  }

  public boolean has(String contextName) {
    return contextMap.containsKey(contextName);
  }

  public void remove(String name) {
    contextMap.remove(name);
  }

  public String namesAndUrls() throws JsonProcessingException {
    final var node = json.createObjectNode();
    contextMap.forEach((k, v) -> node.put(k, v.cluster().urls()));
    return json.writeValueAsString(node);
  }

  public record SchemaRegistryContext(String name, SchemaRegistryCluster cluster) {

    static SchemaRegistryContext parse(JsonNode node) {
      final var name = node.get("name").textValue();
      return new SchemaRegistryContext(name, SchemaRegistryCluster.parse(node.get("cluster")));
    }

    public JsonNode toJson() {
      final var node = json.createObjectNode().put("name", this.name);
      node.set("cluster", cluster.toJson());
      return node;
    }

    public Properties properties() {
      final var props = new Properties();
      props.put("schema.registry.url", cluster.urls());
      switch (cluster.auth().type()) {
        case BASIC_AUTH -> {
          props.put("basic.auth.credentials.source", "USER_INFO");
          var auth = (SchemaRegistryContexts.UsernamePasswordAuth) cluster.auth();
          props.put(
              "basic.auth.user.info",
              "%s:%s".formatted(auth.username(), passwordHelper().decrypt(auth.password())));
        }
        default -> {}
      }
      return props;
    }

    public String kcat() {
      var urls = cluster().urls();
      final var https = "https://";
      return switch (cluster.auth().type()) {
        case BASIC_AUTH -> "\\\n -r "
            + https
            + "$SCHEMA_REGISTRY_USERNAME:$SCHEMA_REGISTRY_PASSWORD"
            + "@"
            + urls.substring(https.length())
            + " -s value=avro";
        case NO_AUTH -> "\\\n -r " + urls + " -s value=avro";
      };
    }

    public String env(boolean includeAuth) {
      var urls = cluster().urls();
      return switch (cluster.auth().type()) {
        case BASIC_AUTH -> includeAuth
            ? """
            export SCHEMA_REGISTRY_URL=%s
            export SCHEMA_REGISTRY_USERNAME=%s
            export SCHEMA_REGISTRY_PASSWORD=%s"""
                .formatted(
                    urls,
                    ((SchemaRegistryContexts.UsernamePasswordAuth) cluster.auth()).username(),
                    passwordHelper()
                        .decrypt(
                            ((SchemaRegistryContexts.UsernamePasswordAuth) cluster.auth())
                                .password()))
            : "export SCHEMA_REGISTRY_URL=%s".formatted(urls);
        case NO_AUTH -> "export SCHEMA_REGISTRY_URL=%s".formatted(urls);
      };
    }
  }

  public record SchemaRegistryCluster(String urls, SchemaRegistryAuth auth) {
    static SchemaRegistryCluster parse(JsonNode cluster) {
      return new SchemaRegistryCluster(
          cluster.get("urls").textValue(), SchemaRegistryAuth.parse(cluster.get("auth")));
    }

    public JsonNode toJson() {
      final var node = json.createObjectNode().put("urls", urls);
      node.set("auth", auth.toJson());
      return node;
    }
  }

  public interface SchemaRegistryAuth {
    AuthType type();

    static SchemaRegistryAuth parse(JsonNode auth) {
      final var type = AuthType.valueOf(auth.get("type").textValue());
      return switch (type) {
        case BASIC_AUTH -> new UsernamePasswordAuth(
            type, auth.get("username").textValue(), auth.get("password").textValue());
        default -> new NoAuth();
      };
    }

    default JsonNode toJson() {
      return json.createObjectNode().put("type", type().name());
    }

    enum AuthType {
      NO_AUTH,
      BASIC_AUTH
    }
  }

  public record NoAuth() implements SchemaRegistryAuth {
    @Override
    public AuthType type() {
      return AuthType.NO_AUTH;
    }
  }

  public record UsernamePasswordAuth(AuthType authType, String username, String password)
      implements SchemaRegistryAuth {

    public static SchemaRegistryAuth build(AuthType authType, String username, String password) {
      return new UsernamePasswordAuth(authType, username, passwordHelper().encrypt(password));
    }

    public PasswordAuthentication passwordAuth() {
      return new PasswordAuthentication(username, passwordHelper().decrypt(password).toCharArray());
    }

    @Override
    public AuthType type() {
      return authType;
    }

    @Override
    public JsonNode toJson() {
      var node = (ObjectNode) SchemaRegistryAuth.super.toJson();
      return node.put("username", username).put("password", password);
    }
  }
}
