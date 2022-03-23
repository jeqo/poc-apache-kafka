package kafka.cli.context;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public record SchemaRegistryContexts(Map<String, SchemaRegistryContext> contextMap) {
    static final ObjectMapper json = new ObjectMapper();

    static SchemaRegistryContexts from(byte[] bytes) throws IOException {
        final var tree = json.readTree(bytes);
        if (!tree.isArray())
            throw new IllegalArgumentException("JSON is not an array");


        final var array = (ArrayNode) tree;
        final var contexts = new HashMap<String, SchemaRegistryContext>(array.size());
        for (final var node : array) {
            final var context = SchemaRegistryContext.parse(node);
            contexts.put(context.name(), context);
        }

        return new SchemaRegistryContexts(contexts);
    }

    public Set<String> names() {
        return contextMap.keySet();
    }

    public byte[] serialize() throws JsonProcessingException {
        final var array = json.createArrayNode();
        for (final var ctx : contextMap.values())
            array.add(ctx.toJson());
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

    record SchemaRegistryContext(String name, SchemaRegistryCluster cluster) {

        static SchemaRegistryContext parse(JsonNode node) {
            final var name = node.get("name").textValue();
            return new SchemaRegistryContext(name, SchemaRegistryCluster.parse(node.get("cluster")));
        }

        public JsonNode toJson() {
            final var node = json.createObjectNode().put("name", this.name);
            node.set("cluster", cluster.toJson());
            return node;
        }

        public Properties properties(PasswordHelper passwordHelper) {
            final var props = new Properties();
            props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                cluster.urls());
            switch (cluster.auth().type()) {
                case BASIC_AUTH -> {
                    props.put(SchemaRegistryClientConfig.BASIC_AUTH_CREDENTIALS_SOURCE, "USER_INFO");
                    var auth = (SchemaRegistryContexts.UsernamePasswordAuth) cluster.auth();
                    props.put("basic.auth.user.info",
                        "%s:%s".formatted(auth.username(), passwordHelper.decrypt(auth.password())));
                }
                default -> {
                }
            }
            return props;
        }
    }


    record SchemaRegistryCluster(String urls, SchemaRegistryAuth auth) {
        static SchemaRegistryCluster parse(JsonNode cluster) {
            return new SchemaRegistryCluster(cluster.get("urls").textValue(),
                SchemaRegistryAuth.parse(cluster.get("auth")));
        }

        public JsonNode toJson() {
            final var node = json.createObjectNode().put("urls", urls);
            node.set("auth", auth.toJson());
            return node;
        }
    }


    interface SchemaRegistryAuth {
        AuthType type();

        static SchemaRegistryAuth parse(JsonNode auth) {
            final var type = AuthType.valueOf(auth.get("type").textValue());
            return switch (type) {
                case BASIC_AUTH -> new UsernamePasswordAuth(type,
                    auth.get("username").textValue(), auth.get("password").textValue());
                default -> new NoAuth();
            };
        }

        default JsonNode toJson() {
            return json.createObjectNode().put("type", type().name());
        }

        enum AuthType {NO_AUTH, BASIC_AUTH}
    }


    record NoAuth() implements SchemaRegistryAuth {
        @Override public AuthType type() {
            return AuthType.NO_AUTH;
        }
    }


    record UsernamePasswordAuth(AuthType authType, String username, String password)
        implements SchemaRegistryAuth {
        @Override public AuthType type() {
            return authType;
        }

        @Override public JsonNode toJson() {
            var node = (ObjectNode) SchemaRegistryAuth.super.toJson();
            return node.put("username", username).put("password", password);
        }
    }

}
