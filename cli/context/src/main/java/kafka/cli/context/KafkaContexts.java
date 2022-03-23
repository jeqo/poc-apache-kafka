package kafka.cli.context;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.security.plain.internals.PlainSaslServer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public record KafkaContexts(Map<String, KafkaContext> contextMap) {
    static final ObjectMapper json = new ObjectMapper();

    static byte[] empty() throws JsonProcessingException {
        return json.writeValueAsBytes(json.createArrayNode());
    }

    static KafkaContexts from(byte[] bytes) throws IOException {
        final var tree = json.readTree(bytes);
        if (!tree.isArray())
            throw new IllegalArgumentException("JSON is not an array");


        final var array = (ArrayNode) tree;
        final var contexts = new HashMap<String, KafkaContext>(array.size());
        for (final var node : array) {
            final var context = KafkaContext.parse(node);
            contexts.put(context.name(), context);
        }

        return new KafkaContexts(contexts);
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

    public void add(KafkaContext ctx) {
        contextMap.put(ctx.name, ctx);
    }

    public KafkaContext get(String name) {
        return contextMap.get(name);
    }

    record KafkaContext(String name, KafkaCluster cluster) {

        static KafkaContext parse(JsonNode node) {
            final var name = node.get("name").textValue();
            return new KafkaContext(name, KafkaCluster.parse(node.get("cluster")));
        }

        public JsonNode toJson() {
            final var node = json.createObjectNode().put("name", this.name);
            node.set("cluster", cluster.toJson());
            return node;
        }

        public Properties properties(PasswordHelper passwordHelper) throws IOException {
            final var props = new Properties();
            props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
                cluster.bootstrapServers());
            switch (cluster.auth().type()) {
                case SASL_PLAIN -> {
                    props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG,
                        SecurityProtocol.SASL_SSL.name);
                    props.put(SaslConfigs.SASL_MECHANISM, PlainSaslServer.PLAIN_MECHANISM);
                    var auth = (KafkaContexts.UsernamePasswordAuth) cluster.auth();
                    props.setProperty(SaslConfigs.SASL_JAAS_CONFIG,
                        "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";".formatted(
                            auth.username(), passwordHelper.decrypt(auth.password())));
                }
                default -> {
                }
            }
            return props;
        }
    }


    record KafkaCluster(String bootstrapServers, KafkaAuth auth) {
        static KafkaCluster parse(JsonNode cluster) {
            return new KafkaCluster(cluster.get("bootstrapServers").textValue(),
                KafkaAuth.parse(cluster.get("auth")));
        }

        public JsonNode toJson() {
            final var node = json.createObjectNode().put("bootstrapServers", bootstrapServers);
            node.set("auth", auth.toJson());
            return node;
        }
    }


    interface KafkaAuth {
        AuthType type();

        static KafkaAuth parse(JsonNode auth) {
            final var type = AuthType.valueOf(auth.get("type").textValue());
            return switch (type) {
                case SASL_PLAIN -> new UsernamePasswordAuth(type,
                    auth.get("username").textValue(), auth.get("password").textValue());
                default -> new NoAuth();
            };
        }

        default JsonNode toJson() {
            return json.createObjectNode().put("type", type().name());
        }

        enum AuthType {PLAINTEXT, SASL_PLAIN}
    }


    record NoAuth() implements KafkaAuth {
        @Override public AuthType type() {
            return AuthType.PLAINTEXT;
        }
    }


    record UsernamePasswordAuth(AuthType authType, String username, String password)
        implements KafkaAuth {
        @Override public AuthType type() {
            return authType;
        }

        @Override public JsonNode toJson() {
            var node = (ObjectNode) KafkaAuth.super.toJson();
            return node.put("username", username).put("password", password);
        }
    }

}