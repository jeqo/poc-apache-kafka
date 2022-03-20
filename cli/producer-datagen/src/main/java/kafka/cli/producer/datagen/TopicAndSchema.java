package kafka.cli.producer.datagen;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import java.util.List;

public record TopicAndSchema(
    String topicName,
    List<Schema> schemas
) {

  static final ObjectMapper objectMapper = new ObjectMapper();

  JsonNode toJson() {
    final var json = objectMapper.createObjectNode()
        .put("topicName", this.topicName);
    final var schemas = objectMapper.createArrayNode();
    this.schemas.forEach(s -> schemas.add(s.toJson()));
    json.set("schemas", schemas);
    return json;
  }

  record Schema (
      String name,
      String canonicalString
  ) {
    static Schema from(ParsedSchema parsedSchema) {
      return new Schema(parsedSchema.name(), parsedSchema.canonicalString());
    }

    public JsonNode toJson() {
      var json = objectMapper.createObjectNode();
      json.put("name", name)
          .put("canonicalString", canonicalString);
      return json;
    }
  }
}
