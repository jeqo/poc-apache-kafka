package kafka.streams.connect.data;

import java.util.Map;
import org.apache.kafka.connect.data.Schema;

public class SchemaAndMap {

  final Schema schema;
  final Map<String, Object> value;

  public SchemaAndMap(Schema schema, Map<String, Object> value) {
    this.schema = schema;
    this.value = value;
  }
}
