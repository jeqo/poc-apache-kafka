package kafka.streams.connect.data;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

public class SchemaAndStruct {

  final Schema schema;
  final Struct value;

  public SchemaAndStruct(Schema schema, Struct value) {
    this.schema = schema;
    this.value = value;
  }
}
