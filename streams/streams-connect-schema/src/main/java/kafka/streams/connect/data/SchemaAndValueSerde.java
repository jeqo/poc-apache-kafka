package kafka.streams.connect.data;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.storage.Converter;

public class SchemaAndValueSerde implements Serde<SchemaAndValue> {

  final Converter converter;

  public SchemaAndValueSerde(Converter converter) {
    this.converter = converter;
  }

  @Override
  public Serializer<SchemaAndValue> serializer() {
    return new SchemaAndValueSerializer(converter);
  }

  @Override
  public Deserializer<SchemaAndValue> deserializer() {
    return new SchemaAndValueDeserializer(converter);
  }

  public static class SchemaAndValueSerializer implements Serializer<SchemaAndValue> {

    final Converter converter;

    public SchemaAndValueSerializer(Converter converter) {
      this.converter = converter;
    }

    @Override
    public byte[] serialize(String topic, SchemaAndValue schemaAndValue) {
      return converter.fromConnectData(topic, schemaAndValue.schema(), schemaAndValue.value());
    }
  }

  public static class SchemaAndValueDeserializer implements Deserializer<SchemaAndValue> {

    final Converter converter;

    public SchemaAndValueDeserializer(Converter converter) {
      this.converter = converter;
    }

    @Override
    public SchemaAndValue deserialize(String topic, byte[] bytes) {
      return converter.toConnectData(topic, bytes);
    }
  }
}
