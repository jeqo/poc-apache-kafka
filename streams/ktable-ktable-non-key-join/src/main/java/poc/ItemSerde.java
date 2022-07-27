package poc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class ItemSerde implements Serde<Item> {

  static ObjectMapper json = new ObjectMapper();

  @Override
  public Serializer<Item> serializer() {
    return new ItemSerializer();
  }

  @Override
  public Deserializer<Item> deserializer() {
    return new ItemDeserializer();
  }

  static class ItemSerializer implements Serializer<Item> {

    @Override
    public byte[] serialize(String s, Item item) {
      try {
        return json.writeValueAsBytes(item);
      } catch (JsonProcessingException e) {
        throw new IllegalArgumentException(e);
      }
    }
  }

  static class ItemDeserializer implements Deserializer<Item> {

    @Override
    public Item deserialize(String s, byte[] bytes) {
      try {
        return json.readValue(bytes, Item.class);
      } catch (IOException e) {
        throw new IllegalArgumentException(e);
      }
    }
  }
}
