package poc.data;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import poc.data.Transaction;

public class TransactionSerde implements Serde<Transaction> {

  static ObjectMapper jsonMapper = new ObjectMapper();

  @Override
  public Serializer<Transaction> serializer() {
    return new TransactionSerializer();
  }

  @Override
  public Deserializer<Transaction> deserializer() {
    return new TransactionDeserializer();
  }

  static class TransactionSerializer implements Serializer<Transaction> {

    @Override
    public byte[] serialize(String s, Transaction transaction) {
      try {
        if (transaction == null) {
          return new byte[]{};
        }
        final var node = transaction.toJson();
        return jsonMapper.writeValueAsBytes(node);
      } catch (JsonProcessingException e) {
        throw new RuntimeException(e);
      }
    }
  }

  static class TransactionDeserializer implements Deserializer<Transaction> {

    @Override
    public Transaction deserialize(String s, byte[] bytes) {
      try {
        if (bytes == null || bytes.length == 0) {
          return null;
        }
        final var node = jsonMapper.readTree(bytes);
        return Transaction.fromJson(node);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
