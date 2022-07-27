package poc.data;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

public record Transaction(long transactionId, long cardId, String userId, long purchaseId, int storeId) {
  static final ObjectMapper jsonMapper = new ObjectMapper();

  public static Transaction fromJson(JsonNode jsonNode) {
    return new Transaction(
      jsonNode.get("transaction_id").asLong(),
      jsonNode.get("card_id").asLong(),
      jsonNode.get("user_id").asText(),
      jsonNode.get("purchase_id").asLong(),
      jsonNode.get("store_id").asInt()
    );
  }

  public ObjectNode toJson() {
    return jsonMapper
      .createObjectNode()
      .put("transaction_id", transactionId)
      .put("card_id", cardId)
      .put("user_id", userId)
      .put("purchase_id", purchaseId)
      .put("store_id", storeId);
  }
}
