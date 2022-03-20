package kafka.cli.quotas;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.server.quota.ClientQuotaType;

public record Quotas(List<Quota> quotas) {

  static final ObjectMapper json = new ObjectMapper();

  public String toJson() throws JsonProcessingException {
    return json.writeValueAsString(this);
  }

  record Quota(KafkaClient kafkaClient,
               Constraint constraint) {
    public static Quota from(ClientQuotaEntity entity, Map<String, Double> quotas) {
      return new Quota(KafkaClient.from(entity), Constraint.from(quotas));
    }
  }

  record KafkaClient(Optional<String> user, Optional<String> clientId, Optional<String> ip) {
    public static KafkaClient from(ClientQuotaEntity entity) {
      final var user = entity.entries().get(ClientQuotaEntity.USER);
      final var clientId = entity.entries().get(ClientQuotaEntity.CLIENT_ID);
      final var ip = entity.entries().get(ClientQuotaEntity.IP);
      return new KafkaClient(Optional.ofNullable(user), Optional.ofNullable(clientId), Optional.ofNullable(ip));
    }
  }

  record Constraint(Optional<NetworkBandwidth> produceRate,
                    Optional<NetworkBandwidth> fetchRate,
                    Optional<RequestRate> requestRate,
                    Optional<ConnectionCreationRate> connectionCreationRate) {
    public static Constraint from(Map<String, Double> quotas) {
      final var produceRate = quotas.get(ClientQuotaType.PRODUCE.name());
      final var fetchRate = quotas.get(ClientQuotaType.FETCH.name());
      final var requestRate = quotas.get(ClientQuotaType.REQUEST.name());
      final var connectionCreationRate = quotas.get("connection_creation_rate");
      return new Constraint(
              Optional.ofNullable(produceRate).map(NetworkBandwidth::new),
              Optional.ofNullable(fetchRate).map(NetworkBandwidth::new),
              Optional.ofNullable(requestRate).map(RequestRate::new),
              Optional.ofNullable(connectionCreationRate).map(ConnectionCreationRate::new)
      );
    }
  }

  record ConnectionCreationRate(double rate) {
  }

  record NetworkBandwidth(double bytesPerSec) {
  }

  record RequestRate(double percent) {
  }
}
