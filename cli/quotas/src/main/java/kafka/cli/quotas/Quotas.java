package kafka.cli.quotas;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.common.config.internals.QuotaConfigs;
import org.apache.kafka.common.quota.ClientQuotaAlteration;
import org.apache.kafka.common.quota.ClientQuotaAlteration.Op;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.server.quota.ClientQuotaType;

public record Quotas(List<Quota> quotas) {

  static final ObjectMapper json = new ObjectMapper().registerModule(new Jdk8Module());

  public String toJson() throws JsonProcessingException {
    return json.writeValueAsString(this);
  }

  record Quota(KafkaClient kafkaClient,
               Constraint constraints) {

    public static Quota from(ClientQuotaEntity entity, Map<String, Double> quotas) {
      return new Quota(KafkaClient.from(entity), Constraint.from(quotas));
    }

    public ClientQuotaAlteration toAlteration() {
      return new ClientQuotaAlteration(
          kafkaClient.toEntity(),
          constraints.toEntries()
      );
    }
  }

  record KafkaClientEntity(boolean isDefault, Optional<String> id) {

  }

  record KafkaClient(KafkaClientEntity user, KafkaClientEntity clientId, KafkaClientEntity ip) {

    public static KafkaClient from(ClientQuotaEntity entity) {
      final var entries = entity.entries();
      final var userEntity = new KafkaClientEntity(
          entries.containsKey(ClientQuotaEntity.USER)
              && entries.get(ClientQuotaEntity.USER) == null,
          Optional.ofNullable(entries.get(ClientQuotaEntity.USER)));
      final var clientEntity = new KafkaClientEntity(
          entries.containsKey(ClientQuotaEntity.CLIENT_ID)
              && entries.get(ClientQuotaEntity.CLIENT_ID) == null,
          Optional.ofNullable(entries.get(ClientQuotaEntity.CLIENT_ID)));
      final var ipEntity = new KafkaClientEntity(
          entries.containsKey(ClientQuotaEntity.IP)
              && entries.get(ClientQuotaEntity.IP) == null,
          Optional.ofNullable(entries.get(ClientQuotaEntity.IP)));
      return new KafkaClient(userEntity, clientEntity, ipEntity);
    }

    public ClientQuotaEntity toEntity() {
      final var entries = new HashMap<String, String>(3);
      user.id().ifPresent(u -> entries.put(ClientQuotaEntity.USER, u));
      clientId.id().ifPresent(c -> entries.put(ClientQuotaEntity.CLIENT_ID, c));
      ip.id().ifPresent(i -> entries.put(ClientQuotaEntity.IP, i));
      return new ClientQuotaEntity(entries);
    }
  }

  record Constraint(Optional<NetworkBandwidth> produceRate,
                    Optional<NetworkBandwidth> fetchRate,
                    Optional<RequestRate> requestRate,
                    Optional<ConnectionCreationRate> connectionCreationRate) {

    static Constraint from(Map<String, Double> quotas) {
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

    public List<Op> toEntries() {
      final var entries = new ArrayList<Op>(5);
      produceRate.ifPresent(r -> entries.add(
          new Op(QuotaConfigs.PRODUCER_BYTE_RATE_OVERRIDE_CONFIG, r.bytesPerSec())));
      fetchRate.ifPresent(r -> entries.add(
          new Op(QuotaConfigs.CONSUMER_BYTE_RATE_OVERRIDE_CONFIG, r.bytesPerSec())));
      requestRate.ifPresent(
          r -> entries.add(new Op(QuotaConfigs.REQUEST_PERCENTAGE_OVERRIDE_CONFIG, r.percent())));
      connectionCreationRate.ifPresent(
          r -> entries.add(new Op(QuotaConfigs.IP_CONNECTION_RATE_OVERRIDE_CONFIG, r.rate())));
      return entries;
    }
  }

  record ConnectionCreationRate(double rate) {
  }

  record NetworkBandwidth(double bytesPerSec) {
  }

  record RequestRate(double percent) {
  }
}
