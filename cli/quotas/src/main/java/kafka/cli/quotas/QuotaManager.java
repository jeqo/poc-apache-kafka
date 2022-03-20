package kafka.cli.quotas;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import kafka.cli.quotas.Quotas.Quota;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.quota.ClientQuotaEntity;
import org.apache.kafka.common.quota.ClientQuotaFilter;
import org.apache.kafka.common.quota.ClientQuotaFilterComponent;

public class QuotaManager {
    final AdminClient kafkaAdmin;

    public QuotaManager(AdminClient adminClient) {
        this.kafkaAdmin = adminClient;
    }

    public Quotas all() {
        final var filter = ClientQuotaFilter.all();
        return query(filter);
    }

    private Quotas query(ClientQuotaFilter filter) {
        try {
            final var all = kafkaAdmin.describeClientQuotas(filter).entities().get();
            final var quotas = new ArrayList<Quota>(all.size());
            for (final var entity : all.keySet()) {
                final var constraints = all.get(entity);
                final var quota = Quota.from(entity, constraints);
                quotas.add(quota);
            }
            return new Quotas(quotas);
        }
        catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public Quotas allByUser() {
        final var conditions = List.of(ClientQuotaFilterComponent.ofEntityType(ClientQuotaEntity.USER));
        final var filter = ClientQuotaFilter.contains(conditions);
        return query(filter);
    }

    public Quotas allByClient() {
        final var conditions = List.of(ClientQuotaFilterComponent.ofEntityType(ClientQuotaEntity.CLIENT_ID));
        final var filter = ClientQuotaFilter.contains(conditions);
        return query(filter);
    }

    public Quotas byUser(List<String> users) {
        final var filter = ClientQuotaFilter.contains(
                users.stream()
                        .map(u -> ClientQuotaFilterComponent.ofEntity(ClientQuotaEntity.USER, u))
                        .toList());
        return query(filter);
    }

    public Quotas byClient(List<String> clientIds) {
        final var filter = ClientQuotaFilter.contains(
                clientIds.stream()
                        .map(id -> ClientQuotaFilterComponent.ofEntity(ClientQuotaEntity.CLIENT_ID, id))
                        .toList());
        return query(filter);
    }

    public void create(Quota quota) {
        throw new UnsupportedOperationException();
    }
}
