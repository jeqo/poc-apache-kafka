package kafka.cli.quotas;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
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

    public Quotas allByUsers() {
        final var conditions = List.of(ClientQuotaFilterComponent.ofEntityType(ClientQuotaEntity.USER));
        final var filter = ClientQuotaFilter.contains(conditions);
        return query(filter);
    }

    public Quotas allByClients() {
        final var conditions = List.of(ClientQuotaFilterComponent.ofEntityType(ClientQuotaEntity.CLIENT_ID));
        final var filter = ClientQuotaFilter.contains(conditions);
        return query(filter);
    }

    public Quotas allByIps() {
        final var conditions = List.of(ClientQuotaFilterComponent.ofEntityType(ClientQuotaEntity.IP));
        final var filter = ClientQuotaFilter.contains(conditions);
        return query(filter);
    }

    public Quotas byUsers(List<String> users, boolean userDefault) {
        final var components = users.stream()
            .map(id -> ClientQuotaFilterComponent.ofEntity(ClientQuotaEntity.USER, id))
            .collect(Collectors.toList());
        if (userDefault) {
            components.add(ClientQuotaFilterComponent.ofDefaultEntity(ClientQuotaEntity.USER));
        }
        final var filter = ClientQuotaFilter.containsOnly(components);
        return query(filter);
    }

    public Quotas byUsers(Map<String, List<String>> users) {
//        final var components = users.keySet().stream()
//            .map(u -> ClientQuotaFilterComponent.ofEntity(ClientQuotaEntity.USER, u))
//            .toList();
        final var all = new Quotas(new ArrayList<>());
        final var components = new ArrayList<ClientQuotaFilterComponent>();
        for (final var user : users.keySet()) {
            components.add(ClientQuotaFilterComponent.ofEntity(ClientQuotaEntity.USER, user));
            for (final var client : users.get(user)) {
                components.add(ClientQuotaFilterComponent.ofEntity(ClientQuotaEntity.CLIENT_ID, client));
            }
            final var filter = ClientQuotaFilter.containsOnly(components);
            final var quotas = query(filter);
            all.append(quotas);
        }
        return all;
    }

    public Quotas byClients(List<String> clientIds, boolean clientIdDefault) {
        final var components = clientIds.stream()
            .map(id -> ClientQuotaFilterComponent.ofEntity(ClientQuotaEntity.CLIENT_ID, id))
            .collect(Collectors.toList());
        if (clientIdDefault) {
            components.add(ClientQuotaFilterComponent.ofDefaultEntity(ClientQuotaEntity.CLIENT_ID));
        }
        final var filter = ClientQuotaFilter.containsOnly(components);
        return query(filter);
    }

    public Quotas byIps(List<String> ips, boolean ipDefault) {
        final var components = ips.stream()
            .map(id -> ClientQuotaFilterComponent.ofEntity(ClientQuotaEntity.IP, id))
            .collect(Collectors.toList());
        if (ipDefault) {
            components.add(ClientQuotaFilterComponent.ofDefaultEntity(ClientQuotaEntity.IP));
        }
        final var filter = ClientQuotaFilter.containsOnly(components);
        return query(filter);
    }

    public void create(Quota quota) throws ExecutionException, InterruptedException {
        kafkaAdmin.alterClientQuotas(List.of(quota.toAlteration())).all().get();
    }
}
