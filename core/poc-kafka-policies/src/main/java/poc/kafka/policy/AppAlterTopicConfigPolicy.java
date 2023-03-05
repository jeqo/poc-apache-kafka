package poc.kafka.policy;

import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.server.policy.AlterConfigPolicy;

import java.util.Map;

public class AppAlterTopicConfigPolicy implements AlterConfigPolicy {

    @Override
    public void configure(Map<String, ?> map) {

    }

    Map<String, String> expected = Map.of("c1", "v1");

    @Override
    public void validate(RequestMetadata meta) throws PolicyViolationException {
        if (meta.resource().type() == ConfigResource.Type.TOPIC) {
            if (meta.resource().name().startsWith("app_")) {
                // match configs with expected state
                for (Map.Entry<String, String> config: meta.configs().entrySet()) {
                    if (expected.containsKey(config.getKey())) {
                        if (!expected.get(config.getKey()).equals(config.getValue())) {
                            throw new PolicyViolationException(
                                    "Changing config %s to %s is not allowed"
                                            .formatted(
                                                    config.getKey(),
                                                    config.getValue()
                                            ));
                        }
                    }
                }
            } // else ignore request
        } // else ignore request
    }

    @Override
    public void close() throws Exception {

    }
}
