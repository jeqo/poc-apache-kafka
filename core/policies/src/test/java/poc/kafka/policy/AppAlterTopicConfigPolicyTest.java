package poc.kafka.policy;

import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.server.policy.AlterConfigPolicy;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class AppAlterTopicConfigPolicyTest {
    @Test()
    void test_shouldFail() {
        AppAlterTopicConfigPolicy policy = new AppAlterTopicConfigPolicy();
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, "app_t1");
        Map<String, String> configs = Map.of("c1", "v2");
        AlterConfigPolicy.RequestMetadata meta = new AlterConfigPolicy.RequestMetadata(resource, configs);
        assertThrows(PolicyViolationException.class, () -> policy.validate(meta));
    }

    @Test()
    void test_shouldPass_withValidValue() {
        AppAlterTopicConfigPolicy policy = new AppAlterTopicConfigPolicy();
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, "app_t1");
        Map<String, String> configs = Map.of("c1", "v1");
        AlterConfigPolicy.RequestMetadata meta = new AlterConfigPolicy.RequestMetadata(resource, configs);
        assertDoesNotThrow(() -> policy.validate(meta));
    }

    @Test()
    void test_shouldPass_WithOtherTopic() {
        AppAlterTopicConfigPolicy policy = new AppAlterTopicConfigPolicy();
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, "other_t1");
        Map<String, String> configs = Map.of("c1", "v1");
        AlterConfigPolicy.RequestMetadata meta = new AlterConfigPolicy.RequestMetadata(resource, configs);
        assertDoesNotThrow(() -> policy.validate(meta));
    }
}