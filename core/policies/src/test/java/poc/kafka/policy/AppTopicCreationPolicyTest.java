package poc.kafka.policy;

import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.server.policy.CreateTopicPolicy;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class AppTopicCreationPolicyTest {

    @Test
    void test_shouldFail() {
        AppTopicCreationPolicy policy = new AppTopicCreationPolicy();
        CreateTopicPolicy.RequestMetadata meta = new CreateTopicPolicy.RequestMetadata(
                "__app_t1",
                6,
                (short) 3,
                Map.of(),
                Map.of()
        );
        assertThrows(PolicyViolationException.class, () -> policy.validate(meta));
    }
}