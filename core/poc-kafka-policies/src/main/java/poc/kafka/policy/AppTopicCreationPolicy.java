package poc.kafka.policy;

import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.server.policy.CreateTopicPolicy;

import java.util.Map;

/**
 * Specific topic creation policies for a certain app.
 * <p>
 * Set of validations could be built from a spec and applied on every topic creation.
 */
public class AppTopicCreationPolicy implements CreateTopicPolicy {

    @Override
    public void configure(Map<String, ?> map) {

    }

    @Override
    public void validate(RequestMetadata requestMetadata) throws PolicyViolationException {
        // e.g. preventing topic creation with certain formats
        if (requestMetadata.topic().startsWith("__app")) {
            throw new PolicyViolationException("Using prefix __app is not allowed");
        }
    }

    @Override
    public void close() throws Exception {

    }
}
