package io.github.alexlitvinov.idembox.starter.aop;

/**
 * Carries contextual metadata (idempotent key, consumer group) across method boundaries
 * and framework call stacks (e.g. into DeadLetterPublishingRecoverer) when exceptions occur.
 */
public class IdempotentMetadataException extends RuntimeException {

    private final String key;
    private final String consumerGroup;

    public IdempotentMetadataException(String key, String consumerGroup) {
        super("Idempotent context: key=" + key + ", consumerGroup=" + consumerGroup);
        this.key = key;
        this.consumerGroup = consumerGroup;
    }

    public String getKey() {
        return key;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }
}
