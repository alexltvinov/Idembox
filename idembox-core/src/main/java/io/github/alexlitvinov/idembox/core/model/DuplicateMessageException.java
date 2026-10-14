package io.github.alexlitvinov.idembox.core.model;

/**
 * Thrown when an idempotent consumer receives a duplicate message and {@code swallowDuplicate} is false.
 */
public class DuplicateMessageException extends RuntimeException {

    private final String eventKey;
    private final String consumerGroup;

    public DuplicateMessageException(String eventKey, String consumerGroup) {
        super(String.format("Duplicate event detected for key '%s' in group '%s'", eventKey, consumerGroup));
        this.eventKey = eventKey;
        this.consumerGroup = consumerGroup;
    }

    public String getEventKey() {
        return eventKey;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }
}
