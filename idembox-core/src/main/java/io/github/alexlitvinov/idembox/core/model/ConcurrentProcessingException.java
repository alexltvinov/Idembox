package io.github.alexlitvinov.idembox.core.model;

/**
 * Thrown when an identical event is currently in-flight by another active worker node.
 * Signals to messaging infrastructure (like Spring Kafka) to trigger a backoff retry.
 */
public class ConcurrentProcessingException extends RuntimeException {

    private final String eventKey;
    private final String consumerGroup;

    public ConcurrentProcessingException(String eventKey, String consumerGroup) {
        super(String.format("Event with key '%s' is currently being processed by another worker in group '%s'", eventKey, consumerGroup));
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
