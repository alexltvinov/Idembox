package io.github.alexlitvinov.idembox.core.model;

import java.time.Instant;
import java.util.Objects;

/**
 * Tracks processed message keys to prevent duplicate execution across distributed consumers.
 */
public class IdempotentRecord {

    private final String eventKey;
    private final String consumerGroup;
    private ProcessingStatus status;
    private final Instant createdAt;
    private Instant completedAt;
    private Instant lockedUntil;
    private Instant expiresAt;

    public IdempotentRecord(
            String eventKey,
            String consumerGroup,
            ProcessingStatus status,
            Instant createdAt,
            Instant completedAt,
            Instant lockedUntil,
            Instant expiresAt) {
        this.eventKey = Objects.requireNonNull(eventKey, "eventKey must not be null");
        this.consumerGroup = Objects.requireNonNull(consumerGroup, "consumerGroup must not be null");
        this.status = Objects.requireNonNull(status, "status must not be null");
        this.createdAt = Objects.requireNonNull(createdAt, "createdAt must not be null");
        this.completedAt = completedAt;
        this.lockedUntil = lockedUntil;
        this.expiresAt = expiresAt;
    }

    public static IdempotentRecord inFlight(String eventKey, String consumerGroup, Instant lockedUntil, Instant expiresAt) {
        return new IdempotentRecord(
                eventKey,
                consumerGroup,
                ProcessingStatus.PROCESSING,
                Instant.now(),
                null,
                lockedUntil,
                expiresAt
        );
    }

    public String getEventKey() {
        return eventKey;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public ProcessingStatus getStatus() {
        return status;
    }

    public void setStatus(ProcessingStatus status) {
        this.status = status;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getCompletedAt() {
        return completedAt;
    }

    public void setCompletedAt(Instant completedAt) {
        this.completedAt = completedAt;
    }

    public Instant getLockedUntil() {
        return lockedUntil;
    }

    public void setLockedUntil(Instant lockedUntil) {
        this.lockedUntil = lockedUntil;
    }

    public Instant getExpiresAt() {
        return expiresAt;
    }

    public void setExpiresAt(Instant expiresAt) {
        this.expiresAt = expiresAt;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IdempotentRecord that = (IdempotentRecord) o;
        return Objects.equals(eventKey, that.eventKey) && Objects.equals(consumerGroup, that.consumerGroup);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventKey, consumerGroup);
    }
}
