package io.github.alexlitvinov.idembox.starter.aop;

import java.time.Duration;
import java.time.Instant;

/**
 * Carries transactional synchronization context between the outer {@link IdempotentConsumerAspect}
 * and the inner {@link IdempotentTransactionSyncAspect} / Spring {@code TransactionSynchronization}.
 */
final class IdempotentSyncContext {

    private final String key;
    private final String consumerGroup;
    private final Duration ttl;
    private final Instant lockedUntil;
    private volatile boolean markedCompleted;
    private volatile boolean synchronizationRegistered;

    IdempotentSyncContext(String key, String consumerGroup, Duration ttl, Instant lockedUntil) {
        this.key = key;
        this.consumerGroup = consumerGroup;
        this.ttl = ttl;
        this.lockedUntil = lockedUntil;
    }

    public String getKey() {
        return key;
    }

    public String getConsumerGroup() {
        return consumerGroup;
    }

    public Duration getTtl() {
        return ttl;
    }

    public Instant getLockedUntil() {
        return lockedUntil;
    }

    public boolean isMarkedCompleted() {
        return markedCompleted;
    }

    public void setMarkedCompleted(boolean markedCompleted) {
        this.markedCompleted = markedCompleted;
    }

    public boolean isSynchronizationRegistered() {
        return synchronizationRegistered;
    }

    public void setSynchronizationRegistered(boolean synchronizationRegistered) {
        this.synchronizationRegistered = synchronizationRegistered;
    }
}
