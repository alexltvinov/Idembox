package io.github.alexlitvinov.idembox.core.model;

import java.time.Instant;

/**
 * Lightweight snapshot of an idempotent record's current state.
 */
public record ProcessingState(ProcessingStatus status, Instant lockedUntil, Instant expiresAt, boolean isLocked) {

    public ProcessingState(ProcessingStatus status, Instant lockedUntil, Instant expiresAt) {
        this(status, lockedUntil, expiresAt, lockedUntil != null && lockedUntil.isAfter(Instant.now()));
    }

    public boolean isCompleted() {
        return status == ProcessingStatus.COMPLETED;
    }
}
