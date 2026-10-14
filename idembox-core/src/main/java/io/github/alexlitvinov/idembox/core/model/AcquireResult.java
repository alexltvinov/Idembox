package io.github.alexlitvinov.idembox.core.model;

/**
 * Result of attempting to acquire execution lock for an idempotent event.
 */
public enum AcquireResult {

    /**
     * Successfully acquired lock for new execution, or safely took over an expired lock from a dead worker node.
     */
    ACQUIRED,

    /**
     * Event has already been successfully completed in this consumer group. Safe duplicate.
     */
    DUPLICATE_COMPLETED,

    /**
     * Another worker node or thread is actively processing this event right now (lockedUntil is in the future).
     */
    IN_FLIGHT_LOCKED,

    /**
     * Event previously failed permanently in this consumer group (status FAILED, e.g. Dead Letter Queue).
     */
    DUPLICATE_FAILED
}
