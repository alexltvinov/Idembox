package io.github.alexlitvinov.idembox.core.repository;

import io.github.alexlitvinov.idembox.core.model.AcquireOutcome;
import io.github.alexlitvinov.idembox.core.model.AcquireResult;
import io.github.alexlitvinov.idembox.core.model.ProcessingState;

import java.time.Duration;
import java.time.Instant;

/**
 * Storage contract for idempotency / consumer deduplication.
 */
public interface IdempotentRepository {

    /**
     * Attempts to acquire execution rights for the given key and group.
     * Deadlines are computed in the database (e.g. CURRENT_TIMESTAMP + interval) to eliminate clock skew.
     *
     * @param eventKey Deduplication key
     * @param consumerGroup Consumer group
     * @param lockTimeout Timeout duration for in-flight crash recovery
     * @param deduplicationTtl Overall deduplication window duration
     * @return AcquireResult indicating if execution was acquired, already completed, or currently locked in-flight
     */
    AcquireResult tryAcquire(String eventKey, String consumerGroup, Duration lockTimeout, Duration deduplicationTtl);

    /**
     * Attempts to acquire execution rights and returns the acquired lease deadline in a single database round-trip.
     *
     * @param eventKey Deduplication key
     * @param consumerGroup Consumer group
     * @param lockTimeout Timeout duration for in-flight crash recovery
     * @param deduplicationTtl Overall deduplication window duration
     * @return AcquireOutcome containing the result and the acquired lease deadline (or null if not acquired)
     */
    default AcquireOutcome tryAcquireWithLease(String eventKey, String consumerGroup, Duration lockTimeout, Duration deduplicationTtl) {
        AcquireResult res = tryAcquire(eventKey, consumerGroup, lockTimeout, deduplicationTtl);
        if (res == AcquireResult.ACQUIRED) {
            ProcessingState state = getProcessingState(eventKey, consumerGroup);
            return new AcquireOutcome(res, state != null ? state.lockedUntil() : null);
        }
        return new AcquireOutcome(res, null);
    }

    /**
     * Lightweight read of the current processing state without taking a row lock or modifying data.
     *
     * @param eventKey Deduplication key
     * @param consumerGroup Consumer group
     * @return ProcessingState or null if no record exists
     */
    ProcessingState getProcessingState(String eventKey, String consumerGroup);

    /**
     * Mark execution as successfully completed with retention TTL calculated relative to database timestamp.
     *
     * @return true if marked completed successfully, false if row was not updated (e.g. stolen lease)
     */
    boolean markCompleted(String eventKey, String consumerGroup, Duration deduplicationTtl);

    /**
     * Mark execution as successfully completed with retention TTL, verifying the worker's specific lease deadline.
     * Prevents clearing the lock of an overtaking worker who stole the lease after timeout.
     *
     * @return true if marked completed successfully, false if row was not updated (e.g. stolen lease)
     */
    default boolean markCompleted(String eventKey, String consumerGroup, Duration deduplicationTtl, Instant lockedUntil) {
        return markCompleted(eventKey, consumerGroup, deduplicationTtl);
    }

    /**
     * Release or mark failed when consumer fails, so subsequent retries can be processed immediately.
     */
    void markFailed(String eventKey, String consumerGroup, String reason);

    /**
     * Release or mark failed when consumer fails, verifying the worker's specific lease deadline.
     * Prevents deleting a lock that has been overtaken by another worker after timeout.
     */
    default void markFailed(String eventKey, String consumerGroup, String reason, Instant lockedUntil) {
        markFailed(eventKey, consumerGroup, reason);
    }

    /**
     * Mark execution as permanently/terminally failed (e.g. poison pill sent to Dead Letter Topic),
     * retaining the failure state and deduplication window so topic replays do not re-execute the poison pill.
     *
     * @param eventKey Deduplication key
     * @param consumerGroup Consumer group
     * @param reason Failure cause or error message
     * @param deduplicationTtl Deduplication retention window
     */
    default void markTerminalFailed(String eventKey, String consumerGroup, String reason, Duration deduplicationTtl) {
        // Default fallback to transient failure marking
        markFailed(eventKey, consumerGroup, reason);
    }

    /**
     * Mark execution as permanently/terminally failed verifying the worker's specific lease deadline.
     *
     * @param eventKey Deduplication key
     * @param consumerGroup Consumer group
     * @param reason Failure cause or error message
     * @param deduplicationTtl Deduplication retention window
     * @param lockedUntil The worker's lease expiration deadline
     */
    default void markTerminalFailed(String eventKey, String consumerGroup, String reason, Duration deduplicationTtl, Instant lockedUntil) {
        markTerminalFailed(eventKey, consumerGroup, reason, deduplicationTtl);
    }

    /**
     * Resets a terminally failed record so that DLQ / DLT replay or manual intervention can re-acquire the lease.
     *
     * @param eventKey Deduplication key
     * @param consumerGroup Consumer group
     * @return true if a FAILED record was found and reset, false otherwise
     */
    default boolean resetFailed(String eventKey, String consumerGroup) {
        return false;
    }

    /**
     * Housekeeping: delete records where expires_at has passed.
     */
    int cleanupExpired(Instant now);

    /**
     * Housekeeping: delete records where expires_at has passed, respecting an execution time budget.
     */
    default int cleanupExpired(Instant now, Duration timeBudget) {
        return cleanupExpired(now);
    }

    /**
     * Housekeeping: delete records where expires_at has passed, respecting an execution time budget,
     * batch size, and maximum batches cap.
     *
     * @param now Current timestamp threshold
     * @param timeBudget Maximum duration allowed for the cleanup run
     * @param batchSize Number of records to delete per batch
     * @param maxBatches Maximum number of delete batches allowed per run
     * @return Total number of deleted records
     */
    default int cleanupExpired(Instant now, Duration timeBudget, int batchSize, int maxBatches) {
        return cleanupExpired(now, timeBudget);
    }

    /**
     * Executes the given action under a named advisory/distributed lock if supported by the repository.
     * Default implementation executes immediately without locking.
     *
     * @param lockName Unique name of the lock
     * @param action Action to execute while holding the lock
     * @return true if action was executed, false if lock could not be acquired
     */
    default boolean executeWithLock(String lockName, Runnable action) {
        action.run();
        return true;
    }
}
