package io.github.alexlitvinov.idembox.core.repository;

import io.github.alexlitvinov.idembox.core.model.OutboxMessage;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Storage contract for transactional Outbox messages.
 */
public interface OutboxRepository {

    /**
     * Persist an outbox message within the caller's active database transaction.
     */
    void save(OutboxMessage message);

    /**
     * Atomically locks and transitions a batch of pending messages into IN_PROGRESS status
     * using FOR UPDATE SKIP LOCKED.
     * Guarantees partition-key FIFO order: subsequent messages for the same partition key
     * will not be claimed until the previous one is marked SENT or FAILED.
     *
     * @param batchSize Number of messages to claim
     * @param lockDuration How long the message is locked in-progress before another worker can retry
     * @return Claimed messages in IN_PROGRESS state
     */
    List<OutboxMessage> claimBatchForProcessing(int batchSize, Duration lockDuration);

    /**
     * Atomically locks and transitions a batch of pending messages into IN_PROGRESS status.
     * Automatically increments retry_count for expired IN_PROGRESS messages and transitions
     * them directly to FAILED if retry_count + 1 >= maxRetries.
     *
     * @param batchSize Number of messages to claim
     * @param lockDuration How long the message is locked in-progress before another worker can retry
     * @param maxRetries Maximum retry threshold before transitioning poisonous messages to FAILED
     * @return Claimed messages in IN_PROGRESS state
     */
    default List<OutboxMessage> claimBatchForProcessing(int batchSize, Duration lockDuration, int maxRetries) {
        return claimBatchForProcessing(batchSize, lockDuration);
    }

    /**
     * Atomically locks and transitions a batch of pending messages into IN_PROGRESS status.
     * Enforces partition-key FIFO order, unblocking partitions if an earlier failed message
     * is older than maxOutageDuration.
     *
     * @param batchSize Number of messages to claim
     * @param lockDuration How long the message is locked in-progress before another worker can retry
     * @param maxRetries Maximum retry threshold before transitioning poisonous messages to FAILED
     * @param maxOutageDuration Maximum duration after which an earlier failed message unblocks subsequent partition messages
     * @return Claimed messages in IN_PROGRESS state
     */
    default List<OutboxMessage> claimBatchForProcessing(int batchSize, Duration lockDuration, int maxRetries, Duration maxOutageDuration) {
        return claimBatchForProcessing(batchSize, lockDuration, maxRetries);
    }

    /**
     * Mark message as successfully delivered to Kafka.
     */
    void markAsSent(UUID id, Instant sentAt);

    /**
     * Mark message as successfully delivered to Kafka, verifying lease ownership.
     *
     * @param id Message ID
     * @param sentAt Delivery timestamp
     * @param lockedUntil Expected lease deadline (next_retry_at) held by the caller
     * @return true if updated, false if lease expired or stolen
     */
    default boolean markAsSent(UUID id, Instant sentAt, Instant lockedUntil) {
        markAsSent(id, sentAt);
        return true;
    }

    /**
     * Unconditionally marks message as SENT by primary key regardless of lease deadline,
     * protecting against duplicates if lease expired during Kafka publication.
     *
     * @param id Message ID
     * @param sentAt Delivery timestamp
     */
    default void forceMarkAsSent(UUID id, Instant sentAt) {
        markAsSent(id, sentAt);
    }

    /**
     * Mark message as failed with retry count increment, backoff calculation,
     * and transition to FAILED status when maxRetries is exceeded.
     */
    void markAsFailed(UUID id, String errorReason, int maxRetries, Duration backoff);

    /**
     * Mark message as failed with retry count increment, backoff calculation,
     * and transition to FAILED status when maxRetries is exceeded, verifying lease ownership.
     *
     * @param id Message ID
     * @param errorReason Failure reason
     * @param maxRetries Maximum retry threshold before transitioning to FAILED
     * @param backoff Delay before next delivery attempt
     * @param lockedUntil Expected lease deadline (next_retry_at) held by the caller
     * @return true if updated, false if lease expired or stolen
     */
    default boolean markAsFailed(UUID id, String errorReason, int maxRetries, Duration backoff, Instant lockedUntil) {
        markAsFailed(id, errorReason, maxRetries, backoff);
        return true;
    }

    /**
     * Housekeeping: delete sent messages older than the specified threshold.
     */
    int deleteSentBefore(Instant threshold);

    /**
     * Housekeeping: delete sent messages older than the specified threshold, respecting a time budget.
     */
    default int deleteSentBefore(Instant threshold, Duration timeBudget) {
        return deleteSentBefore(threshold);
    }

    /**
     * Housekeeping: delete sent messages older than the specified threshold, respecting a time budget, batch size, and maximum batches cap.
     */
    default int deleteSentBefore(Instant threshold, Duration timeBudget, int batchSize, int maxBatches) {
        return deleteSentBefore(threshold, timeBudget);
    }

    /**
     * Housekeeping/archival: delete permanently failed messages older than the specified threshold.
     */
    int deleteFailedBefore(Instant threshold);

    /**
     * Housekeeping/archival: delete permanently failed messages older than the specified threshold, respecting a time budget.
     */
    default int deleteFailedBefore(Instant threshold, Duration timeBudget) {
        return deleteFailedBefore(threshold);
    }

    /**
     * Housekeeping/archival: delete permanently failed messages older than the specified threshold, respecting a time budget, batch size, and maximum batches cap.
     */
    default int deleteFailedBefore(Instant threshold, Duration timeBudget, int batchSize, int maxBatches) {
        return deleteFailedBefore(threshold, timeBudget);
    }

    /**
     * Resets claimed IN_PROGRESS messages back to PENDING immediately upon early batch halt
     * (e.g. during graceful shutdown).
     *
     * @param messageIds IDs of unattempted messages to release
     */
    default void releaseClaims(List<UUID> messageIds) {
        releaseClaims(messageIds, Duration.ZERO);
    }

    /**
     * Resets claimed IN_PROGRESS messages back to PENDING with an optional deferral delay (e.g. broker cooldown),
     * preventing other cluster nodes from immediately claiming them during outages.
     *
     * @param messageIds IDs of unattempted messages to release
     * @param deferral Delay before messages become eligible for next polling attempt
     */
    void releaseClaims(List<UUID> messageIds, Duration deferral);

    /**
     * Resets claimed IN_PROGRESS messages back to PENDING with an optional deferral delay,
     * verifying lease ownership.
     *
     * @param messageIds IDs of unattempted messages to release
     * @param deferral Delay before messages become eligible for next polling attempt
     * @param lockedUntil Expected lease deadline (next_retry_at) held by the caller
     */
    default void releaseClaims(List<UUID> messageIds, Duration deferral, Instant lockedUntil) {
        releaseClaims(messageIds, deferral);
    }

    /**
     * Get the current count of pending messages in the outbox.
     *
     * @return Number of messages with status PENDING
     */
    long getPendingMessagesCount();

    /**
     * Reschedules a message after a recoverable broker outage without incrementing retry_count,
     * preserving retry budget for true poison pills and setting next_retry_at with backoff.
     *
     * @param id Message ID
     * @param errorReason Failure reason
     * @param backoff Delay before next delivery attempt
     */
    void rescheduleMessage(UUID id, String errorReason, Duration backoff);

    /**
     * Reschedules a message after a recoverable broker outage without incrementing retry_count,
     * verifying lease ownership.
     *
     * @param id Message ID
     * @param errorReason Failure reason
     * @param backoff Delay before next delivery attempt
     * @param lockedUntil Expected lease deadline (next_retry_at) held by the caller
     * @return true if updated, false if lease expired or stolen
     */
    default boolean rescheduleMessage(UUID id, String errorReason, Duration backoff, Instant lockedUntil) {
        rescheduleMessage(id, errorReason, backoff);
        return true;
    }

    /**
     * Reschedules a message after a recoverable broker outage with maximum retry cap protection,
     * transitioning to FAILED if the attempt count exceeds maxRetries.
     *
     * @param id Message ID
     * @param errorReason Failure reason
     * @param backoff Delay before next delivery attempt
     * @param maxRetries Maximum retry threshold before transitioning to FAILED
     */
    default void rescheduleMessage(UUID id, String errorReason, Duration backoff, int maxRetries) {
        rescheduleMessage(id, errorReason, backoff);
    }

    /**
     * Reschedules a message after a recoverable broker outage with maximum retry cap protection,
     * transitioning to FAILED if the attempt count exceeds maxRetries, verifying lease ownership.
     *
     * @param id Message ID
     * @param errorReason Failure reason
     * @param backoff Delay before next delivery attempt
     * @param maxRetries Maximum retry threshold before transitioning to FAILED
     * @param lockedUntil Expected lease deadline (next_retry_at) held by the caller
     * @return true if updated, false if lease expired or stolen
     */
    default boolean rescheduleMessage(UUID id, String errorReason, Duration backoff, int maxRetries, Instant lockedUntil) {
        rescheduleMessage(id, errorReason, backoff, maxRetries);
        return true;
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
