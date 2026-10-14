package io.github.alexlitvinov.idembox.starter.poller;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.alexlitvinov.idembox.core.model.OutboxMessage;
import io.github.alexlitvinov.idembox.core.repository.OutboxRepository;
import io.github.alexlitvinov.idembox.starter.config.IdemboxProperties;
import io.github.alexlitvinov.idembox.starter.metrics.IdemboxMetrics;
import io.github.alexlitvinov.idembox.starter.publisher.DefaultOutboxPublisher.OutboxCommittedEvent;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.lang.NonNull;
import org.springframework.scheduling.annotation.Scheduled;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Background poller and worker for the Transactional Outbox pattern.
 * <p>
 * Picks batches of PENDING/IN_PROGRESS messages using PostgreSQL's {@code SKIP LOCKED}
 * and delivers them to Apache Kafka outside the database transaction.
 * <p>
 * <b>Threading and Transactions Note:</b>
 * The {@link KafkaOperations} injected into this poller should ideally be non-transactional.
 * The Transactional Outbox pattern guarantees atomicity without the heavy overhead of 2PC
 * Kafka transactions. If a transactional {@link org.springframework.kafka.core.KafkaTemplate}
 * is supplied, OutboxPoller automatically executes send within {@code executeInTransaction}
 * to prevent {@link IllegalStateException}.
 */
public class OutboxPoller implements SmartLifecycle, DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(OutboxPoller.class);

    private final OutboxRepository outboxRepository;
    private final KafkaOperations<?, ?> kafkaOperations;
    private final IdemboxProperties properties;
    private final IdemboxMetrics metrics;
    private final org.springframework.core.task.TaskExecutor outboxAsyncExecutor;
    private final OutboxMessageRecordMapper recordMapper;
    private final OutboxRetryPolicy retryPolicy = new OutboxRetryPolicy();
    private final AtomicBoolean isPolling = new AtomicBoolean(false);
    private final AtomicBoolean pendingRepoll = new AtomicBoolean(false);
    private final AtomicBoolean isDispatchScheduled = new AtomicBoolean(false);
    private final Object pollingMonitor = new Object();

    private volatile boolean running = false;
    private volatile long brokerCooldownUntilMs = 0;

    @Override
    public boolean isRunning() {
        return running;
    }

    @Override
    public void start() {
        this.running = true;
        log.info("OutboxPoller started (phase={}).", getPhase());
    }

    @Override
    public void stop() {
        shutdown();
    }

    @Override
    public void stop(@NonNull Runnable callback) {
        shutdown();
        callback.run();
    }

    @Override
    public int getPhase() {
        // High phase stops before standard singleton beans (KafkaTemplate, DataSource at phase 0)
        // but after Kafka listener containers (phase Integer.MAX_VALUE - 100)
        return Integer.MAX_VALUE - 500;
    }

    @Override
    public boolean isAutoStartup() {
        return true;
    }

    public long getBrokerCooldownUntilMs() {
        return brokerCooldownUntilMs;
    }

    public void setBrokerCooldownUntilMs(long brokerCooldownUntilMs) {
        this.brokerCooldownUntilMs = brokerCooldownUntilMs;
    }

    @PreDestroy
    @Override
    public void destroy() {
        shutdown();
    }

    public void shutdown() {
        log.info("Initiating graceful shutdown for OutboxPoller...");
        this.running = false;
        Duration sendTimeout = properties.getOutbox().getSendTimeout();
        long timeoutMs = (sendTimeout != null && !sendTimeout.isNegative()) ? sendTimeout.toMillis() + 2000L : 7000L;
        long deadline = System.currentTimeMillis() + timeoutMs;
        synchronized (pollingMonitor) {
            while (isPolling.get()) {
                long remainingMs = deadline - System.currentTimeMillis();
                if (remainingMs <= 0) {
                    break;
                }
                try {
                    pollingMonitor.wait(Math.min(remainingMs, 50L));
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    public enum DeliveryStatus {
        SUCCESS,
        RECOVERABLE_BROKER_OUTAGE,
        MESSAGE_ERROR
    }

    public OutboxPoller(
            OutboxRepository outboxRepository,
            KafkaOperations<?, ?> kafkaOperations,
            IdemboxProperties properties,
            IdemboxMetrics metrics) {
        this(outboxRepository, kafkaOperations, properties, metrics, null, null);
    }

    public OutboxPoller(
            OutboxRepository outboxRepository,
            KafkaOperations<?, ?> kafkaOperations,
            IdemboxProperties properties,
            IdemboxMetrics metrics,
            org.springframework.core.task.TaskExecutor outboxAsyncExecutor) {
        this(outboxRepository, kafkaOperations, properties, metrics, outboxAsyncExecutor, null);
    }

    public OutboxPoller(
            OutboxRepository outboxRepository,
            KafkaOperations<?, ?> kafkaOperations,
            IdemboxProperties properties,
            IdemboxMetrics metrics,
            org.springframework.core.task.TaskExecutor outboxAsyncExecutor,
            ObjectMapper objectMapper) {
        this.outboxRepository = Objects.requireNonNull(outboxRepository, "OutboxRepository must not be null for OutboxPoller");
        this.kafkaOperations = Objects.requireNonNull(kafkaOperations, "KafkaOperations must not be null for OutboxPoller");
        this.properties = Objects.requireNonNull(properties, "IdemboxProperties must not be null for OutboxPoller");
        this.metrics = metrics != null ? metrics : IdemboxMetrics.noOp();
        this.outboxAsyncExecutor = outboxAsyncExecutor;
        this.recordMapper = new OutboxMessageRecordMapper(objectMapper);
    }

    /**
     * Periodic background poller that safely fetches pending messages using FOR UPDATE SKIP LOCKED
     */
    @Scheduled(
            scheduler = "idemboxTaskScheduler",
            fixedDelayString = "${idembox.outbox.poller.rate-ms:${idembox.outbox.poller-rate-ms:500}}",
            initialDelayString = "#{T(java.util.concurrent.ThreadLocalRandom).current().nextLong(100, 1000)}")
    public void schedulePoll() {
        if (!properties.getOutbox().isEnabled() || !properties.getOutbox().getPoller().isEnabled()) {
            return;
        }
        if (System.currentTimeMillis() < brokerCooldownUntilMs) {
            return;
        }
        pollAndPublishBatch();
    }

    /**
     * Event listener triggered after transaction commit.
     * Runs asynchronously on outboxAsyncExecutor background thread pool, never blocking user HTTP/Tomcat threads.
     * Single-bit barrier isDispatchScheduled prevents queue flooding and CallerRunsPolicy thread capture.
     */
    @EventListener
    public void onOutboxCommitted(OutboxCommittedEvent event) {
        if (event == null || !properties.getOutbox().isEnabled() || !properties.getOutbox().getPoller().isEnabled() || !properties.getOutbox().isImmediateFlush()) {
            return;
        }
        if (isPolling.get()) {
            pendingRepoll.set(true);
            return;
        }
        if (isDispatchScheduled.compareAndSet(false, true)) {
            try {
                if (outboxAsyncExecutor != null) {
                    final Thread callerThread = Thread.currentThread();
                    outboxAsyncExecutor.execute(() -> {
                        if (Thread.currentThread() == callerThread) {
                            throw new RejectedExecutionException("Rejecting execution on caller thread to prevent thread hijacking: " + callerThread.getName());
                        }
                        try {
                            pollAndPublishBatch();
                        } finally {
                            isDispatchScheduled.set(false);
                        }
                    });
                } else {
                    isDispatchScheduled.set(false);
                    pendingRepoll.set(true);
                    log.debug("No async executor configured for outbox poller. Deferring immediate flush to scheduled poller to prevent connection pool deadlock during afterCommit.");
                }
            } catch (Throwable t) {
                isDispatchScheduled.set(false);
                pendingRepoll.set(true);
                log.warn("Failed to dispatch immediate outbox flush to executor (e.g. RejectedExecutionException). Flag reset; poller will recover on next scheduled run.", t);
            }
        } else {
            pendingRepoll.set(true);
        }
    }

    /**
     * Processes pending messages in batches.
     * CRITICAL FIX 1.1: DB row claiming is separated from Kafka network operations.
     * Network I/O is executed outside open database transactions, preventing connection pool exhaustion.
     * Refinement 1: Backlog draining continues while full batches are returned, up to maxDrainBatches.
     * Refinement 4: Broker outage fail-fast halts batch processing early to prevent burning retries.
     */
    public int pollAndPublishBatch() {
        if (!running) {
            return 0;
        }

        if (System.currentTimeMillis() < brokerCooldownUntilMs) {
            log.debug("Skipping outbox polling due to active broker cooldown ({} ms remaining)",
                    brokerCooldownUntilMs - System.currentTimeMillis());
            return 0;
        }

        if (!isPolling.compareAndSet(false, true)) {
            // Signal that another commit arrived while poller was busy, so it won't be missed
            pendingRepoll.set(true);
            return 0;
        }

        int totalProcessed = 0;
        boolean repeatDirectly;
        do {
            try {
                totalProcessed += drainBatches();
            } finally {
                isPolling.set(false);
                synchronized (pollingMonitor) {
                    pollingMonitor.notifyAll();
                }
                repeatDirectly = scheduleRepollOnExit();
            }
        } while (running && repeatDirectly);

        return totalProcessed;
    }

    private int drainBatches() {
        int totalProcessed = 0;
        int maxDrainBatches = properties.getOutbox().getMaxDrainBatches();
        boolean shouldContinue;
        do {
            int iterations = 0;
            boolean batchBrokerOutage = false;
            do {
                if (!running || System.currentTimeMillis() < brokerCooldownUntilMs) {
                    break;
                }

                pendingRepoll.set(false);
                iterations++;

                List<OutboxMessage> claimedMessages = claimNextBatch();
                if (claimedMessages.isEmpty()) {
                    break;
                }

                log.debug("Claimed {} outbox messages for delivery (drain iteration {}/{})",
                        claimedMessages.size(), iterations, maxDrainBatches);

                List<PipelinedMessage> dispatched = new ArrayList<>();
                AtomicBoolean step3Exited = new AtomicBoolean(false);
                int[] batchProcessed = new int[1];

                boolean brokerOutageDetected = dispatchClaimedBatch(claimedMessages, dispatched, step3Exited, batchProcessed);
                if (awaitDispatchedBatch(dispatched, step3Exited, batchProcessed)) {
                    brokerOutageDetected = true;
                }
                totalProcessed += batchProcessed[0];

                if (brokerOutageDetected) {
                    batchBrokerOutage = true;
                    break;
                }
            } while (running && iterations < maxDrainBatches);

            shouldContinue = shouldContinueDraining(batchBrokerOutage);
        } while (running && shouldContinue);

        return totalProcessed;
    }

    private List<OutboxMessage> claimNextBatch() {
        int batchSize = properties.getOutbox().getBatchSize();
        Duration lockDuration = resolveEffectiveLockDuration();
        int maxRetries = properties.getOutbox().getMaxRetries();
        Duration maxOutageDuration = properties.getOutbox().getMaxOutageDuration();
        return outboxRepository.claimBatchForProcessing(batchSize, lockDuration, maxRetries, maxOutageDuration);
    }

    private Duration resolveEffectiveLockDuration() {
        Duration configuredLockDuration = properties.getOutbox().getInProgressLockDuration();
        Duration kafkaDeliveryTimeout = properties.getOutbox().getKafkaDeliveryTimeout();
        Duration configuredSendTimeout = properties.getOutbox().getSendTimeout();
        long baseLockMs = configuredLockDuration != null ? configuredLockDuration.toMillis() : 300_000L;
        long deliveryTimeoutMs = kafkaDeliveryTimeout != null ? kafkaDeliveryTimeout.toMillis() : 120_000L;
        long configuredSendTimeoutMs = configuredSendTimeout != null ? configuredSendTimeout.toMillis() : 10_000L;
        long minSafeLockMs = Math.max(deliveryTimeoutMs, configuredSendTimeoutMs) + 30_000L;
        return Duration.ofMillis(Math.max(baseLockMs, minSafeLockMs));
    }

    private boolean shouldContinueDraining(boolean batchBrokerOutage) {
        if (batchBrokerOutage || !running || System.currentTimeMillis() < brokerCooldownUntilMs) {
            return false;
        }
        if (pendingRepoll.get()) {
            if (outboxAsyncExecutor != null) {
                // Yield thread after maxDrainBatches; finally block will schedule repoll on outboxAsyncExecutor after clearing isPolling
                return false;
            }
            return pendingRepoll.compareAndSet(true, false);
        }
        return false;
    }

    private boolean scheduleRepollOnExit() {
        if (!running || System.currentTimeMillis() < brokerCooldownUntilMs || !pendingRepoll.compareAndSet(true, false)) {
            return false;
        }
        if (outboxAsyncExecutor != null) {
            try {
                outboxAsyncExecutor.execute(this::pollAndPublishBatch);
                return false;
            } catch (RejectedExecutionException rejectedEx) {
                log.warn("Outbox executor rejected repoll task after clearing isPolling, continuing in loop iteratively: {}", rejectedEx.getMessage());
            }
        }
        if (isPolling.compareAndSet(false, true)) {
            return true;
        }
        pendingRepoll.set(true);
        return false;
    }

    private boolean dispatchClaimedBatch(
            List<OutboxMessage> claimedMessages,
            List<PipelinedMessage> dispatched,
            AtomicBoolean step3Exited,
            int[] batchProcessed) {
        for (int i = 0; i < claimedMessages.size(); i++) {
            if (!running) {
                int remaining = claimedMessages.size() - i;
                log.info("Graceful shutdown received mid-batch. Releasing {} unattempted claims back to PENDING.", remaining);
                releaseUnattemptedClaims(claimedMessages.subList(i, claimedMessages.size()), Duration.ZERO);
                return true;
            }

            if (System.currentTimeMillis() < brokerCooldownUntilMs) {
                Duration releaseCooldown = Duration.ofMillis(calculateJitteredCooldownMs());
                releaseUnattemptedClaims(claimedMessages.subList(i, claimedMessages.size()), releaseCooldown);
                return true;
            }

            OutboxMessage msg = claimedMessages.get(i);

            if (kafkaOperations instanceof org.springframework.kafka.core.KafkaTemplate<?, ?> kafkaTemplate && kafkaTemplate.isTransactional()) {
                DeliveryStatus status = processSingleMessage(msg);
                if (status == DeliveryStatus.SUCCESS || status == DeliveryStatus.MESSAGE_ERROR) {
                    batchProcessed[0]++;
                }
                if (status == DeliveryStatus.RECOVERABLE_BROKER_OUTAGE) {
                    if (i + 1 < claimedMessages.size()) {
                        Duration releaseCooldown = !running ? Duration.ZERO : Duration.ofMillis(calculateJitteredCooldownMs());
                        releaseUnattemptedClaims(claimedMessages.subList(i + 1, claimedMessages.size()), releaseCooldown);
                    }
                    return true;
                }
                continue;
            }

            ProducerRecord<String, Object> record = createProducerRecord(msg);
            long startNanos = System.nanoTime();
            CompletableFuture<?> future;
            try {
                future = sendRecord(kafkaOperations, record);
            } catch (Exception ex) {
                future = CompletableFuture.failedFuture(ex);
            }

            AtomicBoolean sentMarked = new AtomicBoolean(false);
            CompletableFuture<Void> dbMarkFuture = new CompletableFuture<>();
            AtomicBoolean failureHandled = new AtomicBoolean(false);
            CompletableFuture<DeliveryStatus> failureFuture = new CompletableFuture<>();
            attachSendCallbacks(msg, future, startNanos, sentMarked, dbMarkFuture, failureHandled, failureFuture, step3Exited);

            if (future.isCompletedExceptionally()) {
                batchProcessed[0]++;
                try {
                    future.join();
                } catch (Throwable t) {
                    Throwable cause = t.getCause() != null ? t.getCause() : t;
                    Duration sendTimeout = properties.getOutbox().getSendTimeout();
                    long waitTimeoutMs = (sendTimeout != null && !sendTimeout.isNegative()) ? Math.min(sendTimeout.toMillis(), 5000L) : 5000L;
                    DeliveryStatus status = resolveFailureStatus(msg, cause, failureHandled, failureFuture, waitTimeoutMs);
                    if (status == DeliveryStatus.RECOVERABLE_BROKER_OUTAGE) {
                        if (i + 1 < claimedMessages.size()) {
                            Duration releaseCooldown = !running ? Duration.ZERO : Duration.ofMillis(calculateJitteredCooldownMs());
                            releaseUnattemptedClaims(claimedMessages.subList(i + 1, claimedMessages.size()), releaseCooldown);
                        }
                        return true;
                    }
                }
                continue;
            }

            dispatched.add(new PipelinedMessage(msg, future, startNanos, sentMarked, dbMarkFuture, failureHandled, failureFuture));
        }
        return false;
    }

    private void attachSendCallbacks(
            OutboxMessage msg,
            CompletableFuture<?> future,
            long startNanos,
            AtomicBoolean sentMarked,
            CompletableFuture<Void> dbMarkFuture,
            AtomicBoolean failureHandled,
            CompletableFuture<DeliveryStatus> failureFuture,
            AtomicBoolean step3Exited) {
        UUID messageId = msg.getId();
        String topic = msg.getTopic();
        future.whenComplete((result, ex) -> {
            if (result != null) {
                Runnable markTask = () -> {
                    if (sentMarked.compareAndSet(false, true)) {
                        try {
                            boolean marked = markAsSent(msg, Instant.now());
                            if (marked) {
                                Duration actualLatency = Duration.ofNanos(System.nanoTime() - startNanos);
                                metrics.recordOutboxSent(topic, actualLatency);
                            } else {
                                metrics.recordOutboxLeaseStolen(topic);
                                log.warn("Outbox lease expired/stolen for message id={} topic={} before SENT finalization. Forcing status to SENT to prevent Kafka duplicates.",
                                        messageId, topic);
                                outboxRepository.forceMarkAsSent(msg.getId(), Instant.now());
                            }
                            dbMarkFuture.complete(null);
                        } catch (Throwable t) {
                            log.error("Failed to mark outbox message id={} as SENT in database: {}. Leaving in IN_PROGRESS to prevent Kafka duplicate publication.",
                                    messageId, t.getMessage(), t);
                            dbMarkFuture.completeExceptionally(t);
                        }
                    }
                };
                dispatchMarkTask(markTask, step3Exited);
            } else if (ex != null) {
                Runnable failTask = () -> {
                    if (failureHandled.compareAndSet(false, true)) {
                        try {
                            Throwable cause = ex instanceof java.util.concurrent.ExecutionException ? ex.getCause() : ex;
                            DeliveryStatus status = handleSendFailure(msg, cause);
                            failureFuture.complete(status);
                        } catch (Throwable t) {
                            failureFuture.completeExceptionally(t);
                        }
                    }
                };
                if (outboxAsyncExecutor != null) {
                    try {
                        outboxAsyncExecutor.execute(failTask);
                    } catch (Throwable t) {
                        failTask.run();
                    }
                } else {
                    failTask.run();
                }
            }
        });
    }

    private void dispatchMarkTask(Runnable markTask, AtomicBoolean step3Exited) {
        if (isKafkaThread(Thread.currentThread())) {
            if (outboxAsyncExecutor != null) {
                try {
                    outboxAsyncExecutor.execute(markTask);
                } catch (RejectedExecutionException rejectedEx) {
                    if (step3Exited.get()) {
                        log.warn("Outbox async executor rejected markAsSent task on Kafka thread after Step 3 exit, executing via IdemboxFallbackIoExecutor: {}", rejectedEx.getMessage());
                        io.github.alexlitvinov.idembox.starter.util.IdemboxFallbackIoExecutor.execute(markTask);
                    } else {
                        log.warn("Outbox async executor rejected markAsSent task on Kafka thread, deferring to synchronous wait: {}", rejectedEx.getMessage());
                    }
                }
            } else if (step3Exited.get()) {
                io.github.alexlitvinov.idembox.starter.util.IdemboxFallbackIoExecutor.execute(markTask);
            }
        } else if (outboxAsyncExecutor != null) {
            try {
                outboxAsyncExecutor.execute(markTask);
            } catch (RejectedExecutionException rejectedEx) {
                log.warn("Outbox async executor rejected markAsSent task (shutting down), executing directly: {}", rejectedEx.getMessage());
                markTask.run();
            }
        } else {
            markTask.run();
        }
    }

    private boolean awaitDispatchedBatch(
            List<PipelinedMessage> dispatched,
            AtomicBoolean step3Exited,
            int[] batchProcessed) {
        if (dispatched.isEmpty()) {
            step3Exited.set(true);
            return false;
        }
        Duration sendTimeout = properties.getOutbox().getSendTimeout();
        long sendTimeoutMs = (sendTimeout != null && !sendTimeout.isNegative()) ? sendTimeout.toMillis() : 10_000L;
        long dbMarkTimeoutMs = Math.max(1000L, Math.min(sendTimeoutMs, 5000L));
        boolean brokerOutageDetected = false;

        try {
            for (int i = 0; i < dispatched.size(); i++) {
                PipelinedMessage pipelinedMessage = dispatched.get(i);
                try {
                    awaitSingleDispatchedMessage(pipelinedMessage, sendTimeoutMs, dbMarkTimeoutMs);
                    batchProcessed[0]++;
                } catch (Exception ex) {
                    batchProcessed[0]++;
                    Throwable cause = ex instanceof java.util.concurrent.ExecutionException ? ex.getCause() : ex;
                    if (ex instanceof InterruptedException || cause instanceof InterruptedException) {
                        Thread.currentThread().interrupt();
                    }
                    if (ex instanceof TimeoutException
                            || ex instanceof InterruptedException
                            || !pipelinedMessage.future().isDone()) {
                        brokerOutageDetected = true;
                        long cooldownMs = calculateJitteredCooldownMs();
                        brokerCooldownUntilMs = System.currentTimeMillis() + cooldownMs;
                        log.warn("Local send wait interrupted or timed out on outbox message id={} to topic={}. Leaving message in IN_PROGRESS to prevent duplicate deliveries while background Kafka producer finishes.",
                                pipelinedMessage.message().getId(), pipelinedMessage.message().getTopic());
                        int remaining = dispatched.size() - (i + 1);
                        if (remaining > 0) {
                            log.warn("Broker outage/timeout detected during pipeline drain. Leaving {} in-flight dispatched messages in IN_PROGRESS.", remaining);
                        }
                        break;
                    }
                    DeliveryStatus status = resolveFailureStatus(
                            pipelinedMessage.message(), cause, pipelinedMessage.failureHandled(), pipelinedMessage.failureFuture(), dbMarkTimeoutMs);
                    if (status == DeliveryStatus.RECOVERABLE_BROKER_OUTAGE) {
                        brokerOutageDetected = true;
                        int remaining = dispatched.size() - (i + 1);
                        if (remaining > 0) {
                            log.warn("Broker outage detected during pipeline drain. Leaving {} in-flight dispatched messages in IN_PROGRESS to prevent duplicate deliveries upon eventual send success.", remaining);
                        }
                        break;
                    }
                }
            }
        } finally {
            step3Exited.set(true);
            finalizeCompletedMessages(dispatched);
        }
        return brokerOutageDetected;
    }

    private void awaitSingleDispatchedMessage(PipelinedMessage pipelinedMessage, long sendTimeoutMs, long dbMarkTimeoutMs) throws Exception {
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - pipelinedMessage.startNanos());
        long remainingMs = Math.max(1, sendTimeoutMs - elapsedMs);
        pipelinedMessage.future().get(remainingMs, TimeUnit.MILLISECONDS);
        boolean alreadyDone = false;
        boolean asyncMarkFailed = false;
        try {
            if (pipelinedMessage.dbMarkFuture().isDone()) {
                pipelinedMessage.dbMarkFuture().get();
                alreadyDone = true;
            }
        } catch (Exception asyncEx) {
            asyncMarkFailed = true;
            Throwable cause = asyncEx instanceof java.util.concurrent.ExecutionException ? asyncEx.getCause() : asyncEx;
            log.warn("Async markAsSent failed for outbox message id={}: {}. Falling back to synchronous retry.",
                    pipelinedMessage.message().getId(), cause != null ? cause.getMessage() : "Unknown");
        }

        if (!alreadyDone) {
            if (pipelinedMessage.sentMarked().compareAndSet(false, true)) {
                tryMarkAsSentSynchronously(pipelinedMessage, "Synchronous");
            } else if (asyncMarkFailed) {
                tryMarkAsSentSynchronously(pipelinedMessage, "Synchronous fallback");
            } else {
                try {
                    pipelinedMessage.dbMarkFuture().get(dbMarkTimeoutMs, TimeUnit.MILLISECONDS);
                } catch (Exception waitEx) {
                    Throwable cause = waitEx instanceof java.util.concurrent.ExecutionException ? waitEx.getCause() : waitEx;
                    log.warn("Async markTask for message id={} did not complete within timeout ({}). Attempting synchronous markAsSent fallback.",
                            pipelinedMessage.message().getId(), cause != null ? cause.getMessage() : "Timeout");
                    tryMarkAsSentSynchronously(pipelinedMessage, "Synchronous fallback");
                }
            }
        }
        logDeliveredMessage(pipelinedMessage.message());
    }

    private void tryMarkAsSentSynchronously(PipelinedMessage pipelinedMessage, String context) {
        try {
            boolean marked = markAsSent(pipelinedMessage.message(), Instant.now());
            if (marked) {
                Duration sendLatency = Duration.ofNanos(System.nanoTime() - pipelinedMessage.startNanos());
                metrics.recordOutboxSent(pipelinedMessage.message().getTopic(), sendLatency);
            } else {
                metrics.recordOutboxLeaseStolen(pipelinedMessage.message().getTopic());
                log.warn("{} markAsSent lease expired/stolen for message id={} topic={}. Forcing status to SENT to prevent Kafka duplicates.",
                        context, pipelinedMessage.message().getId(), pipelinedMessage.message().getTopic());
                outboxRepository.forceMarkAsSent(pipelinedMessage.message().getId(), Instant.now());
            }
            pipelinedMessage.dbMarkFuture().complete(null);
        } catch (Throwable markEx) {
            log.warn("{} markAsSent failed for message id={}: {}. Leaving in IN_PROGRESS to prevent Kafka duplicate publication.",
                    context, pipelinedMessage.message().getId(), markEx.getMessage(), markEx);
        }
    }

    private void finalizeCompletedMessages(List<PipelinedMessage> dispatched) {
        for (PipelinedMessage pipelinedMessage : dispatched) {
            if (pipelinedMessage.future().isDone() && !pipelinedMessage.future().isCompletedExceptionally()
                    && (pipelinedMessage.sentMarked().compareAndSet(false, true) || pipelinedMessage.dbMarkFuture().isCompletedExceptionally())) {
                try {
                    boolean marked = markAsSent(pipelinedMessage.message(), Instant.now());
                    if (marked) {
                        Duration sendLatency = Duration.ofNanos(System.nanoTime() - pipelinedMessage.startNanos());
                        metrics.recordOutboxSent(pipelinedMessage.message().getTopic(), sendLatency);
                    } else {
                        metrics.recordOutboxLeaseStolen(pipelinedMessage.message().getTopic());
                        log.warn("Post-Step-3 markAsSent lease expired/stolen for message id={} topic={}. Forcing status to SENT to prevent Kafka duplicates.",
                                pipelinedMessage.message().getId(), pipelinedMessage.message().getTopic());
                        outboxRepository.forceMarkAsSent(pipelinedMessage.message().getId(), Instant.now());
                    }
                    pipelinedMessage.dbMarkFuture().complete(null);
                } catch (Throwable markEx) {
                    log.warn("Post-Step-3 markAsSent failed for message id={}: {}. Leaving in IN_PROGRESS to prevent Kafka duplicate publication.",
                            pipelinedMessage.message().getId(), markEx.getMessage());
                    pipelinedMessage.dbMarkFuture().completeExceptionally(markEx);
                }
            }
        }
    }

    private static void logDeliveredMessage(OutboxMessage msg) {
        log.trace("Delivered outbox message id={} to topic={}", msg.getId(), msg.getTopic());
    }

    @SuppressWarnings("unchecked")
    private static CompletableFuture<?> sendRecord(
            KafkaOperations<?, ?> operations,
            ProducerRecord<String, Object> record) {
        return ((KafkaOperations<String, Object>) operations).send(record);
    }

    private DeliveryStatus resolveFailureStatus(
            OutboxMessage msg,
            Throwable cause,
            AtomicBoolean failureHandled,
            CompletableFuture<DeliveryStatus> failureFuture,
            long waitTimeoutMs) {
        if (failureHandled.compareAndSet(false, true)) {
            try {
                DeliveryStatus status = handleSendFailure(msg, cause);
                failureFuture.complete(status);
                return status;
            } catch (Throwable t) {
                failureFuture.completeExceptionally(t);
                throw t;
            }
        }
        try {
            return failureFuture.get(Math.max(1000L, waitTimeoutMs), TimeUnit.MILLISECONDS);
        } catch (Exception waitEx) {
            if (waitEx instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            log.warn("Timed out or failed waiting for async handleSendFailure for message id={}: {}",
                    msg.getId(), waitEx.getMessage());
            return isBrokerOutage(cause) ? DeliveryStatus.RECOVERABLE_BROKER_OUTAGE : DeliveryStatus.MESSAGE_ERROR;
        }
    }

    static Object resolvePayloadForKafka(OutboxMessage msg) {
        return OutboxMessageRecordMapper.resolvePayloadForKafka(msg);
    }

    ProducerRecord<String, Object> createProducerRecord(OutboxMessage msg) {
        return recordMapper.toProducerRecord(msg, properties);
    }

    private record PipelinedMessage(
            OutboxMessage message,
            CompletableFuture<?> future,
            long startNanos,
            AtomicBoolean sentMarked,
            CompletableFuture<Void> dbMarkFuture,
            AtomicBoolean failureHandled,
            CompletableFuture<DeliveryStatus> failureFuture) {}

    long calculateJitteredCooldownMs() {
        return retryPolicy.calculateJitteredCooldownMs(properties);
    }

    static boolean isKafkaThread(Thread thread) {
        return OutboxRetryPolicy.isKafkaThread(thread);
    }

    DeliveryStatus handleSendFailure(OutboxMessage msg, Throwable ex) {
        if (!running) {
            log.info("Send aborted due to application shutdown for message id={}. Releasing claim to PENDING.", msg.getId());
            releaseClaims(List.of(msg.getId()), Duration.ZERO, msg.getNextRetryAt());
            return DeliveryStatus.RECOVERABLE_BROKER_OUTAGE;
        }

        boolean brokerOutage = isBrokerOutage(ex);
        Duration backoff = calculateBackoff(msg.getRetryCount());
        String errorReason = extractErrorReason(ex, brokerOutage ? "Broker outage" : "Unknown send error");

        if (brokerOutage) {
            Duration maxOutageDuration = properties.getOutbox().getMaxOutageDuration();
            boolean outageExpired = maxOutageDuration != null && !maxOutageDuration.isZero() && !maxOutageDuration.isNegative()
                    && msg.getCreatedAt() != null
                    && Instant.now().isAfter(msg.getCreatedAt().plus(maxOutageDuration));

            if (outageExpired) {
                metrics.recordOutboxTerminalFailure(msg.getTopic());
                String terminalReason = "Exceeded maximum broker outage duration (" + maxOutageDuration + "): " + errorReason;
                log.error("CRITICAL: Message id={} permanently failed after persistent broker outage: {}. Transitioned to FAILED status to prevent Head-of-Line deadlock.",
                        msg.getId(), terminalReason);
                markAsFailed(msg, terminalReason, 1, Duration.ZERO);
                return DeliveryStatus.MESSAGE_ERROR;
            }

            metrics.recordOutboxFailed(msg.getTopic());

            long cooldownMs = calculateJitteredCooldownMs();
            brokerCooldownUntilMs = System.currentTimeMillis() + cooldownMs;

            log.warn("Broker outage or timeout while publishing outbox message id={} to topic={}: {}. Activated {}ms cooldown.",
                    msg.getId(), msg.getTopic(), errorReason, cooldownMs);

            long elapsedMs = msg.getCreatedAt() != null ? Math.max(0, Duration.between(msg.getCreatedAt(), Instant.now()).toMillis()) : 0L;
            long exponentialMs = Math.min(properties.getOutbox().getMaxBackoff().toMillis(),
                    (long) (properties.getOutbox().getInitialBackoff().toMillis() * Math.pow(properties.getOutbox().getBackoffMultiplier(), Math.min(elapsedMs / 5000L, 10))));
            Duration deferral = Duration.ofMillis(Math.max(cooldownMs, Math.max(backoff.toMillis(), exponentialMs)));
            rescheduleMessage(msg, errorReason, deferral);
            return DeliveryStatus.RECOVERABLE_BROKER_OUTAGE;
        } else {
            boolean isFatal = isFatalProducerError(ex);
            if (isFatal) {
                metrics.recordOutboxTerminalFailure(msg.getTopic());
                log.error("Message id={} encountered non-retryable fatal Kafka error: {}. Transitioning immediately to FAILED.",
                        msg.getId(), ex != null ? ex.getClass().getSimpleName() : "Unknown");
                markAsFailed(msg, errorReason, 1, Duration.ZERO);
                return DeliveryStatus.MESSAGE_ERROR;
            }

            log.error("Failed to publish outbox message id={} to topic={}: {}", msg.getId(), msg.getTopic(), errorReason, ex);

            // Step 3b: Calculate exponential backoff and update status (transitions to FAILED if max-retries exceeded)
            int maxRetries = properties.getOutbox().getMaxRetries();
            if ((msg.getRetryCount() + 1) >= maxRetries) {
                metrics.recordOutboxTerminalFailure(msg.getTopic());
                log.error("Message id={} permanently failed after {} attempts. Transitioned to FAILED status.", msg.getId(), maxRetries);
            } else {
                metrics.recordOutboxFailed(msg.getTopic());
            }

            markAsFailed(msg, errorReason, maxRetries, backoff);
            return DeliveryStatus.MESSAGE_ERROR;
        }
    }

    String extractErrorReason(Throwable ex, String defaultReason) {
        return retryPolicy.extractErrorReason(ex, defaultReason);
    }

    public DeliveryStatus processSingleMessage(OutboxMessage msg) {
        CompletableFuture<?> sendFuture = null;
        long startNanos = System.nanoTime();
        try {
            ProducerRecord<String, Object> record = createProducerRecord(msg);
            Duration sendTimeout = properties.getOutbox().getSendTimeout();
            if (kafkaOperations instanceof org.springframework.kafka.core.KafkaTemplate<?, ?> kafkaTemplate && kafkaTemplate.isTransactional()) {
                kafkaTemplate.executeInTransaction(ops -> {
                    try {
                        return sendRecord(ops, record).get(sendTimeout.toMillis(), TimeUnit.MILLISECONDS);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
            } else {
                sendFuture = sendRecord(kafkaOperations, record);
                sendFuture.get(sendTimeout.toMillis(), TimeUnit.MILLISECONDS);
            }

            // Step 3a: Mark as SENT in short DB update
            PipelinedMessage singlePipeline = new PipelinedMessage(
                    msg, sendFuture, startNanos, new AtomicBoolean(true), new CompletableFuture<>(), new AtomicBoolean(false), new CompletableFuture<>());
            tryMarkAsSentSynchronously(singlePipeline, "Synchronous");
            logDeliveredMessage(msg);
            return DeliveryStatus.SUCCESS;
        } catch (Exception ex) {
            Throwable cause = ex instanceof java.util.concurrent.ExecutionException ? ex.getCause() : ex;
            if (ex instanceof InterruptedException || cause instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            if (sendFuture != null && !sendFuture.isDone()) {
                final long dispatchStartNanos = startNanos;
                sendFuture.whenComplete((res, asyncEx) -> {
                    if (asyncEx == null) {
                        try {
                            boolean marked = markAsSent(msg, Instant.now());
                            if (marked) {
                                Duration latency = Duration.ofNanos(System.nanoTime() - dispatchStartNanos);
                                metrics.recordOutboxSent(msg.getTopic(), latency);
                            } else {
                                metrics.recordOutboxLeaseStolen(msg.getTopic());
                                log.warn("Delayed markAsSent lease expired/stolen for message id={} topic={}. Forcing status to SENT to prevent Kafka duplicates.",
                                        msg.getId(), msg.getTopic());
                                outboxRepository.forceMarkAsSent(msg.getId(), Instant.now());
                            }
                        } catch (Throwable markEx) {
                            log.warn("Delayed markAsSent failed for message id={}: {}. Leaving in IN_PROGRESS.",
                                    msg.getId(), markEx.getMessage());
                        }
                    }
                });
                long cooldownMs = calculateJitteredCooldownMs();
                brokerCooldownUntilMs = System.currentTimeMillis() + cooldownMs;
                log.warn("Local send wait timed out or was interrupted for single outbox message id={} to topic={}. Leaving message in IN_PROGRESS while background Kafka producer finishes.",
                        msg.getId(), msg.getTopic());
                return DeliveryStatus.RECOVERABLE_BROKER_OUTAGE;
            }
            return handleSendFailure(msg, cause);
        }
    }

    public boolean isBrokerOutage(Throwable t) {
        return retryPolicy.isBrokerOutage(t);
    }

    public boolean isFatalProducerError(Throwable t) {
        return retryPolicy.isFatalProducerError(t);
    }

    Duration calculateBackoff(int retryCount) {
        return retryPolicy.calculateBackoff(retryCount, properties);
    }

    private boolean markAsSent(OutboxMessage msg, Instant sentAt) {
        if (msg.getNextRetryAt() != null) {
            return outboxRepository.markAsSent(msg.getId(), sentAt, msg.getNextRetryAt());
        } else {
            outboxRepository.markAsSent(msg.getId(), sentAt);
            return true;
        }
    }

    private void rescheduleMessage(OutboxMessage msg, String errorReason, Duration backoff) {
        if (msg.getNextRetryAt() != null) {
            outboxRepository.rescheduleMessage(msg.getId(), errorReason, backoff, msg.getNextRetryAt());
        } else {
            outboxRepository.rescheduleMessage(msg.getId(), errorReason, backoff);
        }
    }

    private void markAsFailed(OutboxMessage msg, String errorReason, int maxRetries, Duration backoff) {
        if (msg.getNextRetryAt() != null) {
            outboxRepository.markAsFailed(msg.getId(), errorReason, maxRetries, backoff, msg.getNextRetryAt());
        } else {
            outboxRepository.markAsFailed(msg.getId(), errorReason, maxRetries, backoff);
        }
    }

    private void releaseUnattemptedClaims(List<OutboxMessage> unattemptedMessages, Duration releaseCooldown) {
        if (unattemptedMessages == null || unattemptedMessages.isEmpty()) {
            return;
        }
        Map<Instant, List<UUID>> grouped = new LinkedHashMap<>();
        for (OutboxMessage m : unattemptedMessages) {
            grouped.computeIfAbsent(m.getNextRetryAt(), k -> new ArrayList<>()).add(m.getId());
        }
        grouped.forEach((lockedUntil, ids) -> releaseClaims(ids, releaseCooldown, lockedUntil));
    }

    private void releaseClaims(List<UUID> messageIds, Duration deferral, Instant lockedUntil) {
        if (lockedUntil != null) {
            outboxRepository.releaseClaims(messageIds, deferral, lockedUntil);
        } else {
            outboxRepository.releaseClaims(messageIds, deferral);
        }
    }

    /**
     * Housekeeping: delete sent messages and archived failed messages older than configured retention periods.
     * Delegates to {@link io.github.alexlitvinov.idembox.starter.cleaner.OutboxCleaner}.
     */
    public void cleanupOldMessages() {
        new io.github.alexlitvinov.idembox.starter.cleaner.OutboxCleaner(outboxRepository, properties).cleanupOldMessages();
    }
}
