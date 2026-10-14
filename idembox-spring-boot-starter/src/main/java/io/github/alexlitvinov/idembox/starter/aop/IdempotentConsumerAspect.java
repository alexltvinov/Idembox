package io.github.alexlitvinov.idembox.starter.aop;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.alexlitvinov.idembox.core.annotation.Idempotent;
import io.github.alexlitvinov.idembox.core.model.AcquireOutcome;
import io.github.alexlitvinov.idembox.core.model.AcquireResult;
import io.github.alexlitvinov.idembox.core.model.ConcurrentProcessingException;
import io.github.alexlitvinov.idembox.core.model.DuplicateMessageException;
import io.github.alexlitvinov.idembox.core.model.IdempotencyException;
import io.github.alexlitvinov.idembox.core.model.ProcessingState;
import io.github.alexlitvinov.idembox.core.model.ProcessingStatus;
import io.github.alexlitvinov.idembox.core.repository.IdempotentRepository;
import io.github.alexlitvinov.idembox.core.spel.DurationParser;
import io.github.alexlitvinov.idembox.core.spel.SpelKeyExtractor;
import io.github.alexlitvinov.idembox.starter.config.IdemboxProperties;
import io.github.alexlitvinov.idembox.starter.metrics.IdemboxMetrics;
import io.github.alexlitvinov.idembox.starter.util.IdemboxFallbackIoExecutor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.header.Headers;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.support.AopUtils;
import org.springframework.context.EmbeddedValueResolverAware;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.core.annotation.Order;
import org.springframework.dao.DataAccessException;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.lang.NonNull;
import org.springframework.messaging.Message;
import org.springframework.transaction.NoTransactionException;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.interceptor.TransactionAspectSupport;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.util.StringValueResolver;

import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

/**
 * Aspect enforcing idempotent execution.
 * Configured with the highest precedence ({@link Ordered#HIGHEST_PRECEDENCE}) so that it executes
 * strictly outside any {@code @Transactional} boundaries. This ensures that lease acquisition
 * ({@code PROPAGATION_REQUIRES_NEW}) commits and releases its connection back to the HikariCP pool
 * before any business transaction acquires its connection, eliminating connection pool starvation and deadlocks.
 */
@Aspect
@Order(IdempotentConsumerAspect.DEFAULT_ORDER)
public class IdempotentConsumerAspect implements EmbeddedValueResolverAware, Ordered {

    public static final int DEFAULT_ORDER = Ordered.HIGHEST_PRECEDENCE;

    private static final Logger log = LoggerFactory.getLogger(IdempotentConsumerAspect.class);

    private final IdempotentRepository idempotentRepository;
    private final SpelKeyExtractor spelKeyExtractor;
    private final IdemboxProperties properties;
    private final IdemboxMetrics metrics;
    private final Executor asyncExecutor;
    private final IdempotentExceptionClassifier exceptionClassifier = new IdempotentExceptionClassifier();
    private final KafkaConsumerMetadataResolver metadataResolver = new KafkaConsumerMetadataResolver();
    private StringValueResolver valueResolver;
    private int order = DEFAULT_ORDER;

    private static final ThreadLocal<IdempotentSyncContext> CURRENT_SYNC_CONTEXT = new ThreadLocal<>();

    static IdempotentSyncContext getCurrentSyncContext() {
        return CURRENT_SYNC_CONTEXT.get();
    }

    public IdempotentConsumerAspect(
            IdempotentRepository idempotentRepository,
            SpelKeyExtractor spelKeyExtractor,
            IdemboxProperties properties,
            IdemboxMetrics metrics) {
        this(idempotentRepository, spelKeyExtractor, properties, metrics, null);
    }

    public IdempotentConsumerAspect(
            IdempotentRepository idempotentRepository,
            SpelKeyExtractor spelKeyExtractor,
            IdemboxProperties properties,
            IdemboxMetrics metrics,
            Executor asyncExecutor) {
        this.idempotentRepository = idempotentRepository;
        this.spelKeyExtractor = spelKeyExtractor;
        this.properties = properties;
        this.metrics = metrics != null ? metrics : IdemboxMetrics.noOp();
        this.asyncExecutor = asyncExecutor;
        if (properties != null && properties.getIdempotency() != null) {
            this.order = properties.getIdempotency().getOrder();
        }
    }

    @Override
    public int getOrder() {
        return this.order;
    }

    public void setOrder(int order) {
        this.order = order;
    }

    private void executeAsyncOffload(Runnable task) {
        if (asyncExecutor != null) {
            try {
                asyncExecutor.execute(task);
                return;
            } catch (RejectedExecutionException rex) {
                log.warn("Async executor rejected task on event loop thread, falling back to IdemboxFallbackIoExecutor: {}", rex.getMessage());
            }
        }
        IdemboxFallbackIoExecutor.execute(task);
    }

    @Override
    public void setEmbeddedValueResolver(@NonNull StringValueResolver resolver) {
        this.valueResolver = resolver;
    }

    private String resolve(String value) {
        if (valueResolver != null && value != null) {
            return valueResolver.resolveStringValue(value);
        }
        return value;
    }

    @Around("@annotation(io.github.alexlitvinov.idembox.core.annotation.Idempotent) || @within(io.github.alexlitvinov.idembox.core.annotation.Idempotent)")
    public Object enforceIdempotency(ProceedingJoinPoint joinPoint) throws Throwable {
        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        Object target = joinPoint.getTarget();
        Method method = target != null
                ? AopUtils.getMostSpecificMethod(signature.getMethod(), target.getClass())
                : signature.getMethod();
        Idempotent idempotent = AnnotatedElementUtils.findMergedAnnotation(method, Idempotent.class);
        if (idempotent == null && target != null) {
            idempotent = AnnotatedElementUtils.findMergedAnnotation(target.getClass(), Idempotent.class);
        }
        if (idempotent == null) {
            return joinPoint.proceed();
        }
        return enforceIdempotency(joinPoint, idempotent);
    }

    public Object enforceIdempotency(ProceedingJoinPoint joinPoint, Idempotent idempotent) throws Throwable {
        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        Object target = joinPoint.getTarget();
        Method method = target != null
                ? AopUtils.getMostSpecificMethod(signature.getMethod(), target.getClass())
                : signature.getMethod();
        Object[] args = joinPoint.getArgs();
        if (args != null && args.length > 0) {
            boolean isKafkaBatch = isKafkaBatchListener(target != null ? target.getClass() : null, method);
            Type[] genericParamTypes = method.getGenericParameterTypes();
            boolean isBatchArg = false;
            for (int i = 0; i < args.length; i++) {
                Object arg = args[i];
                if (arg == null || arg instanceof JsonNode) {
                    continue;
                }
                Type paramType = (i < genericParamTypes.length) ? genericParamTypes[i] : null;
                if (isKafkaBatchParameterType(paramType)
                        || arg instanceof ConsumerRecords<?, ?>
                        || arg instanceof ConsumerRecord<?, ?>[]) {
                    isBatchArg = true;
                    break;
                }
                Class<?> componentType = arg.getClass().getComponentType();
                if (componentType != null) {
                    if (ConsumerRecord.class.isAssignableFrom(componentType)
                            || Message.class.isAssignableFrom(componentType)) {
                        isBatchArg = true;
                        break;
                    }
                    if (!componentType.isPrimitive() && componentType != byte.class) {
                        if (isKafkaBatch) {
                            isBatchArg = true;
                            break;
                        }
                        Object[] arr = (Object[]) arg;
                        if (arr.length > 0 && (arr[0] instanceof ConsumerRecord<?, ?> || arr[0] instanceof Message<?>)) {
                            isBatchArg = true;
                            break;
                        }
                    }
                }
                if (arg instanceof Collection<?> collection) {
                    if (isKafkaBatch) {
                        isBatchArg = true;
                        break;
                    }
                    if (!collection.isEmpty()) {
                        Object first = collection.iterator().next();
                        if (first instanceof ConsumerRecord<?, ?> || first instanceof Message<?>) {
                            isBatchArg = true;
                            break;
                        }
                    }
                } else if (arg instanceof Iterable<?>) {
                    if (isKafkaBatch) {
                        isBatchArg = true;
                        break;
                    }
                }
            }
            if (isBatchArg) {
                String errorMsg = String.format("@Idempotent cannot be applied to batch listener method '%s#%s' which accepts a batch collection or array. @Idempotent is designed for single-record consumers; batch consumers must iterate and invoke an idempotent service per element.",
                        method.getDeclaringClass().getSimpleName(), method.getName());
                log.error(errorMsg);
                throw new IllegalArgumentException(errorMsg);
            }
        }

        String rawCondition = resolve(idempotent.condition());
        if (rawCondition != null && !rawCondition.isBlank()) {
            boolean shouldApply;
            try {
                shouldApply = spelKeyExtractor.evaluateCondition(rawCondition, target, method, args);
            } catch (Exception ex) {
                boolean failOnConditionError = properties == null || properties.getIdempotency() == null || properties.getIdempotency().isFailOnConditionError();
                if (failOnConditionError) {
                    String errorMsg = String.format("Failed to evaluate @Idempotent condition '%s' on method '%s#%s': %s",
                            rawCondition, method.getDeclaringClass().getSimpleName(), method.getName(), ex.getMessage());
                    log.error(errorMsg, ex);
                    throw new IllegalStateException(errorMsg, ex);
                }
                log.warn("Failed to evaluate @Idempotent condition '{}' on method '{}#{}' ({}: {}). Bypassing deduplication safely as failOnConditionError=false.",
                        rawCondition, method.getDeclaringClass().getSimpleName(), method.getName(), ex.getClass().getSimpleName(), ex.getMessage());
                shouldApply = false;
            }
            if (!shouldApply) {
                log.debug("Idempotency condition evaluated to false for method '{}#{}'. Bypassing deduplication.",
                        method.getDeclaringClass().getSimpleName(), method.getName());
                return joinPoint.proceed();
            }
        }

        String rawKeyExpression = resolve(idempotent.key());
        final String key = resolveKey(rawKeyExpression, target, method, args);
        String rawConsumerGroup = resolve(idempotent.consumerGroup());
        String resolvedConsumerGroup = resolveConsumerGroup(rawConsumerGroup, method, args);
        final String consumerGroup = normalizeConsumerGroup(resolvedConsumerGroup);

        attachHeadersIfConsumerRecordPresent(args, key, consumerGroup);

        String rawTtl = resolve(idempotent.ttl());
        if (rawTtl == null || rawTtl.isBlank()) {
            rawTtl = (properties != null && properties.getIdempotency() != null)
                    ? resolve(properties.getIdempotency().getDefaultTtl())
                    : null;
        }
        Duration ttl = DurationParser.parse(rawTtl, Duration.ofDays(7));

        String rawLockTimeout = resolve(idempotent.lockTimeout());
        if (rawLockTimeout == null || rawLockTimeout.isBlank()) {
            rawLockTimeout = (properties != null && properties.getIdempotency() != null)
                    ? resolve(properties.getIdempotency().getDefaultLockTimeout())
                    : null;
        }
        Duration lockTimeout = DurationParser.parse(rawLockTimeout, Duration.ofMinutes(2));
        if (TransactionSynchronizationManager.isActualTransactionActive()) {
            String msg = String.format("Active transaction detected upon entering @Idempotent method '%s#%s'. " +
                    "Acquiring lease via REQUIRES_NEW while holding an outer transaction connection leads to connection pool starvation and potential deadlocks. " +
                    "Ensure @Idempotent is configured with HIGHEST_PRECEDENCE or executed outside outer @Transactional boundaries, " +
                    "or set idembox.idempotency.fail-on-existing-transaction=false if intentional.",
                    method.getDeclaringClass().getSimpleName(), method.getName());
            boolean failFast = properties != null && properties.getIdempotency() != null && properties.getIdempotency().isFailOnExistingTransaction();
            if (failFast) {
                log.error(msg);
                throw new IllegalStateException(msg);
            } else {
                log.warn(msg);
            }
        }

        AcquireOutcome outcome = idempotentRepository.tryAcquireWithLease(key, consumerGroup, lockTimeout, ttl);
        AcquireResult result = (outcome != null) ? outcome.result() : idempotentRepository.tryAcquire(key, consumerGroup, lockTimeout, ttl);

        // Handle concurrent in-flight executions with lightweight read polling
        if (result == AcquireResult.IN_FLIGHT_LOCKED) {
            if (isEventLoopOrNonBlockingThread(Thread.currentThread())) {
                metrics.recordIdempotentConcurrentRetry(consumerGroup);
                log.warn("Event key='{}' in group='{}' is actively locked by another worker on non-blocking thread '{}'. Skipping blocking wait and triggering immediate retry.",
                        key, consumerGroup, Thread.currentThread().getName());
                throw new ConcurrentProcessingException(key, consumerGroup);
            }
            outcome = waitForInFlightCompletion(key, consumerGroup, lockTimeout, ttl);
            result = outcome.result();
            if (result == AcquireResult.IN_FLIGHT_LOCKED) {
                metrics.recordIdempotentConcurrentRetry(consumerGroup);
                log.warn("Event key='{}' in group='{}' is actively locked by another worker. Triggering backoff retry.", key, consumerGroup);
                throw new ConcurrentProcessingException(key, consumerGroup);
            }
        }

        // Handle terminally failed records with replay / retry support
        if (result == AcquireResult.DUPLICATE_FAILED) {
            boolean replayAllowed = idempotent.allowFailedRetry() || isReplayRequested(method, args);
            if (replayAllowed) {
                log.info("Terminally failed idempotent record detected for key='{}' in group='{}', but retry/replay is permitted. Resetting FAILED status and re-acquiring lease.", key, consumerGroup);
                idempotentRepository.resetFailed(key, consumerGroup);
                outcome = idempotentRepository.tryAcquireWithLease(key, consumerGroup, lockTimeout, ttl);
                result = (outcome != null) ? outcome.result() : idempotentRepository.tryAcquire(key, consumerGroup, lockTimeout, ttl);

                if (result == AcquireResult.IN_FLIGHT_LOCKED) {
                    if (isEventLoopOrNonBlockingThread(Thread.currentThread())) {
                        metrics.recordIdempotentConcurrentRetry(consumerGroup);
                        log.warn("Event key='{}' in group='{}' is actively locked by another worker during replay on non-blocking thread '{}'. Skipping blocking wait and triggering immediate retry.",
                                key, consumerGroup, Thread.currentThread().getName());
                        throw new ConcurrentProcessingException(key, consumerGroup);
                    }
                    outcome = waitForInFlightCompletion(key, consumerGroup, lockTimeout, ttl);
                    result = outcome.result();
                    if (result == AcquireResult.IN_FLIGHT_LOCKED) {
                        metrics.recordIdempotentConcurrentRetry(consumerGroup);
                        log.warn("Event key='{}' in group='{}' is actively locked by another worker during replay. Triggering backoff retry.", key, consumerGroup);
                        throw new ConcurrentProcessingException(key, consumerGroup);
                    }
                }
            }
        }

        if (result == AcquireResult.DUPLICATE_COMPLETED || result == AcquireResult.DUPLICATE_FAILED) {
            metrics.recordIdempotentDuplicate(consumerGroup);
            if (!idempotent.swallowDuplicate()) {
                log.warn("Duplicate {} message detected for key='{}' in group='{}'. Throwing DuplicateMessageException.",
                        result == AcquireResult.DUPLICATE_FAILED ? "failed" : "completed", key, consumerGroup);
                throw new DuplicateMessageException(key, consumerGroup);
            }
            // Acknowledge offset for swallowed duplicate in case manual acknowledgment mode is used
            if (args != null) {
                for (Object arg : args) {
                    if (arg instanceof Acknowledgment ack) {
                        ack.acknowledge();
                        log.trace("Acknowledged skipped duplicate Kafka message offset for key='{}' in group='{}'", key, consumerGroup);
                        break;
                    }
                    if (arg instanceof Message<?> springMessage) {
                        Object ackHeader = springMessage.getHeaders().get(KafkaHeaders.ACKNOWLEDGMENT);
                        if (ackHeader instanceof Acknowledgment ack) {
                            ack.acknowledge();
                            log.trace("Acknowledged skipped duplicate Kafka message offset from Message header for key='{}' in group='{}'", key, consumerGroup);
                            break;
                        }
                    }
                }
            }
            log.debug("Duplicate {} message detected for key='{}' in group='{}'. Safely swallowed.",
                    result == AcquireResult.DUPLICATE_FAILED ? "failed" : "completed", key, consumerGroup);
            Class<?> returnType = method.getReturnType();
            if (returnType.isPrimitive() && returnType != void.class) {
                log.warn("Method '{}#{}' has primitive return type '{}' with @Idempotent(swallowDuplicate = true). Returning default primitive value on duplicate, which callers may misinterpret as business failure. Consider using void or Optional/CompletableFuture return types.",
                        method.getDeclaringClass().getSimpleName(), method.getName(), returnType.getName());
            } else if (!IdempotentStartupValidator.isSafeSwallowReturnType(returnType)) {
                log.warn("Method '{}#{}' has non-container return type '{}' with @Idempotent(swallowDuplicate = true). Returning null on duplicate, which may cause NullPointerException in callers. Consider using Optional<{}> or setting swallowDuplicate = false.",
                        method.getDeclaringClass().getSimpleName(), method.getName(), returnType.getName(), returnType.getSimpleName());
            }
            if (IdempotentStartupValidator.hasSendToAnnotation(method, target != null ? target.getClass() : null)) {
                log.warn("Method '{}#{}' is annotated with @SendTo and has duplicate swallowed. Returning default value may cause Spring Kafka to publish a Tombstone record to the reply topic.",
                        method.getDeclaringClass().getSimpleName(), method.getName());
            }
            return getDefaultReturnValue(returnType);
        }

        if (result != AcquireResult.ACQUIRED) {
            log.warn("Acquire lease was not obtained for key='{}' in group='{}' (result='{}'). Aborting execution.", key, consumerGroup, result);
            throw new ConcurrentProcessingException(key, consumerGroup);
        }

        // ACQUIRED: proceed with target business logic
        metrics.recordIdempotentAcquired(consumerGroup);
        final Instant acquiredLockedUntil = (outcome != null && outcome.lockedUntil() != null)
                ? outcome.lockedUntil().truncatedTo(ChronoUnit.MICROS)
                : null;

        TransactionalAnnotationUtils.TransactionalInfo invocationTxInfo =
                TransactionalAnnotationUtils.findTransactionalInfo(method, target != null ? target.getClass() : null);

        final IdempotentSyncContext syncContext = new IdempotentSyncContext(key, consumerGroup, ttl, acquiredLockedUntil);
        final IdempotentSyncContext previousContext = CURRENT_SYNC_CONTEXT.get();
        CURRENT_SYNC_CONTEXT.set(syncContext);

        try {
            if (TransactionSynchronizationManager.isSynchronizationActive()) {
                registerTransactionSynchronization(syncContext);
            }

            Object invocationResult;
            try {
                invocationResult = joinPoint.proceed();
            } catch (Throwable t) {
                IdempotentExceptionClassifier.attachMetadata(t, key, consumerGroup);
                doHandleFailure(idempotent, key, consumerGroup, t, method, target, acquiredLockedUntil, ttl, false, invocationTxInfo);
                throw t;
            }

            if (isReactive(invocationResult)) {
                IllegalStateException ex = new IllegalStateException(
                        "Reactive return type detected for @Idempotent method " + method.getDeclaringClass().getSimpleName() + "#" + method.getName() +
                        ". Reactive streams (Mono/Flux/Publisher) are not supported with @Idempotent because execution is deferred.");
                IdempotentExceptionClassifier.attachMetadata(ex, key, consumerGroup);
                doHandleFailure(idempotent, key, consumerGroup, ex, method, target, acquiredLockedUntil, ttl, false, invocationTxInfo);
                throw ex;
            }

            if (invocationResult instanceof CompletionStage<?> stage) {
                CompletableFuture<Object> resultFuture = new CompletableFuture<>();
                stage.whenComplete((res, ex) -> {
                    Runnable postTask = () -> {
                        try {
                            if (ex != null) {
                                Throwable cause = (ex instanceof CompletionException && ex.getCause() != null)
                                        ? ex.getCause()
                                        : ex;
                                IdempotentExceptionClassifier.attachMetadata(cause, key, consumerGroup);
                                doHandleFailure(idempotent, key, consumerGroup, cause, method, target, acquiredLockedUntil, ttl, true, invocationTxInfo);
                                resultFuture.completeExceptionally(cause);
                            } else {
                                try {
                                    if (!syncContext.isMarkedCompleted()) {
                                        doHandleSuccess(key, consumerGroup, ttl, acquiredLockedUntil);
                                        syncContext.setMarkedCompleted(true);
                                    }
                                    resultFuture.complete(res);
                                } catch (Throwable successEx) {
                                    resultFuture.completeExceptionally(successEx);
                                }
                            }
                        } catch (Throwable t) {
                            resultFuture.completeExceptionally(t);
                        }
                    };
                    if (isEventLoopOrNonBlockingThread(Thread.currentThread())) {
                        executeAsyncOffload(postTask);
                    } else {
                        postTask.run();
                    }
                });
                return resultFuture;
            }

            if (!syncContext.isMarkedCompleted()) {
                doHandleSuccess(key, consumerGroup, ttl, acquiredLockedUntil);
                syncContext.setMarkedCompleted(true);
            }
            return invocationResult;
        } finally {
            if (previousContext != null) {
                CURRENT_SYNC_CONTEXT.set(previousContext);
            } else {
                CURRENT_SYNC_CONTEXT.remove();
            }
        }
    }

    void registerTransactionSynchronization(IdempotentSyncContext syncContext) {
        if (syncContext == null || syncContext.isSynchronizationRegistered()) {
            return;
        }
        syncContext.setSynchronizationRegistered(true);
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void beforeCommit(boolean readOnly) {
                if (syncContext.isMarkedCompleted()) {
                    return;
                }
                boolean completed = idempotentRepository.markCompleted(
                        syncContext.getKey(),
                        syncContext.getConsumerGroup(),
                        syncContext.getTtl(),
                        syncContext.getLockedUntil()
                );
                if (completed) {
                    syncContext.setMarkedCompleted(true);
                    log.debug("Atomically marked idempotent key '{}' in group '{}' COMPLETED in business transaction before commit",
                            syncContext.getKey(), syncContext.getConsumerGroup());
                } else {
                    metrics.recordIdempotentLeaseStolen(syncContext.getConsumerGroup());
                    log.warn("CRITICAL: Lease was stolen or lock expired before commit for key='{}' in group='{}'. Rolling back transaction.",
                            syncContext.getKey(), syncContext.getConsumerGroup());
                    throw new IdempotencyException("Lease was stolen or lock expired before transaction commit for key='" +
                            syncContext.getKey() + "' in group='" + syncContext.getConsumerGroup() + "'");
                }
            }

            @Override
            public void afterCompletion(int status) {
                if (status == TransactionSynchronization.STATUS_ROLLED_BACK) {
                    handleSurroundingTransactionRollback(
                            syncContext.getKey(),
                            syncContext.getConsumerGroup(),
                            syncContext.getLockedUntil()
                    );
                }
            }
        });
    }

    private void handleSurroundingTransactionRollback(String key, String consumerGroup, Instant lockedUntil) {
        log.warn("Surrounding transaction rolled back after idempotent method completion for key='{}' in group='{}'. Releasing lock for retry.",
                key, consumerGroup);
        try {
            idempotentRepository.markFailed(
                    key,
                    consumerGroup,
                    "Surrounding transaction rolled back after completion",
                    lockedUntil
            );
        } catch (Exception ex) {
            log.warn("Failed to release idempotent lock after surrounding transaction rollback for key='{}' in group='{}': {}",
                    key, consumerGroup, ex.getMessage());
        }
    }

    private void doHandleSuccess(
            String key,
            String consumerGroup,
            Duration ttl,
            Instant lockedUntil) {

        int maxAttempts = 3;
        long[] delaysMs = {50, 100, 200};
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                boolean completed = idempotentRepository.markCompleted(key, consumerGroup, ttl, lockedUntil);
                if (!completed) {
                    metrics.recordIdempotentLeaseStolen(consumerGroup);
                    log.warn("CRITICAL: Lease was stolen or lock expired before completion for key='{}' in group='{}'. " +
                            "Another worker may have overtaken this event (possible split-brain/duplicate processing).",
                            key, consumerGroup);
                    throw new IdempotencyException("Lease was stolen or lock expired before completion for key='" +
                            key + "' in group='" + consumerGroup + "'");
                }
                registerRollbackCleanupIfSynchronizationActive(key, consumerGroup, lockedUntil);
                break;
            } catch (IdempotencyException ex) {
                throw ex;
            } catch (Exception ex) {
                if (TransactionSynchronizationManager.isActualTransactionActive() || isTransactionAbortedException(ex)) {
                    metrics.recordIdempotentFailed(consumerGroup);
                    log.error("CRITICAL: Business method succeeded for key='{}' in group='{}', but setting COMPLETED status failed inside an active or aborted transaction (cannot retry on poisoned transaction).",
                            key, consumerGroup, ex);
                    throw new IdempotencyException("Failed to set COMPLETED status in idempotency store for key='" +
                            key + "' in group='" + consumerGroup + "' inside active/aborted transaction", ex);
                }
                if (attempt < maxAttempts) {
                    long delay = delaysMs[Math.min(attempt - 1, delaysMs.length - 1)];
                    log.warn("Failed to mark completed for key='{}' in group='{}' (attempt {}/{}). Retrying in {}ms: {}",
                            key, consumerGroup, attempt, maxAttempts, delay, ex.getMessage());
                    try {
                        Thread.sleep(delay);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        log.error("Interrupted during markCompleted retry loop for key='{}' in group='{}'", key, consumerGroup, ie);
                        throw new IdempotencyException("Interrupted during markCompleted retry loop for key='" + key + "' in group='" + consumerGroup + "'", ie);
                    }
                } else {
                    metrics.recordIdempotentFailed(consumerGroup);
                    log.error("CRITICAL: Business method succeeded for key='{}' in group='{}', but all {} attempts to set COMPLETED status in idempotency store failed.",
                            key, consumerGroup, maxAttempts, ex);
                    throw new IdempotencyException("Failed to set COMPLETED status in idempotency store for key='" +
                            key + "' in group='" + consumerGroup + "' after " + maxAttempts + " attempts", ex);
                }
            }
        }
    }

    private void registerRollbackCleanupIfSynchronizationActive(
            String key,
            String consumerGroup,
            Instant lockedUntil) {
        IdempotentSyncContext currentContext = CURRENT_SYNC_CONTEXT.get();
        if (currentContext != null && currentContext.isSynchronizationRegistered()) {
            return;
        }
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            Instant effectiveLockedUntil = resolveEffectiveLockedUntil(lockedUntil);
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCompletion(int status) {
                    if (status == TransactionSynchronization.STATUS_ROLLED_BACK) {
                        handleSurroundingTransactionRollback(key, consumerGroup, effectiveLockedUntil);
                    }
                }
            });
        }
    }

    static boolean isTransactionAbortedException(Throwable t) {
        return IdempotentExceptionClassifier.isTransactionAbortedException(t);
    }

    private void doHandleFailure(
            Idempotent idempotent,
            String key,
            String consumerGroup,
            Throwable t,
            Method method,
            Object target,
            Instant lockedUntil,
            Duration ttl,
            boolean isAsyncStage,
            TransactionalAnnotationUtils.TransactionalInfo invocationTxInfo) {

        metrics.recordIdempotentFailed(consumerGroup);

        Instant effectiveLockedUntil = resolveEffectiveLockedUntil(lockedUntil);

        // Terminal Failure Guard (must execute before any commit guards):
        // If the exception is terminal, it must NEVER be marked COMPLETED, even if it is a checked exception
        // or matches noRollbackFor rules. Partial business changes must be rolled back.
        if (isTerminalException(idempotent, t)) {
            log.error("Execution encountered terminal failure for idempotent key='{}' in group='{}'. Marking record as FAILED with ttl={}.",
                    key, consumerGroup, ttl, t);
            markActiveTransactionRollbackOnly(t);

            if (TransactionSynchronizationManager.isSynchronizationActive()) {
                TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                    @Override
                    public void afterCompletion(int status) {
                        try {
                            idempotentRepository.markTerminalFailed(key, consumerGroup, t.getMessage(), ttl, effectiveLockedUntil);
                        } catch (Exception ex) {
                            log.warn("Failed to mark idempotent record as terminally failed in afterCompletion for key='{}' in group='{}': {}",
                                    key, consumerGroup, ex.getMessage());
                        }
                    }
                });
            } else {
                try {
                    idempotentRepository.markTerminalFailed(key, consumerGroup, t.getMessage(), ttl, effectiveLockedUntil);
                } catch (DataAccessException dae) {
                    log.warn("Failed to mark idempotent record as terminally failed for key='{}' in group='{}': {}. " +
                            "The database transaction may be in an aborted state (e.g. PostgreSQL 25P02). Lock lease will expire naturally.",
                            key, consumerGroup, dae.getMessage());
                } catch (Exception ex) {
                    log.warn("Unexpected error when marking terminal failure for key='{}' in group='{}': {}. Lock lease will expire naturally.",
                            key, consumerGroup, ex.getMessage());
                }
            }
            return;
        }

        TransactionalAnnotationUtils.TransactionalInfo txInfo = invocationTxInfo != null
                ? invocationTxInfo
                : TransactionalAnnotationUtils.findTransactionalInfo(method, target != null ? target.getClass() : null);

        // Async CompletionStage + @Transactional race guard:
        // When a method returns CompletionStage/CompletableFuture and is annotated with @Transactional,
        // Spring's TransactionInterceptor commits the database transaction synchronously when the method returns the stage.
        // If the async stage later fails, deleting the lock in the idempotency store would allow Kafka redelivery
        // to re-execute the method and duplicate already-committed database modifications.
        // Therefore, we mark the record COMPLETED to protect idempotency.
        if (isAsyncStage && txInfo.isTransactional()) {
            log.error("CRITICAL: Method '{}#{}' is annotated with @Transactional and returned CompletionStage/CompletableFuture. " +
                    "Transaction committed synchronously upon method return, before asynchronous stage completion. " +
                    "The asynchronous stage failed with '{}', but the database transaction has ALREADY COMMITTED! " +
                    "Marking COMPLETED in idempotency store to prevent duplicate re-execution on retry.",
                    method.getDeclaringClass().getSimpleName(), method.getName(), t.getClass().getName(), t);
            try {
                doHandleSuccess(key, consumerGroup, ttl, effectiveLockedUntil);
            } catch (Exception ex) {
                if (ex != t) {
                    t.addSuppressed(ex);
                }
                log.error("Failed to mark COMPLETED after async failure with committed tx for key='{}' in group='{}'", key, consumerGroup, ex);
            }
            return;
        }

        // Exception Rollback Guard:
        // If the method is annotated with @Transactional and the exception does not trigger a rollback
        // (either matching an explicit noRollbackFor rule or being a Checked Exception
        // without rollbackFor = Exception.class), Spring's TransactionInterceptor will commit the database
        // transaction. Calling setRollbackOnly() would cause AbstractPlatformTransactionManager.commit()
        // to throw UnexpectedRollbackException, masking the original exception, while deleting the lock
        // in idempotency store would cause Kafka retry to re-execute committed business logic.
        // To preserve the @Transactional contract and prevent duplicate re-execution on retry, we mark COMPLETED
        // via doHandleSuccess so stolen leases (markCompleted == false) or DB errors roll back the transaction.
        boolean isCheckedException = !(t instanceof RuntimeException) && !(t instanceof Error);

        if (txInfo.isTransactional() && !txInfo.isRollbackFor(t)) {
            log.info("Exception '{}' thrown from @Transactional method '{}#{}' matches noRollbackFor rule (or default non-rollback). " +
                    "Database transaction will commit! Marking COMPLETED in idempotency store to prevent duplicate processing on retry.",
                    t.getClass().getName(), method.getDeclaringClass().getSimpleName(), method.getName());
            try {
                doHandleSuccess(key, consumerGroup, ttl, effectiveLockedUntil);
            } catch (RuntimeException | Error ex) {
                if (ex != t) {
                    ex.addSuppressed(t);
                }
                markActiveTransactionRollbackOnly(ex);
                throw ex;
            }
            return;
        }

        log.error("Execution failed for idempotent key='{}' in group='{}'. Releasing lock for retry.", key, consumerGroup, t);

        if ((txInfo.isTransactional() && txInfo.isRollbackFor(t)) || !isCheckedException) {
            markActiveTransactionRollbackOnly(t);
        }

        if (isCheckedException) {
            log.warn("Checked exception thrown from idempotent method '{}#{}'. Ensure @Transactional(rollbackFor = Exception.class) is configured if using declarative transactions.",
                    method.getDeclaringClass().getSimpleName(), method.getName());
        }

        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCompletion(int status) {
                    try {
                        idempotentRepository.markFailed(key, consumerGroup, t.getMessage(), effectiveLockedUntil);
                    } catch (Exception ex) {
                        log.warn("Failed to release idempotent lock in afterCompletion for key='{}' in group='{}': {}",
                                key, consumerGroup, ex.getMessage());
                    }
                }
            });
        } else {
            try {
                idempotentRepository.markFailed(key, consumerGroup, t.getMessage(), effectiveLockedUntil);
            } catch (DataAccessException dae) {
                log.warn("Failed to mark idempotent record as failed for key='{}' in group='{}': {}. " +
                        "The database transaction may be in an aborted state (e.g. PostgreSQL 25P02). Lock lease will expire naturally.",
                        key, consumerGroup, dae.getMessage());
            } catch (Exception ex) {
                log.warn("Unexpected error when releasing lock on failure for key='{}' in group='{}': {}. Lock lease will expire naturally.",
                        key, consumerGroup, ex.getMessage());
            }
        }
    }

    private void markActiveTransactionRollbackOnly(Throwable cause) {
        try {
            TransactionStatus txStatus = TransactionAspectSupport.currentTransactionStatus();
            if (!txStatus.isCompleted()) {
                txStatus.setRollbackOnly();
                log.warn("Marked active database transaction as rollback-only due to exception in idempotent method: {}",
                        cause != null ? cause.getMessage() : "unknown");
            }
        } catch (NoTransactionException ignored) {
            // No surrounding Spring transaction active on current thread
        }
    }

    private Instant resolveEffectiveLockedUntil(Instant lockedUntil) {
        return lockedUntil != null ? lockedUntil.truncatedTo(ChronoUnit.MICROS) : null;
    }

    boolean isTerminalException(Idempotent idempotent, Throwable t) {
        return exceptionClassifier.isTerminalException(idempotent, t);
    }

    private AcquireOutcome waitForInFlightCompletion(
            String key,
            String group,
            Duration lockTimeout,
            Duration deduplicationTtl) {

        if (isEventLoopOrNonBlockingThread(Thread.currentThread())) {
            log.warn("Blocking in-flight wait invoked on non-blocking thread '{}' for key='{}' in group='{}'. Aborting wait to protect event loop.",
                    Thread.currentThread().getName(), key, group);
            return new AcquireOutcome(AcquireResult.IN_FLIGHT_LOCKED, null);
        }

        Duration inFlightWait = properties.getIdempotency().getInFlightWaitTimeout();
        long maxWaitCapMs = IdemboxProperties.IdempotencyProperties.MAX_IN_FLIGHT_WAIT_TIMEOUT.toMillis();
        long waitTimeoutMs = (inFlightWait != null && !inFlightWait.isNegative())
                ? Math.min(inFlightWait.toMillis(), maxWaitCapMs)
                : Math.min(3000L, maxWaitCapMs);
        long intervalMs = 25; // Progressive backoff starts fast
        long maxIntervalMs = 250;
        long elapsed = 0;

        while (elapsed < waitTimeoutMs) {
            try {
                Thread.sleep(intervalMs);
                elapsed += intervalMs;
                intervalMs = Math.min(intervalMs * 2, maxIntervalMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return new AcquireOutcome(AcquireResult.IN_FLIGHT_LOCKED, null);
            }

            ProcessingState state = idempotentRepository.getProcessingState(key, group);
            if (state == null) {
                AcquireOutcome probe = idempotentRepository.tryAcquireWithLease(key, group, lockTimeout, deduplicationTtl);
                if (probe == null) {
                    AcquireResult res = idempotentRepository.tryAcquire(key, group, lockTimeout, deduplicationTtl);
                    probe = new AcquireOutcome(res, null);
                }
                if (probe.result() != AcquireResult.IN_FLIGHT_LOCKED) {
                    return probe;
                }
            } else if (state.isCompleted()) {
                return new AcquireOutcome(AcquireResult.DUPLICATE_COMPLETED, null);
            } else if (state.status() == ProcessingStatus.FAILED) {
                return new AcquireOutcome(AcquireResult.DUPLICATE_FAILED, null);
            } else if (!state.isLocked()) {
                AcquireOutcome probe = idempotentRepository.tryAcquireWithLease(key, group, lockTimeout, deduplicationTtl);
                if (probe == null) {
                    AcquireResult res = idempotentRepository.tryAcquire(key, group, lockTimeout, deduplicationTtl);
                    probe = new AcquireOutcome(res, null);
                }
                if (probe.result() != AcquireResult.IN_FLIGHT_LOCKED) {
                    return probe;
                }
            }
        }

        return new AcquireOutcome(AcquireResult.IN_FLIGHT_LOCKED, null);
    }

    Object getDefaultReturnValue(Class<?> returnType) {
        return KafkaConsumerMetadataResolver.getDefaultReturnValue(returnType);
    }

    static String normalizeConsumerGroup(String group) {
        return KafkaConsumerMetadataResolver.normalizeConsumerGroup(group);
    }

    private String resolveKey(String rawKeyExpression, Object target, Method method, Object[] args) {
        return metadataResolver.resolveKey(rawKeyExpression, target, method, args, spelKeyExtractor);
    }

    private String resolveConsumerGroup(String rawConsumerGroup, Method method, Object[] args) {
        return metadataResolver.resolveConsumerGroup(rawConsumerGroup, method, args, valueResolver);
    }

    static boolean isReactive(Object obj) {
        return KafkaConsumerMetadataResolver.isReactive(obj);
    }

    static boolean isReactiveType(Class<?> clazz) {
        return KafkaConsumerMetadataResolver.isReactiveType(clazz);
    }

    static boolean isReplayRequested(Method method, Object[] args) {
        return KafkaConsumerMetadataResolver.isReplayRequested(method, args);
    }

    static boolean isEventLoopOrNonBlockingThread(Thread thread) {
        return KafkaConsumerMetadataResolver.isEventLoopOrNonBlockingThread(thread);
    }

    static boolean hasKafkaListenerAnnotation(Class<?> targetClass, Method method) {
        return KafkaConsumerMetadataResolver.hasKafkaListenerAnnotation(targetClass, method);
    }

    boolean isKafkaBatchListener(Class<?> targetClass, Method method) {
        return metadataResolver.isKafkaBatchListener(targetClass, method, valueResolver);
    }

    static boolean isKafkaBatchParameterType(Type type) {
        return KafkaConsumerMetadataResolver.isKafkaBatchParameterType(type);
    }

    static void attachHeadersIfConsumerRecordPresent(Object[] args, String key, String consumerGroup) {
        if (args == null) {
            return;
        }
        for (Object arg : args) {
            if (arg instanceof ConsumerRecord<?, ?> cr) {
                populateRecordHeaders(cr.headers(), key, consumerGroup);
                break;
            }
            if (arg instanceof Message<?> msg) {
                Object raw = msg.getHeaders().get(KafkaHeaders.RAW_DATA);
                if (raw instanceof ConsumerRecord<?, ?> cr) {
                    populateRecordHeaders(cr.headers(), key, consumerGroup);
                    break;
                }
            }
        }
    }

    private static void populateRecordHeaders(Headers headers, String key, String consumerGroup) {
        if (headers == null) {
            return;
        }
        if (key != null && !key.isBlank()) {
            headers.add(new org.apache.kafka.common.header.internals.RecordHeader(
                    "X-Idempotent-Key", key.getBytes(StandardCharsets.UTF_8)));
        }
        if (consumerGroup != null && !consumerGroup.isBlank()) {
            headers.add(new org.apache.kafka.common.header.internals.RecordHeader(
                    "X-Idempotent-Group", consumerGroup.getBytes(StandardCharsets.UTF_8)));
        }
    }
}
