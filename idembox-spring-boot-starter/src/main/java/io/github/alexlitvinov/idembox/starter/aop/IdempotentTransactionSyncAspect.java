package io.github.alexlitvinov.idembox.starter.aop;

import io.github.alexlitvinov.idembox.core.model.IdempotencyException;
import io.github.alexlitvinov.idembox.starter.metrics.IdemboxMetrics;
import io.github.alexlitvinov.idembox.core.repository.IdempotentRepository;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.Ordered;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * Inner aspect with {@link Ordered#LOWEST_PRECEDENCE} running inside Spring's
 * {@code TransactionInterceptor}. Registers a {@link TransactionSynchronization} to mark
 * the idempotent key as {@code COMPLETED} in {@code beforeCommit}, fusing business data changes
 * and idempotency status update into a single atomic database transaction (preventing Dual-Commit vulnerabilities).
 */
@Aspect
public class IdempotentTransactionSyncAspect implements Ordered {

    private static final Logger log = LoggerFactory.getLogger(IdempotentTransactionSyncAspect.class);

    private final IdempotentRepository idempotentRepository;
    private final IdemboxMetrics metrics;
    private int order = Ordered.LOWEST_PRECEDENCE;

    public IdempotentTransactionSyncAspect(
            IdempotentRepository idempotentRepository,
            IdemboxMetrics metrics) {
        this.idempotentRepository = idempotentRepository;
        this.metrics = metrics != null ? metrics : IdemboxMetrics.noOp();
    }

    @Override
    public int getOrder() {
        return this.order;
    }

    public void setOrder(int order) {
        this.order = order;
    }

    @Around("@annotation(io.github.alexlitvinov.idembox.core.annotation.Idempotent) || @within(io.github.alexlitvinov.idembox.core.annotation.Idempotent)")
    public Object syncWithTransaction(ProceedingJoinPoint joinPoint) throws Throwable {
        IdempotentSyncContext syncContext = IdempotentConsumerAspect.getCurrentSyncContext();
        if (syncContext != null && TransactionSynchronizationManager.isSynchronizationActive()) {
            registerSynchronization(syncContext);
        }
        return joinPoint.proceed();
    }

    void registerSynchronization(IdempotentSyncContext syncContext) {
        if (syncContext.isSynchronizationRegistered()) {
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
                    log.warn("Surrounding transaction rolled back after idempotent method completion for key='{}' in group='{}'. Releasing lock for retry.",
                            syncContext.getKey(), syncContext.getConsumerGroup());
                    try {
                        idempotentRepository.markFailed(
                                syncContext.getKey(),
                                syncContext.getConsumerGroup(),
                                "Surrounding transaction rolled back after completion",
                                syncContext.getLockedUntil()
                        );
                    } catch (Exception ex) {
                        log.warn("Failed to release idempotent lock after surrounding transaction rollback for key='{}' in group='{}': {}",
                                syncContext.getKey(), syncContext.getConsumerGroup(), ex.getMessage());
                    }
                }
            }
        });
    }
}
