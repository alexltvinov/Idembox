package io.github.alexlitvinov.idembox.starter.cleaner;

import io.github.alexlitvinov.idembox.starter.config.IdemboxProperties;
import io.github.alexlitvinov.idembox.starter.poller.OutboxPoller;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.ApplicationContext;
import org.springframework.core.env.Environment;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.config.TaskManagementConfigUtils;
import org.springframework.scheduling.support.CronTrigger;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledFuture;

/**
 * Programmatically registers cleaner housekeeping tasks on {@code idemboxTaskScheduler}.
 * <p>
 * Ensures cleaner tasks run even when {@code @EnableScheduling} is not enabled globally on the host application,
 * avoiding leaking global scheduling into the host application while preventing duplicate task execution
 * if {@link org.springframework.scheduling.annotation.ScheduledAnnotationBeanPostProcessor} is already active.
 */
public class IdemboxCleanerTaskRegistrar implements SmartInitializingSingleton, DisposableBean {

    private static final Logger log = LoggerFactory.getLogger(IdemboxCleanerTaskRegistrar.class);

    private final TaskScheduler scheduler;
    private final ObjectProvider<OutboxCleaner> outboxCleanerProvider;
    private final ObjectProvider<IdempotencyCleaner> idempotencyCleanerProvider;
    private final ObjectProvider<OutboxPoller> outboxPollerProvider;
    private final IdemboxProperties properties;
    private final ApplicationContext applicationContext;
    private final Environment environment;
    private final List<ScheduledFuture<?>> scheduledTasks = new CopyOnWriteArrayList<>();

    public IdemboxCleanerTaskRegistrar(
            TaskScheduler scheduler,
            ObjectProvider<OutboxCleaner> outboxCleanerProvider,
            ObjectProvider<IdempotencyCleaner> idempotencyCleanerProvider,
            ObjectProvider<OutboxPoller> outboxPollerProvider,
            IdemboxProperties properties,
            ApplicationContext applicationContext,
            Environment environment) {
        this.scheduler = scheduler;
        this.outboxCleanerProvider = outboxCleanerProvider;
        this.idempotencyCleanerProvider = idempotencyCleanerProvider;
        this.outboxPollerProvider = outboxPollerProvider;
        this.properties = properties;
        this.applicationContext = applicationContext;
        this.environment = environment;
    }

    private static final String DEFAULT_CLEANUP_CRON = "0 0 * * * *";

    @Override
    public void afterSingletonsInstantiated() {
        boolean hasScheduledAnnotationProcessor = applicationContext != null &&
                applicationContext.containsBean(TaskManagementConfigUtils.SCHEDULED_ANNOTATION_PROCESSOR_BEAN_NAME);

        if (hasScheduledAnnotationProcessor) {
            log.debug("ScheduledAnnotationBeanPostProcessor detected in application context. Cleaner tasks will be scheduled via @Scheduled.");
            return;
        }

        if (scheduler == null) {
            log.warn("No ThreadPoolTaskScheduler available to programmatically schedule cleaner tasks.");
            return;
        }

        OutboxCleaner outboxCleaner = outboxCleanerProvider != null ? outboxCleanerProvider.getIfAvailable() : null;
        if (outboxCleaner != null && properties != null && properties.getOutbox().isEnabled() && properties.getOutbox().getCleaner().isEnabled()) {
            String cron = resolveCron(
                    properties.getOutbox().getCleanupCron(),
                    "idembox.outbox.cleanup-cron");
            try {
                ScheduledFuture<?> future = scheduler.schedule(outboxCleaner::cleanupOldMessages, new CronTrigger(cron));
                if (future != null) {
                    scheduledTasks.add(future);
                    log.info("Programmatically scheduled OutboxCleaner with cron '{}' on idemboxTaskScheduler", cron);
                }
            } catch (Exception ex) {
                log.error("Failed to programmatically schedule OutboxCleaner with cron '{}'", cron, ex);
            }
        }

        IdempotencyCleaner idempotencyCleaner = idempotencyCleanerProvider != null ? idempotencyCleanerProvider.getIfAvailable() : null;
        if (idempotencyCleaner != null && properties != null && properties.getIdempotency().isEnabled() && properties.getIdempotency().getCleaner().isEnabled()) {
            String cron = resolveCron(
                    properties.getIdempotency().getCleanupCron(),
                    "idembox.idempotency.cleanup-cron");
            try {
                ScheduledFuture<?> future = scheduler.schedule(idempotencyCleaner::cleanupExpiredRecords, new CronTrigger(cron));
                if (future != null) {
                    scheduledTasks.add(future);
                    log.info("Programmatically scheduled IdempotencyCleaner with cron '{}' on idemboxTaskScheduler", cron);
                }
            } catch (Exception ex) {
                log.error("Failed to programmatically schedule IdempotencyCleaner with cron '{}'", cron, ex);
            }
        }

        OutboxPoller outboxPoller = outboxPollerProvider != null ? outboxPollerProvider.getIfAvailable() : null;
        if (outboxPoller != null && properties != null && properties.getOutbox().isEnabled() && properties.getOutbox().getPoller().isEnabled()) {
            long rateMs = resolveRateMs(properties.getOutbox().getPoller().getRateMs());
            try {
                long initialDelayMs = java.util.concurrent.ThreadLocalRandom.current().nextLong(100, 1000);
                Instant startTime = Instant.now().plusMillis(initialDelayMs);
                ScheduledFuture<?> future = scheduler.scheduleWithFixedDelay(outboxPoller::schedulePoll, startTime, Duration.ofMillis(rateMs));
                scheduledTasks.add(future);
                log.info("Programmatically scheduled OutboxPoller with fixedDelay {}ms on idemboxTaskScheduler", rateMs);
            } catch (Exception ex) {
                log.error("Failed to programmatically schedule OutboxPoller with rate {}ms", rateMs, ex);
            }
        }
    }

    private long resolveRateMs(long defaultRate) {
        if (environment != null) {
            String prop1 = environment.getProperty("idembox.outbox.poller.rate-ms");
            if (prop1 != null && !prop1.isBlank()) {
                try {
                    return Long.parseLong(environment.resolvePlaceholders(prop1.trim()));
                } catch (NumberFormatException ignored) {}
            }
            String prop2 = environment.getProperty("idembox.outbox.poller-rate-ms");
            if (prop2 != null && !prop2.isBlank()) {
                try {
                    return Long.parseLong(environment.resolvePlaceholders(prop2.trim()));
                } catch (NumberFormatException ignored) {}
            }
        }
        return defaultRate > 0 ? defaultRate : 500L;
    }

    private String resolveCron(String propCron, String envKey) {
        if (environment != null) {
            String envVal = environment.getProperty(envKey);
            if (envVal != null && !envVal.isBlank()) {
                return environment.resolvePlaceholders(envVal.trim());
            }
        }
        if (propCron != null && !propCron.isBlank()) {
            return environment != null ? environment.resolvePlaceholders(propCron.trim()) : propCron.trim();
        }
        return DEFAULT_CLEANUP_CRON;
    }

    @Override
    public void destroy() {
        for (ScheduledFuture<?> future : scheduledTasks) {
            try {
                future.cancel(false);
            } catch (Exception ignored) {
            }
        }
        scheduledTasks.clear();
    }
}
