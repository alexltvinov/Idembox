package io.github.alexlitvinov.idembox.starter.cleaner;

import io.github.alexlitvinov.idembox.core.repository.OutboxRepository;
import io.github.alexlitvinov.idembox.starter.config.IdemboxProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

import java.time.Duration;
import java.time.Instant;

/**
 * Housekeeping worker for removing sent and archived failed outbox messages according to configured retention periods.
 */
public class OutboxCleaner {

    private static final Logger log = LoggerFactory.getLogger(OutboxCleaner.class);

    private final OutboxRepository outboxRepository;
    private final IdemboxProperties properties;

    public OutboxCleaner(OutboxRepository outboxRepository, IdemboxProperties properties) {
        this.outboxRepository = outboxRepository;
        this.properties = properties;
    }

    @Scheduled(
            scheduler = "idemboxTaskScheduler",
            cron = "${idembox.outbox.cleanup-cron:0 0 * * * *}")
    public void cleanupOldMessages() {
        if (!properties.getOutbox().isEnabled() || !properties.getOutbox().getCleaner().isEnabled()) {
            log.debug("Outbox or outbox cleaner is disabled, skipping cleanup of old messages.");
            return;
        }
        outboxRepository.executeWithLock("idembox_outbox_cleanup", () -> {
            try {
                Duration retention = properties.getOutbox().getRetention();
                int batchSize = properties.getOutbox().getCleaner().getBatchSize();
                int maxBatches = properties.getOutbox().getCleaner().getMaxBatchesPerRun();
                Duration timeBudget = Duration.ofSeconds(30);

                if (retention != null && !retention.isNegative() && !retention.isZero()) {
                    Instant sentThreshold = Instant.now().minus(retention);
                    int deletedSent = outboxRepository.deleteSentBefore(sentThreshold, timeBudget, batchSize, maxBatches);
                    if (deletedSent > 0) {
                        log.info("Cleaned up {} sent outbox messages older than {}", deletedSent, sentThreshold);
                    }
                }

                Duration failedRetention = properties.getOutbox().getFailedRetention();
                if (failedRetention != null && !failedRetention.isNegative() && !failedRetention.isZero()) {
                    Instant failedThreshold = Instant.now().minus(failedRetention);
                    int deletedFailed = outboxRepository.deleteFailedBefore(failedThreshold, timeBudget, batchSize, maxBatches);
                    if (deletedFailed > 0) {
                        log.info("Cleaned up {} failed outbox messages older than {}", deletedFailed, failedThreshold);
                    }
                }
            } catch (Exception e) {
                log.error("Failed during outbox cleanup job", e);
            }
        });
    }
}
