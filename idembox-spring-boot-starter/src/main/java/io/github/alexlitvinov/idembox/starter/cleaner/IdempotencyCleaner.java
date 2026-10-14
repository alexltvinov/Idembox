package io.github.alexlitvinov.idembox.starter.cleaner;

import io.github.alexlitvinov.idembox.core.repository.IdempotentRepository;
import io.github.alexlitvinov.idembox.starter.config.IdemboxProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

import java.time.Instant;

/**
 * Housekeeping worker for removing expired idempotency records according to configured cron schedule.
 */
public class IdempotencyCleaner {

    private static final Logger log = LoggerFactory.getLogger(IdempotencyCleaner.class);

    private final IdempotentRepository idempotentRepository;
    private final IdemboxProperties properties;

    public IdempotencyCleaner(IdempotentRepository idempotentRepository, IdemboxProperties properties) {
        this.idempotentRepository = idempotentRepository;
        this.properties = properties;
    }

    @Scheduled(
            scheduler = "idemboxTaskScheduler",
            cron = "${idembox.idempotency.cleanup-cron:0 0 * * * *}")
    public int cleanupExpiredRecords() {
        if (!properties.getIdempotency().isEnabled() || !properties.getIdempotency().getCleaner().isEnabled()) {
            return 0;
        }

        int[] countHolder = new int[1];
        idempotentRepository.executeWithLock("idembox_idempotency_cleanup", () -> {
            try {
                Instant now = Instant.now();
                java.time.Duration timeBudget = java.time.Duration.ofSeconds(30);
                int batchSize = properties.getIdempotency().getCleaner().getBatchSize();
                int maxBatches = properties.getIdempotency().getCleaner().getMaxBatchesPerRun();
                int deletedCount = idempotentRepository.cleanupExpired(now, timeBudget, batchSize, maxBatches);
                if (deletedCount > 0) {
                    log.info("Cleaned up {} expired idempotency records older than {}", deletedCount, now);
                }
                countHolder[0] = deletedCount;
            } catch (Exception e) {
                log.error("Failed during scheduled idempotency table cleanup", e);
            }
        });
        return countHolder[0];
    }
}
