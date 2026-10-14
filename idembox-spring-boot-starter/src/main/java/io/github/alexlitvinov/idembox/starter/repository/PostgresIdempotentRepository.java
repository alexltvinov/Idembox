package io.github.alexlitvinov.idembox.starter.repository;

import io.github.alexlitvinov.idembox.core.model.AcquireOutcome;
import io.github.alexlitvinov.idembox.core.model.AcquireResult;
import io.github.alexlitvinov.idembox.core.model.ProcessingState;
import io.github.alexlitvinov.idembox.core.model.ProcessingStatus;
import io.github.alexlitvinov.idembox.core.repository.IdempotentRepository;
import io.github.alexlitvinov.idembox.core.util.IdemboxUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class PostgresIdempotentRepository implements IdempotentRepository {

    private static final Logger log = LoggerFactory.getLogger(PostgresIdempotentRepository.class);

    public static final String DEFAULT_TABLE_NAME = "idempotent_records";

    private final JdbcTemplate jdbcTemplate;
    private final JdbcTemplate cleanupJdbcTemplate;
    private final String tableName;
    private final String quotedTableName;
    private final String quotedTargetTable;
    private final TransactionTemplate requiresNewTransactionTemplate;

    public PostgresIdempotentRepository(JdbcTemplate jdbcTemplate) {
        this(jdbcTemplate, (PlatformTransactionManager) null, DEFAULT_TABLE_NAME);
    }

    public PostgresIdempotentRepository(JdbcTemplate jdbcTemplate, String tableName) {
        this(jdbcTemplate, (PlatformTransactionManager) null, tableName);
    }

    public PostgresIdempotentRepository(JdbcTemplate jdbcTemplate, PlatformTransactionManager transactionManager) {
        this(jdbcTemplate, transactionManager, DEFAULT_TABLE_NAME);
    }

    public PostgresIdempotentRepository(JdbcTemplate jdbcTemplate, PlatformTransactionManager transactionManager, String tableName) {
        this(jdbcTemplate, createRequiresNewTransactionTemplate(transactionManager), tableName);
    }

    private static TransactionTemplate createRequiresNewTransactionTemplate(PlatformTransactionManager transactionManager) {
        if (transactionManager == null) {
            return null;
        }
        TransactionTemplate tt = new TransactionTemplate(transactionManager);
        tt.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        return tt;
    }

    public PostgresIdempotentRepository(JdbcTemplate jdbcTemplate, TransactionTemplate transactionTemplate) {
        this(jdbcTemplate, transactionTemplate, DEFAULT_TABLE_NAME);
    }

    public PostgresIdempotentRepository(JdbcTemplate jdbcTemplate, TransactionTemplate transactionTemplate, String tableName) {
        this.jdbcTemplate = jdbcTemplate;
        this.cleanupJdbcTemplate = (jdbcTemplate != null && jdbcTemplate.getDataSource() != null)
                ? new JdbcTemplate(jdbcTemplate.getDataSource())
                : jdbcTemplate;
        String cleanTable = (tableName != null && !tableName.isBlank()) ? tableName.trim() : DEFAULT_TABLE_NAME;
        this.tableName = io.github.alexlitvinov.idembox.starter.util.TableUtils.validateQualifiedTableName(cleanTable);
        this.quotedTableName = io.github.alexlitvinov.idembox.starter.util.TableUtils.quoteQualifiedTableName(cleanTable);
        String bareTable = cleanTable.contains(".") ? cleanTable.substring(cleanTable.lastIndexOf('.') + 1) : cleanTable;
        this.quotedTargetTable = io.github.alexlitvinov.idembox.starter.util.TableUtils.quoteIdentifier(bareTable);
        if (transactionTemplate != null) {
            if (transactionTemplate.getTransactionManager() != null) {
                TransactionTemplate tt = new TransactionTemplate(transactionTemplate.getTransactionManager());
                tt.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
                tt.setIsolationLevel(transactionTemplate.getIsolationLevel());
                tt.setReadOnly(false);
                this.requiresNewTransactionTemplate = tt;
            } else {
                this.requiresNewTransactionTemplate = transactionTemplate;
            }
        } else {
            this.requiresNewTransactionTemplate = null;
        }
    }

    public TransactionTemplate getRequiresNewTransactionTemplate() {
        return requiresNewTransactionTemplate;
    }

    public JdbcTemplate getJdbcTemplate() {
        return jdbcTemplate;
    }

    private void executeInRequiresNew(Runnable action) {
        if (requiresNewTransactionTemplate != null) {
            requiresNewTransactionTemplate.executeWithoutResult(status -> action.run());
        } else {
            action.run();
        }
    }

    private <T> T executeInRequiresNew(java.util.function.Supplier<T> action) {
        if (requiresNewTransactionTemplate != null) {
            return requiresNewTransactionTemplate.execute(status -> action.get());
        } else {
            return action.get();
        }
    }

    private <T> T executeInCurrentOrRequiresNew(java.util.function.Supplier<T> action) {
        if (TransactionSynchronizationManager.isActualTransactionActive()
                && !TransactionSynchronizationManager.isCurrentTransactionReadOnly()) {
            return action.get();
        }
        return executeInRequiresNew(action);
    }

    public String getTableName() {
        return tableName;
    }

    static String sanitizeText(String text) {
        return IdemboxUtils.sanitizeText(text);
    }

    static String normalizeEventKey(String eventKey) {
        String clean = sanitizeText(eventKey);
        if (clean == null || clean.isBlank()) {
            return clean;
        }
        byte[] bytes = clean.getBytes(StandardCharsets.UTF_8);
        if (bytes.length > 2000) {
            return "SHA256:" + IdemboxUtils.sha256Hex(bytes);
        }
        return clean;
    }

    private static OffsetDateTime toOffsetDateTime(Instant instant) {
        return IdemboxUtils.toOffsetDateTime(instant);
    }

    @Override
    public AcquireResult tryAcquire(
            String eventKey,
            String consumerGroup,
            Duration lockTimeout,
            Duration deduplicationTtl) {
        return tryAcquireWithLease(eventKey, consumerGroup, lockTimeout, deduplicationTtl).result();
    }

    @Override
    public AcquireOutcome tryAcquireWithLease(
            String eventKey,
            String consumerGroup,
            Duration lockTimeout,
            Duration deduplicationTtl) {
        return executeInRequiresNew(() -> doTryAcquireWithLease(eventKey, consumerGroup, lockTimeout, deduplicationTtl, 0));
    }

    private AcquireOutcome doTryAcquireWithLease(
            String eventKey,
            String consumerGroup,
            Duration lockTimeout,
            Duration deduplicationTtl,
            int attempt) {

        final String cleanEventKey = normalizeEventKey(eventKey);
        final String cleanConsumerGroup = sanitizeText(consumerGroup);
        long lockTimeoutMs = Math.max(lockTimeout.toMillis(), 1000);
        long ttlMs = Math.max(deduplicationTtl.toMillis(), 1000);

        String upsertSql = "INSERT INTO " + quotedTableName + " (" +
                " event_key, consumer_group, status, created_at, locked_until, expires_at" +
                ") VALUES (" +
                " ?, ?, 'PROCESSING', CURRENT_TIMESTAMP," +
                " CURRENT_TIMESTAMP + (? * INTERVAL '1 millisecond')," +
                " CURRENT_TIMESTAMP + (? * INTERVAL '1 millisecond')" +
                ")" +
                " ON CONFLICT (consumer_group, event_key) DO UPDATE" +
                " SET status = 'PROCESSING'," +
                " locked_until = EXCLUDED.locked_until," +
                " expires_at = EXCLUDED.expires_at," +
                " completed_at = NULL" +
                " WHERE (" +
                " " + quotedTargetTable + ".expires_at <= CURRENT_TIMESTAMP" +
                " OR (" + quotedTargetTable + ".status = 'PROCESSING' AND " + quotedTargetTable + ".locked_until <= CURRENT_TIMESTAMP)" +
                ")" +
                " RETURNING locked_until";

        List<Instant> lockedUntilList = jdbcTemplate.query(
                upsertSql,
                (rs, rowNum) -> {
                    OffsetDateTime odt = rs.getObject("locked_until", OffsetDateTime.class);
                    return odt != null ? odt.toInstant() : null;
                },
                cleanEventKey,
                cleanConsumerGroup,
                lockTimeoutMs,
                ttlMs
        );

        if (!lockedUntilList.isEmpty()) {
            return new AcquireOutcome(AcquireResult.ACQUIRED, lockedUntilList.get(0));
        }

        // Check existing state to differentiate between completed duplicate and in-flight lock
        // Locking validity is strictly evaluated by PostgreSQL CURRENT_TIMESTAMP to eliminate NTP clock skew
        String querySql = "SELECT status, (locked_until IS NOT NULL AND locked_until > CURRENT_TIMESTAMP) AS is_locked" +
                " FROM " + quotedTableName +
                " WHERE event_key = ? AND consumer_group = ?";

        try {
            record ExistingState(String status, boolean isLocked) {}
            ExistingState state = jdbcTemplate.queryForObject(querySql, (rs, rowNum) -> new ExistingState(
                    rs.getString("status"),
                    rs.getBoolean("is_locked")
            ), cleanEventKey, cleanConsumerGroup);

            if (state != null) {
                if ("COMPLETED".equalsIgnoreCase(state.status())) {
                    return new AcquireOutcome(AcquireResult.DUPLICATE_COMPLETED, null);
                }
                if ("FAILED".equalsIgnoreCase(state.status())) {
                    return new AcquireOutcome(AcquireResult.DUPLICATE_FAILED, null);
                }

                if (!state.isLocked()) {
                    // Senior Refinements Round 54: The existing lease on PROCESSING record has expired or was unlocked.
                    // Attempt immediate takeover without spinning in waitForInFlightCompletion.
                    String takeOverSql = "UPDATE " + quotedTableName +
                            " SET status = 'PROCESSING'," +
                            " locked_until = CURRENT_TIMESTAMP + (? * INTERVAL '1 millisecond')," +
                            " expires_at = CURRENT_TIMESTAMP + (? * INTERVAL '1 millisecond')," +
                            " completed_at = NULL" +
                            " WHERE event_key = ? AND consumer_group = ?" +
                            " AND status = 'PROCESSING'" +
                            " AND (locked_until IS NULL OR locked_until <= CURRENT_TIMESTAMP)" +
                            " RETURNING locked_until";
                    List<Instant> taken = jdbcTemplate.query(
                            takeOverSql,
                            (takeRs, rowNum2) -> {
                                OffsetDateTime odt = takeRs.getObject("locked_until", OffsetDateTime.class);
                                return odt != null ? odt.toInstant() : null;
                            },
                            lockTimeoutMs,
                            ttlMs,
                            cleanEventKey,
                            cleanConsumerGroup
                    );
                    if (!taken.isEmpty()) {
                        return new AcquireOutcome(AcquireResult.ACQUIRED, taken.get(0));
                    }
                    // Takeover missed: record was completed, failed, or locked by another worker in the race window.
                    ProcessingState freshState = getProcessingState(cleanEventKey, cleanConsumerGroup);
                    if (freshState != null) {
                        if (freshState.isCompleted()) {
                            return new AcquireOutcome(AcquireResult.DUPLICATE_COMPLETED, null);
                        }
                        if (freshState.status() == ProcessingStatus.FAILED) {
                            return new AcquireOutcome(AcquireResult.DUPLICATE_FAILED, null);
                        }
                    } else if (attempt < 2) {
                        // Senior Refinements Round 61: The previous worker deleted the record (e.g. markFailed deleted PROCESSING row).
                        // Record is physically gone from DB and not held by anyone. Immediately retry acquire afresh.
                        return doTryAcquireWithLease(eventKey, consumerGroup, lockTimeout, deduplicationTtl, attempt + 1);
                    }
                }
            }

            return new AcquireOutcome(AcquireResult.IN_FLIGHT_LOCKED, null);
        } catch (EmptyResultDataAccessException e) {
            // Concurrent delete happened right after upsert, retry acquire afresh
            if (attempt < 2) {
                return doTryAcquireWithLease(eventKey, consumerGroup, lockTimeout, deduplicationTtl, attempt + 1);
            }
            return new AcquireOutcome(AcquireResult.IN_FLIGHT_LOCKED, null);
        }
    }

    @Override
    public ProcessingState getProcessingState(String eventKey, String consumerGroup) {
        String cleanEventKey = normalizeEventKey(eventKey);
        String cleanConsumerGroup = sanitizeText(consumerGroup);
        String sql = "SELECT status, locked_until, expires_at," +
                " (locked_until IS NOT NULL AND locked_until > CURRENT_TIMESTAMP) AS is_locked" +
                " FROM " + quotedTableName +
                " WHERE event_key = ? AND consumer_group = ?";
        try {
            return jdbcTemplate.queryForObject(sql, (rs, rowNum) -> {
                String statusStr = rs.getString("status");
                ProcessingStatus status = ProcessingStatus.valueOf(statusStr);
                OffsetDateTime lockedUntilOdt = rs.getObject("locked_until", OffsetDateTime.class);
                Instant lockedUntil = lockedUntilOdt != null ? lockedUntilOdt.toInstant() : null;
                OffsetDateTime expiresAtOdt = rs.getObject("expires_at", OffsetDateTime.class);
                Instant expiresAt = expiresAtOdt != null ? expiresAtOdt.toInstant() : null;
                boolean isLocked = rs.getBoolean("is_locked");
                return new ProcessingState(status, lockedUntil, expiresAt, isLocked);
            }, cleanEventKey, cleanConsumerGroup);
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }

    @Override
    public boolean markCompleted(String eventKey, String consumerGroup, Duration deduplicationTtl) {
        String cleanEventKey = normalizeEventKey(eventKey);
        String cleanConsumerGroup = sanitizeText(consumerGroup);
        long ttlMs = Math.max(deduplicationTtl.toMillis(), 1000);
        String sql = "UPDATE " + quotedTableName +
                " SET status = 'COMPLETED'," +
                " completed_at = CURRENT_TIMESTAMP," +
                " locked_until = NULL," +
                " expires_at = CURRENT_TIMESTAMP + (? * INTERVAL '1 millisecond')" +
                " WHERE event_key = ? AND consumer_group = ? AND status = 'PROCESSING'";
        return Boolean.TRUE.equals(executeInCurrentOrRequiresNew(() -> {
            int affected = jdbcTemplate.update(sql, ttlMs, cleanEventKey, cleanConsumerGroup);
            return checkMarkCompletedAffected(affected, cleanEventKey, cleanConsumerGroup);
        }));
    }

    @Override
    public boolean markCompleted(String eventKey, String consumerGroup, Duration deduplicationTtl, Instant lockedUntil) {
        if (lockedUntil != null) {
            String cleanEventKey = normalizeEventKey(eventKey);
            String cleanConsumerGroup = sanitizeText(consumerGroup);
            long ttlMs = Math.max(deduplicationTtl.toMillis(), 1000);
            OffsetDateTime lockedUntilOdt = toOffsetDateTime(lockedUntil);
            String sql = "UPDATE " + quotedTableName +
                    " SET status = 'COMPLETED'," +
                    " completed_at = CURRENT_TIMESTAMP," +
                    " locked_until = NULL," +
                    " expires_at = CURRENT_TIMESTAMP + (? * INTERVAL '1 millisecond')" +
                    " WHERE event_key = ? AND consumer_group = ? AND status = 'PROCESSING'" +
                    " AND (locked_until = ? OR (locked_until >= ? - INTERVAL '1 microsecond' AND locked_until <= ? + INTERVAL '1 microsecond'))";
            return Boolean.TRUE.equals(executeInCurrentOrRequiresNew(() -> {
                int affected = jdbcTemplate.update(sql, ttlMs, cleanEventKey, cleanConsumerGroup, lockedUntilOdt, lockedUntilOdt, lockedUntilOdt);
                return checkMarkCompletedAffected(affected, cleanEventKey, cleanConsumerGroup);
            }));
        } else {
            return markCompleted(eventKey, consumerGroup, deduplicationTtl);
        }
    }

    private static boolean checkMarkCompletedAffected(int affected, String cleanEventKey, String cleanConsumerGroup) {
        if (affected == 0) {
            log.warn("Failed to mark COMPLETED: lock was stolen or record state changed for key='{}', consumerGroup='{}'",
                    cleanEventKey, cleanConsumerGroup);
            return false;
        }
        return true;
    }

    @Override
    public void markFailed(String eventKey, String consumerGroup, String reason) {
        String cleanEventKey = normalizeEventKey(eventKey);
        String cleanConsumerGroup = sanitizeText(consumerGroup);
        String sql = "DELETE FROM " + quotedTableName + " WHERE event_key = ? AND consumer_group = ? AND status = 'PROCESSING'";
        try {
            executeInRequiresNew(() -> jdbcTemplate.update(sql, cleanEventKey, cleanConsumerGroup));
        } catch (org.springframework.dao.DataAccessException exception) {
            logDeleteLockFailure(cleanEventKey, cleanConsumerGroup, exception);
        }
    }

    @Override
    public void markFailed(String eventKey, String consumerGroup, String reason, Instant lockedUntil) {
        if (lockedUntil != null) {
            String cleanEventKey = normalizeEventKey(eventKey);
            String cleanConsumerGroup = sanitizeText(consumerGroup);
            OffsetDateTime lockedUntilOdt = toOffsetDateTime(lockedUntil);
            String sql = "DELETE FROM " + quotedTableName +
                    " WHERE event_key = ? AND consumer_group = ? AND status = 'PROCESSING'" +
                    " AND (locked_until = ? OR (locked_until >= ? - INTERVAL '1 microsecond' AND locked_until <= ? + INTERVAL '1 microsecond'))";
            try {
                executeInRequiresNew(() -> {
                    int affected = jdbcTemplate.update(sql, cleanEventKey, cleanConsumerGroup, lockedUntilOdt, lockedUntilOdt, lockedUntilOdt);
                    if (affected == 0) {
                        log.warn("Failed to delete PROCESSING lock on failure: lock was stolen or record state changed for key='{}', consumerGroup='{}'",
                                cleanEventKey, cleanConsumerGroup);
                    }
                });
            } catch (org.springframework.dao.DataAccessException exception) {
                logDeleteLockFailure(cleanEventKey, cleanConsumerGroup, exception);
            }
        } else {
            markFailed(eventKey, consumerGroup, reason);
        }
    }

    private static void logDeleteLockFailure(
            String cleanEventKey,
            String cleanConsumerGroup,
            org.springframework.dao.DataAccessException exception) {
        log.warn("Failed to delete PROCESSING lock on failure for key='{}', consumerGroup='{}': {}. Lock will expire naturally.",
                cleanEventKey, cleanConsumerGroup, exception.getMessage());
    }

    @Override
    public void markTerminalFailed(String eventKey, String consumerGroup, String reason, Duration deduplicationTtl) {
        markTerminalFailed(eventKey, consumerGroup, reason, deduplicationTtl, null);
    }

    @Override
    public void markTerminalFailed(String eventKey, String consumerGroup, String reason, Duration deduplicationTtl, Instant lockedUntil) {
        try {
            executeInRequiresNew(() -> doMarkTerminalFailed(eventKey, consumerGroup, reason, deduplicationTtl, lockedUntil));
        } catch (org.springframework.dao.DataAccessException dae) {
            log.warn("Failed to mark terminal failure for key='{}', consumerGroup='{}': {}.",
                    eventKey, consumerGroup, dae.getMessage());
        }
    }

    private void doMarkTerminalFailed(String eventKey, String consumerGroup, String reason, Duration deduplicationTtl, Instant lockedUntil) {
        String cleanEventKey = normalizeEventKey(eventKey);
        String cleanConsumerGroup = sanitizeText(consumerGroup);
        log.warn("Marking idempotent record as terminally FAILED for key='{}', consumerGroup='{}'. Reason: {}",
                cleanEventKey, cleanConsumerGroup, reason != null ? reason : "unspecified");
        long ttlMs = Math.max(deduplicationTtl.toMillis(), 1000);
        String updateSql;
        int affected;
        if (lockedUntil != null) {
            OffsetDateTime lockedUntilOdt = toOffsetDateTime(lockedUntil);
            updateSql = "UPDATE " + quotedTableName +
                    " SET status = 'FAILED'," +
                    " completed_at = NULL," +
                    " locked_until = NULL," +
                    " expires_at = CURRENT_TIMESTAMP + (? * INTERVAL '1 millisecond')" +
                    " WHERE event_key = ? AND consumer_group = ? AND status <> 'COMPLETED'" +
                    " AND (locked_until = ? OR (locked_until >= ? - INTERVAL '1 microsecond' AND locked_until <= ? + INTERVAL '1 microsecond')" +
                    " OR locked_until IS NULL OR locked_until <= CURRENT_TIMESTAMP)";
            affected = jdbcTemplate.update(updateSql, ttlMs, cleanEventKey, cleanConsumerGroup, lockedUntilOdt, lockedUntilOdt, lockedUntilOdt);
            if (affected == 0) {
                String insertSql = "INSERT INTO " + quotedTableName + " (" +
                        " event_key, consumer_group, status, created_at, locked_until, expires_at" +
                        ") VALUES (" +
                        " ?, ?, 'FAILED', CURRENT_TIMESTAMP, NULL," +
                        " CURRENT_TIMESTAMP + (? * INTERVAL '1 millisecond')" +
                        ")" +
                        " ON CONFLICT (consumer_group, event_key) DO UPDATE" +
                        " SET status = 'FAILED'," +
                        " completed_at = NULL," +
                        " locked_until = NULL," +
                        " expires_at = EXCLUDED.expires_at" +
                        " WHERE " + quotedTargetTable + ".status <> 'COMPLETED'" +
                        " AND (" + quotedTargetTable + ".locked_until = ?" +
                        " OR (" + quotedTargetTable + ".locked_until >= ? - INTERVAL '1 microsecond' AND " + quotedTargetTable + ".locked_until <= ? + INTERVAL '1 microsecond')" +
                        " OR " + quotedTargetTable + ".locked_until IS NULL OR " + quotedTargetTable + ".locked_until <= CURRENT_TIMESTAMP)";
                jdbcTemplate.update(insertSql, cleanEventKey, cleanConsumerGroup, ttlMs, lockedUntilOdt, lockedUntilOdt, lockedUntilOdt);
            }
        } else {
            updateSql = "UPDATE " + quotedTableName +
                    " SET status = 'FAILED'," +
                    " completed_at = NULL," +
                    " locked_until = NULL," +
                    " expires_at = CURRENT_TIMESTAMP + (? * INTERVAL '1 millisecond')" +
                    " WHERE event_key = ? AND consumer_group = ? AND status <> 'COMPLETED'" +
                    " AND (locked_until IS NULL OR locked_until <= CURRENT_TIMESTAMP)";
            affected = jdbcTemplate.update(updateSql, ttlMs, cleanEventKey, cleanConsumerGroup);
            if (affected == 0) {
                String insertSql = "INSERT INTO " + quotedTableName + " (" +
                        " event_key, consumer_group, status, created_at, locked_until, expires_at" +
                        ") VALUES (" +
                        " ?, ?, 'FAILED', CURRENT_TIMESTAMP, NULL," +
                        " CURRENT_TIMESTAMP + (? * INTERVAL '1 millisecond')" +
                        ")" +
                        " ON CONFLICT (consumer_group, event_key) DO UPDATE" +
                        " SET status = 'FAILED'," +
                        " completed_at = NULL," +
                        " locked_until = NULL," +
                        " expires_at = EXCLUDED.expires_at" +
                        " WHERE " + quotedTargetTable + ".status <> 'COMPLETED'" +
                        " AND (" + quotedTargetTable + ".locked_until IS NULL OR " + quotedTargetTable + ".locked_until <= CURRENT_TIMESTAMP)";
                jdbcTemplate.update(insertSql, cleanEventKey, cleanConsumerGroup, ttlMs);
            }
        }
    }

    @Override
    public boolean resetFailed(String eventKey, String consumerGroup) {
        String cleanEventKey = normalizeEventKey(eventKey);
        String cleanConsumerGroup = sanitizeText(consumerGroup);
        boolean hasGroup = cleanConsumerGroup != null && !cleanConsumerGroup.isBlank();
        String sql = resolveResetFailedSql(hasGroup);
        try {
            return Boolean.TRUE.equals(executeInRequiresNew(() -> {
                int affected = hasGroup
                        ? jdbcTemplate.update(sql, cleanEventKey, cleanConsumerGroup)
                        : jdbcTemplate.update(sql, cleanEventKey);
                if (affected > 0) {
                    log.info("Reset FAILED idempotent record for replay: key='{}', consumerGroup='{}'", cleanEventKey, cleanConsumerGroup);
                    return true;
                }
                return false;
            }));
        } catch (org.springframework.dao.DataAccessException dae) {
            log.warn("Failed to reset FAILED idempotent record for key='{}', consumerGroup='{}': {}",
                    cleanEventKey, cleanConsumerGroup, dae.getMessage());
            return false;
        }
    }

    private String resolveResetFailedSql(boolean hasGroup) {
        String baseSql = "UPDATE " + quotedTableName +
                " SET status = 'PROCESSING'," +
                " locked_until = CURRENT_TIMESTAMP - INTERVAL '1 second'," +
                " expires_at = CURRENT_TIMESTAMP - INTERVAL '1 second'" +
                " WHERE event_key = ?";
        return hasGroup
                ? baseSql + " AND consumer_group = ? AND status = 'FAILED'"
                : baseSql + " AND status = 'FAILED'";
    }

    @Override
    public int cleanupExpired(Instant now) {
        return cleanupExpired(now, Duration.ofSeconds(30));
    }

    @Override
    public int cleanupExpired(Instant now, Duration timeBudget) {
        return cleanupExpired(now, timeBudget, 5000, 20);
    }

    /**
     * Cleans up expired idempotency records before {@code now} in bounded statement chunks.
     * <p>
     * <b>Transaction Boundary:</b> When executed inside {@link PostgresAdvisoryLockManager#executeWithLock},
     * all statement iterations run on the advisory lock connection and commit atomically as a single unit of work.
     * The {@code batchSize} limits memory and lock overhead within individual PostgreSQL SQL statements.
     */
    @Override
    public int cleanupExpired(Instant now, Duration timeBudget, int batchSize, int maxBatches) {
        int effectiveBatchSize = batchSize > 0 ? batchSize : 5000;
        int effectiveMaxBatches = maxBatches > 0 ? maxBatches : 100;
        String sql = buildCleanupExpiredSql(effectiveBatchSize);
        int totalDeleted = 0;
        int batches = 0;
        int affected;
        OffsetDateTime threshold = toOffsetDateTime(now);
        long budgetMs = (timeBudget != null && !timeBudget.isNegative() && !timeBudget.isZero())
                ? timeBudget.toMillis()
                : 30_000L;
        long deadline = System.currentTimeMillis() + budgetMs;
        int timeoutSeconds = (int) Math.max(1, Math.min(budgetMs / 1000, 60));

        Connection activeCon = PostgresAdvisoryLockManager.getActiveLockConnection();
        do {
            if (activeCon != null) {
                try (PreparedStatement ps = activeCon.prepareStatement(sql)) {
                    ps.setQueryTimeout(timeoutSeconds);
                    ps.setObject(1, threshold);
                    affected = ps.executeUpdate();
                } catch (Exception ex) {
                    throw new RuntimeException("Error executing idempotent cleanup on active lock connection: " + ex.getMessage(), ex);
                }
            } else {
                Integer batchAffected = executeInRequiresNew(() ->
                        cleanupJdbcTemplate.update(sql, ps -> {
                            ps.setQueryTimeout(timeoutSeconds);
                            ps.setObject(1, threshold);
                        }));
                affected = batchAffected != null ? batchAffected : 0;
            }
            totalDeleted += affected;
            batches++;
        } while (affected > 0 && batches < effectiveMaxBatches && System.currentTimeMillis() < deadline);
        return totalDeleted;
    }

    private String buildCleanupExpiredSql(int batchSize) {
        return String.format(
                "%s to_delete AS (%s consumer_group, event_key FROM %s WHERE expires_at < ? " +
                        "AND (status <> 'PROCESSING' OR (locked_until IS NOT NULL AND locked_until <= CURRENT_TIMESTAMP)) " +
                        "ORDER BY expires_at ASC, consumer_group ASC, event_key ASC LIMIT %d FOR UPDATE SKIP LOCKED) " +
                        "%s FROM %s r USING to_delete d WHERE r.consumer_group = d.consumer_group AND r.event_key = d.event_key",
                "WITH", "SELECT", quotedTableName, batchSize, "DELETE", quotedTableName
        );
    }

    @Override
    public boolean executeWithLock(String lockName, Runnable action) {
        return PostgresAdvisoryLockManager.executeWithLock(jdbcTemplate, tableName, lockName, action);
    }
}
