package io.github.alexlitvinov.idembox.starter.repository;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.alexlitvinov.idembox.core.model.OutboxMessage;
import io.github.alexlitvinov.idembox.core.model.OutboxStatus;
import io.github.alexlitvinov.idembox.core.repository.OutboxRepository;
import io.github.alexlitvinov.idembox.core.util.IdemboxUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class PostgresOutboxRepository implements OutboxRepository {

    private static final Logger log = LoggerFactory.getLogger(PostgresOutboxRepository.class);
    public static final String DEFAULT_TABLE_NAME = "outbox_messages";
    private static final int MAX_CLAIM_ITERATIONS = 5;

    private final JdbcTemplate jdbcTemplate;
    private final JdbcTemplate cleanupJdbcTemplate;
    private final ObjectMapper objectMapper;
    private final OutboxRowMapper rowMapper;
    private final TransactionTemplate claimTransactionTemplate;
    private final TransactionTemplate lockTransactionTemplate;
    private final io.github.alexlitvinov.idembox.starter.metrics.IdemboxMetrics metrics;
    private final String tableName;
    private final String quotedTableName;

    public PostgresOutboxRepository(JdbcTemplate jdbcTemplate, ObjectMapper objectMapper) {
        this(jdbcTemplate, objectMapper, (PlatformTransactionManager) null, io.github.alexlitvinov.idembox.starter.metrics.IdemboxMetrics.noOp());
    }

    public PostgresOutboxRepository(JdbcTemplate jdbcTemplate, ObjectMapper objectMapper, String tableName) {
        this(jdbcTemplate, objectMapper, null, io.github.alexlitvinov.idembox.starter.metrics.IdemboxMetrics.noOp(), tableName);
    }

    public PostgresOutboxRepository(JdbcTemplate jdbcTemplate, ObjectMapper objectMapper, TransactionTemplate transactionTemplate) {
        this(jdbcTemplate, objectMapper, transactionTemplate, io.github.alexlitvinov.idembox.starter.metrics.IdemboxMetrics.noOp());
    }

    public PostgresOutboxRepository(
            JdbcTemplate jdbcTemplate,
            ObjectMapper objectMapper,
            TransactionTemplate transactionTemplate,
            io.github.alexlitvinov.idembox.starter.metrics.IdemboxMetrics metrics) {
        this(jdbcTemplate, objectMapper, transactionTemplate, transactionTemplate != null ? copyWithoutTimeout(transactionTemplate) : null, metrics);
    }

    public PostgresOutboxRepository(
            JdbcTemplate jdbcTemplate,
            ObjectMapper objectMapper,
            TransactionTemplate claimTransactionTemplate,
            TransactionTemplate lockTransactionTemplate,
            io.github.alexlitvinov.idembox.starter.metrics.IdemboxMetrics metrics) {
        this(jdbcTemplate, objectMapper, claimTransactionTemplate, lockTransactionTemplate, metrics, DEFAULT_TABLE_NAME);
    }

    public PostgresOutboxRepository(
            JdbcTemplate jdbcTemplate,
            ObjectMapper objectMapper,
            PlatformTransactionManager transactionManager) {
        this(jdbcTemplate, objectMapper, transactionManager, io.github.alexlitvinov.idembox.starter.metrics.IdemboxMetrics.noOp(), DEFAULT_TABLE_NAME);
    }

    public PostgresOutboxRepository(
            JdbcTemplate jdbcTemplate,
            ObjectMapper objectMapper,
            PlatformTransactionManager transactionManager,
            io.github.alexlitvinov.idembox.starter.metrics.IdemboxMetrics metrics) {
        this(jdbcTemplate, objectMapper, transactionManager, metrics, DEFAULT_TABLE_NAME);
    }

    public PostgresOutboxRepository(
            JdbcTemplate jdbcTemplate,
            ObjectMapper objectMapper,
            PlatformTransactionManager transactionManager,
            io.github.alexlitvinov.idembox.starter.metrics.IdemboxMetrics metrics,
            String tableName) {
        this(jdbcTemplate, objectMapper,
                createClaimTransactionTemplate(transactionManager),
                createLockTransactionTemplate(transactionManager),
                metrics,
                tableName);
    }

    private static TransactionTemplate createClaimTransactionTemplate(PlatformTransactionManager transactionManager) {
        if (transactionManager == null) {
            return null;
        }
        TransactionTemplate tt = new TransactionTemplate(transactionManager);
        tt.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        tt.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
        tt.setTimeout(5);
        return tt;
    }

    private static TransactionTemplate createLockTransactionTemplate(PlatformTransactionManager transactionManager) {
        if (transactionManager == null) {
            return null;
        }
        TransactionTemplate tt = new TransactionTemplate(transactionManager);
        tt.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        tt.setIsolationLevel(TransactionDefinition.ISOLATION_READ_COMMITTED);
        return tt;
    }

    public PostgresOutboxRepository(
            JdbcTemplate jdbcTemplate,
            ObjectMapper objectMapper,
            TransactionTemplate claimTransactionTemplate,
            TransactionTemplate lockTransactionTemplate,
            io.github.alexlitvinov.idembox.starter.metrics.IdemboxMetrics metrics,
            String tableName) {
        this.jdbcTemplate = jdbcTemplate;
        this.cleanupJdbcTemplate = (jdbcTemplate != null && jdbcTemplate.getDataSource() != null)
                ? new JdbcTemplate(jdbcTemplate.getDataSource())
                : jdbcTemplate;
        this.objectMapper = objectMapper;
        this.rowMapper = new OutboxRowMapper(objectMapper);
        TransactionTemplate resolvedClaimTt = copyWithRequiresNew(claimTransactionTemplate);
        TransactionTemplate resolvedLockTt = copyWithoutTimeout(lockTransactionTemplate);
        if (resolvedClaimTt != null && resolvedLockTt == null) {
            resolvedLockTt = copyWithoutTimeout(resolvedClaimTt);
        } else if (resolvedClaimTt == null && resolvedLockTt != null) {
            if (resolvedLockTt.getTransactionManager() != null) {
                TransactionTemplate claimTt = new TransactionTemplate(resolvedLockTt.getTransactionManager());
                claimTt.setPropagationBehavior(org.springframework.transaction.TransactionDefinition.PROPAGATION_REQUIRES_NEW);
                claimTt.setIsolationLevel(org.springframework.transaction.TransactionDefinition.ISOLATION_READ_COMMITTED);
                claimTt.setReadOnly(false);
                claimTt.setTimeout(5);
                resolvedClaimTt = claimTt;
            } else {
                resolvedClaimTt = resolvedLockTt;
            }
        }
        this.claimTransactionTemplate = resolvedClaimTt;
        this.lockTransactionTemplate = resolvedLockTt;
        this.metrics = metrics != null ? metrics : io.github.alexlitvinov.idembox.starter.metrics.IdemboxMetrics.noOp();
        String cleanTable = (tableName != null && !tableName.isBlank()) ? tableName.trim() : DEFAULT_TABLE_NAME;
        this.tableName = io.github.alexlitvinov.idembox.starter.util.TableUtils.validateQualifiedTableName(cleanTable);
        this.quotedTableName = io.github.alexlitvinov.idembox.starter.util.TableUtils.quoteQualifiedTableName(cleanTable);
    }

    public String getTableName() {
        return tableName;
    }

    public JdbcTemplate getJdbcTemplate() {
        return jdbcTemplate;
    }

    private static TransactionTemplate copyWithRequiresNew(TransactionTemplate source) {
        if (source == null) {
            return null;
        }
        source.setPropagationBehavior(org.springframework.transaction.TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        source.setReadOnly(false);
        if (source.getTransactionManager() == null) {
            return source;
        }
        TransactionTemplate copy = new TransactionTemplate(source.getTransactionManager());
        copy.setPropagationBehavior(org.springframework.transaction.TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        copy.setIsolationLevel(source.getIsolationLevel());
        copy.setTimeout(source.getTimeout());
        copy.setReadOnly(false);
        return copy;
    }

    private static TransactionTemplate copyWithoutTimeout(TransactionTemplate source) {
        if (source == null) {
            return null;
        }
        source.setPropagationBehavior(org.springframework.transaction.TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        source.setReadOnly(false);
        if (source.getTransactionManager() == null) {
            return source;
        }
        TransactionTemplate copy = new TransactionTemplate(source.getTransactionManager());
        copy.setPropagationBehavior(org.springframework.transaction.TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        copy.setIsolationLevel(source.getIsolationLevel());
        copy.setReadOnly(false);
        // Default timeout (-1), so large maintenance and cleanup batches do not abort prematurely after 5 seconds
        return copy;
    }

    public TransactionTemplate getTransactionTemplate() {
        return claimTransactionTemplate != null ? claimTransactionTemplate : lockTransactionTemplate;
    }

    public TransactionTemplate getLockTransactionTemplate() {
        return lockTransactionTemplate;
    }

    private static OffsetDateTime toOffsetDateTime(Instant instant) {
        return IdemboxUtils.toOffsetDateTime(instant);
    }

    static String sanitizeJsonb(String value) {
        return IdemboxUtils.sanitizeText(value);
    }

    static String sanitizeText(String value) {
        return IdemboxUtils.sanitizeText(value);
    }

    @Override
    public void save(OutboxMessage message) {
        String sql = "INSERT INTO " + quotedTableName + " (" +
                " id, topic, partition_key, payload, headers, status, retry_count, created_at, sent_at, failed_at, next_retry_at, last_error" +
                ") VALUES (?, ?, ?, ?::jsonb, ?::jsonb, ?, ?, ?, ?, ?, ?, ?)";

        String headersJson = serializeHeaders(message.getMultiHeaders());

        jdbcTemplate.update(
                sql,
                message.getId(),
                sanitizeText(message.getTopic()),
                sanitizeText(message.getPartitionKey()),
                sanitizeJsonb(message.getPayload()),
                sanitizeJsonb(headersJson),
                message.getStatus().name(),
                message.getRetryCount(),
                toOffsetDateTime(message.getCreatedAt()),
                toOffsetDateTime(message.getSentAt()),
                toOffsetDateTime(message.getFailedAt()),
                toOffsetDateTime(message.getNextRetryAt()),
                sanitizeText(message.getLastError())
        );
        invalidatePendingCountCache();
    }

    @Override
    public List<OutboxMessage> claimBatchForProcessing(int batchSize, Duration lockDuration) {
        return claimBatchForProcessing(batchSize, lockDuration, 5);
    }

    /**
     * Claims a batch of messages for outbox dispatch with strict FIFO per topic and partition key.
     * <p>
     * <b>Partition Ordering on Failure Trade-off:</b><br>
     * The FIFO subquery ensures that for any given {@code (topic, partition_key)}, a message is claimed only
     * if there is no earlier message where {@code earlier.status IN ('PENDING', 'IN_PROGRESS')}.
     * <ul>
     *   <li><b>Design Rationale:</b> If a poison message permanently fails (transitions to status {@code FAILED}),
     *       subsequent messages for the same partition key are intentionally unblocked rather than causing
     *       an infinite Head-of-Line (HoL) deadlock for the entire partition key.</li>
     *   <li><b>Architectural Trade-off:</b> Subsequent messages for that partition key will be published out-of-order
     *       relative to the skipped failed message. If a business domain mandates absolute aggregate halt
     *       on failure (strict linearizability where no subsequent state change may be broadcast after a failure),
     *       dead-letter monitoring and operational alerting must be integrated, or maxRetries tuned accordingly.</li>
     * </ul>
     */
    @Override
    public List<OutboxMessage> claimBatchForProcessing(int batchSize, Duration lockDuration, int maxRetries) {
        return claimBatchForProcessing(batchSize, lockDuration, maxRetries, Duration.ofHours(24));
    }

    @Override
    public List<OutboxMessage> claimBatchForProcessing(int batchSize, Duration lockDuration, int maxRetries, Duration maxOutageDuration) {
        TransactionTemplate tx = claimTransactionTemplate != null ? claimTransactionTemplate : lockTransactionTemplate;
        if (tx != null) {
            return tx.execute(status -> doClaimBatch(batchSize, lockDuration, maxRetries, maxOutageDuration));
        }
        return doClaimBatch(batchSize, lockDuration, maxRetries, maxOutageDuration);
    }

    private List<OutboxMessage> doClaimBatch(int batchSize, Duration lockDuration, int maxRetries, Duration maxOutageDuration) {
        long lockMs = Math.max(lockDuration.toMillis(), 1000);
        long outageMs;
        if (maxOutageDuration == null) {
            outageMs = 86400000L; // default 24h
        } else if (maxOutageDuration.isZero() || maxOutageDuration.isNegative()) {
            outageMs = 3153600000000L; // Disabled: 100 years sentinel so earlier messages always block
        } else {
            outageMs = Math.max(maxOutageDuration.toMillis(), 1000);
        }

        String sql = "WITH candidates AS (" +
                " SELECT m.id" +
                " FROM " + quotedTableName + " m" +
                " WHERE (" +
                " (m.status = 'PENDING' AND (m.next_retry_at IS NULL OR m.next_retry_at <= CURRENT_TIMESTAMP))" +
                " OR (m.status = 'IN_PROGRESS' AND m.next_retry_at <= CURRENT_TIMESTAMP)" +
                " )" +
                " AND (m.partition_key IS NULL OR m.partition_key = '' OR NOT EXISTS (" +
                " SELECT 1 FROM " + quotedTableName + " earlier" +
                " WHERE earlier.topic = m.topic" +
                " AND earlier.partition_key = m.partition_key" +
                " AND earlier.id <> m.id" +
                " AND (" +
                " earlier.created_at < m.created_at" +
                " OR (earlier.created_at = m.created_at AND earlier.id < m.id)" +
                " )" +
                " AND earlier.status IN ('PENDING', 'IN_PROGRESS')" +
                " AND NOT (earlier.created_at <= CURRENT_TIMESTAMP - (? * INTERVAL '1 millisecond'))" +
                " ))" +
                " ORDER BY m.created_at ASC, m.id ASC" +
                " LIMIT ?" +
                " FOR UPDATE SKIP LOCKED" +
                ")" +
                " UPDATE " + quotedTableName +
                " SET status = CASE WHEN (status = 'IN_PROGRESS' AND retry_count + 1 >= ?) THEN 'FAILED' ELSE 'IN_PROGRESS' END," +
                " retry_count = CASE WHEN status = 'IN_PROGRESS' THEN retry_count + 1 ELSE retry_count END," +
                " failed_at = CASE WHEN (status = 'IN_PROGRESS' AND retry_count + 1 >= ?) THEN CURRENT_TIMESTAMP ELSE failed_at END," +
                " next_retry_at = CASE WHEN (status = 'IN_PROGRESS' AND retry_count + 1 >= ?) THEN NULL ELSE CURRENT_TIMESTAMP + (? * INTERVAL '1 millisecond') END," +
                " last_error = CASE WHEN status = 'IN_PROGRESS' THEN 'Worker lease expired / ungraceful shutdown' ELSE last_error END" +
                " WHERE id IN (SELECT id FROM candidates)" +
                " RETURNING id, topic, partition_key, payload, headers, status, retry_count, created_at, sent_at, next_retry_at, last_error, failed_at";

        List<OutboxMessage> activeMessages = new ArrayList<>();
        int remainingNeeded = batchSize;
        int iterations = 0;
        while (remainingNeeded > 0 && iterations++ < MAX_CLAIM_ITERATIONS) {
            List<OutboxMessage> batch = jdbcTemplate.query(sql, rowMapper, outageMs, remainingNeeded, maxRetries, maxRetries, maxRetries, lockMs);
            if (batch.isEmpty()) {
                break;
            }
            int fetchedCount = batch.size();
            for (OutboxMessage m : batch) {
                if (m.getStatus() == OutboxStatus.FAILED) {
                    log.error("Outbox message id={} topic={} partitionKey={} permanently failed due to worker crash / lease expiration (exceeded maxRetries={}). Transitioned directly to FAILED.",
                            m.getId(), m.getTopic(), m.getPartitionKey(), maxRetries);
                    metrics.recordOutboxTerminalFailure(m.getTopic());
                } else {
                    activeMessages.add(m);
                }
            }
            if (fetchedCount < remainingNeeded) {
                break;
            }
            remainingNeeded = batchSize - activeMessages.size();
        }
        if (remainingNeeded > 0 && iterations >= MAX_CLAIM_ITERATIONS && !activeMessages.isEmpty()) {
            log.warn("Reached maximum claim iterations ({}) during doClaimBatch. Fetched {} active messages out of {} requested.",
                    MAX_CLAIM_ITERATIONS, activeMessages.size(), batchSize);
        }
        activeMessages.sort(Comparator.comparing(OutboxMessage::getCreatedAt).thenComparing(OutboxMessage::getId));
        return activeMessages;
    }

    @Override
    public void releaseClaims(List<UUID> messageIds) {
        releaseClaims(messageIds, Duration.ZERO);
    }

    private void executeInRequiresNew(Runnable action) {
        TransactionTemplate tt = lockTransactionTemplate != null ? lockTransactionTemplate : claimTransactionTemplate;
        if (tt != null) {
            tt.executeWithoutResult(status -> action.run());
        } else {
            action.run();
        }
    }

    private <T> T executeInRequiresNew(java.util.function.Supplier<T> action) {
        TransactionTemplate tt = lockTransactionTemplate != null ? lockTransactionTemplate : claimTransactionTemplate;
        if (tt != null) {
            return tt.execute(status -> action.get());
        } else {
            return action.get();
        }
    }

    @Override
    public void releaseClaims(List<UUID> messageIds, Duration deferral) {
        releaseClaims(messageIds, deferral, null);
    }

    @Override
    public void releaseClaims(List<UUID> messageIds, Duration deferral, Instant lockedUntil) {
        if (messageIds == null || messageIds.isEmpty()) {
            return;
        }
        UUID[] idArray = messageIds.toArray(new UUID[0]);
        long deferralMs = (deferral != null && !deferral.isNegative()) ? deferral.toMillis() : 0L;
        executeInRequiresNew(() -> {
            String sql;
            if (lockedUntil != null) {
                OffsetDateTime lockedUntilOdt = toOffsetDateTime(lockedUntil);
                sql = "UPDATE " + quotedTableName +
                        " SET status = 'PENDING'," +
                        " next_retry_at = GREATEST(COALESCE(next_retry_at, CURRENT_TIMESTAMP), CURRENT_TIMESTAMP + (? * INTERVAL '1 millisecond'))" +
                        " WHERE status = 'IN_PROGRESS' AND id = ANY(?::uuid[])" +
                        " AND (next_retry_at = ? OR (next_retry_at >= ? - INTERVAL '1 microsecond' AND next_retry_at <= ? + INTERVAL '1 microsecond'))";
                jdbcTemplate.update(sql, deferralMs, idArray, lockedUntilOdt, lockedUntilOdt, lockedUntilOdt);
            } else {
                sql = "UPDATE " + quotedTableName +
                        " SET status = 'PENDING'," +
                        " next_retry_at = GREATEST(COALESCE(next_retry_at, CURRENT_TIMESTAMP), CURRENT_TIMESTAMP + (? * INTERVAL '1 millisecond'))" +
                        " WHERE status = 'IN_PROGRESS' AND id = ANY(?::uuid[])";
                jdbcTemplate.update(sql, deferralMs, idArray);
            }
        });
    }

    @Override
    public void markAsSent(UUID id, Instant sentAt) {
        markAsSent(id, sentAt, null);
    }

    @Override
    public boolean markAsSent(UUID id, Instant sentAt, Instant lockedUntil) {
        return Boolean.TRUE.equals(executeInRequiresNew(() -> {
            String sql;
            int affected;
            if (lockedUntil != null) {
                OffsetDateTime lockedUntilOdt = toOffsetDateTime(lockedUntil);
                sql = "UPDATE " + quotedTableName +
                        " SET status = 'SENT'," +
                        " sent_at = ?," +
                        " next_retry_at = NULL" +
                        " WHERE id = ? AND status = 'IN_PROGRESS'" +
                        " AND (next_retry_at = ? OR (next_retry_at >= ? - INTERVAL '1 microsecond' AND next_retry_at <= ? + INTERVAL '1 microsecond'))";
                affected = jdbcTemplate.update(sql, toOffsetDateTime(sentAt), id, lockedUntilOdt, lockedUntilOdt, lockedUntilOdt);
            } else {
                sql = "UPDATE " + quotedTableName +
                        " SET status = 'SENT'," +
                        " sent_at = ?," +
                        " next_retry_at = NULL" +
                        " WHERE id = ? AND status IN ('PENDING', 'IN_PROGRESS')";
                affected = jdbcTemplate.update(sql, toOffsetDateTime(sentAt), id);
            }
            if (affected == 0) {
                log.warn("Failed to mark outbox message id={} as SENT: lease expired or message was claimed/updated by another worker", id);
                return false;
            }
            return true;
        }));
    }

    @Override
    public void forceMarkAsSent(UUID id, Instant sentAt) {
        executeInRequiresNew(() -> {
            String sql = "UPDATE " + quotedTableName +
                    " SET status = 'SENT'," +
                    " sent_at = ?," +
                    " next_retry_at = NULL" +
                    " WHERE id = ? AND status IN ('PENDING', 'IN_PROGRESS')";
            jdbcTemplate.update(sql, toOffsetDateTime(sentAt), id);
        });
    }

    @Override
    public void markAsFailed(UUID id, String errorReason, int maxRetries, Duration backoff) {
        markAsFailed(id, errorReason, maxRetries, backoff, null);
    }

    @Override
    public boolean markAsFailed(UUID id, String errorReason, int maxRetries, Duration backoff, Instant lockedUntil) {
        return rescheduleMessage(id, errorReason, backoff, maxRetries, lockedUntil);
    }

    @Override
    public void rescheduleMessage(UUID id, String errorReason, Duration backoff) {
        rescheduleMessage(id, errorReason, backoff, null);
    }

    @Override
    public boolean rescheduleMessage(UUID id, String errorReason, Duration backoff, Instant lockedUntil) {
        long backoffMs = Math.max(backoff.toMillis(), 100);
        return Boolean.TRUE.equals(executeInRequiresNew(() -> {
            String sql;
            int affected;
            if (lockedUntil != null) {
                OffsetDateTime lockedUntilOdt = toOffsetDateTime(lockedUntil);
                sql = "UPDATE " + quotedTableName +
                        " SET status = 'PENDING'," +
                        " next_retry_at = CURRENT_TIMESTAMP + (? * INTERVAL '1 millisecond')," +
                        " last_error = ?" +
                        " WHERE id = ? AND status = 'IN_PROGRESS'" +
                        " AND (next_retry_at = ? OR (next_retry_at >= ? - INTERVAL '1 microsecond' AND next_retry_at <= ? + INTERVAL '1 microsecond'))";
                affected = jdbcTemplate.update(sql, backoffMs, sanitizeText(errorReason), id, lockedUntilOdt, lockedUntilOdt, lockedUntilOdt);
            } else {
                sql = "UPDATE " + quotedTableName +
                        " SET status = 'PENDING'," +
                        " next_retry_at = CURRENT_TIMESTAMP + (? * INTERVAL '1 millisecond')," +
                        " last_error = ?" +
                        " WHERE id = ? AND status IN ('PENDING', 'IN_PROGRESS')";
                affected = jdbcTemplate.update(sql, backoffMs, sanitizeText(errorReason), id);
            }
            return checkRescheduleAffected(affected, id);
        }));
    }

    @Override
    public void rescheduleMessage(UUID id, String errorReason, Duration backoff, int maxRetries) {
        rescheduleMessage(id, errorReason, backoff, maxRetries, null);
    }

    @Override
    public boolean rescheduleMessage(UUID id, String errorReason, Duration backoff, int maxRetries, Instant lockedUntil) {
        long backoffMs = Math.max(backoff.toMillis(), 100);
        return Boolean.TRUE.equals(executeInRequiresNew(() -> {
            String sql;
            int affected;
            if (lockedUntil != null) {
                OffsetDateTime lockedUntilOdt = toOffsetDateTime(lockedUntil);
                sql = "UPDATE " + quotedTableName +
                        " SET retry_count = retry_count + 1," +
                        " status = CASE WHEN (retry_count + 1) >= ? THEN 'FAILED' ELSE 'PENDING' END," +
                        " failed_at = CASE WHEN (retry_count + 1) >= ? THEN CURRENT_TIMESTAMP ELSE failed_at END," +
                        " next_retry_at = CASE WHEN (retry_count + 1) >= ? THEN NULL ELSE CURRENT_TIMESTAMP + (? * INTERVAL '1 millisecond') END," +
                        " last_error = ?" +
                        " WHERE id = ? AND status = 'IN_PROGRESS'" +
                        " AND (next_retry_at = ? OR (next_retry_at >= ? - INTERVAL '1 microsecond' AND next_retry_at <= ? + INTERVAL '1 microsecond'))";
                affected = jdbcTemplate.update(sql, maxRetries, maxRetries, maxRetries, backoffMs, sanitizeText(errorReason), id, lockedUntilOdt, lockedUntilOdt, lockedUntilOdt);
            } else {
                sql = "UPDATE " + quotedTableName +
                        " SET retry_count = retry_count + 1," +
                        " status = CASE WHEN (retry_count + 1) >= ? THEN 'FAILED' ELSE 'PENDING' END," +
                        " failed_at = CASE WHEN (retry_count + 1) >= ? THEN CURRENT_TIMESTAMP ELSE failed_at END," +
                        " next_retry_at = CASE WHEN (retry_count + 1) >= ? THEN NULL ELSE CURRENT_TIMESTAMP + (? * INTERVAL '1 millisecond') END," +
                        " last_error = ?" +
                        " WHERE id = ? AND status IN ('PENDING', 'IN_PROGRESS')";
                affected = jdbcTemplate.update(sql, maxRetries, maxRetries, maxRetries, backoffMs, sanitizeText(errorReason), id);
            }
            return checkRescheduleAffected(affected, id);
        }));
    }

    private static boolean checkRescheduleAffected(int affected, UUID id) {
        if (affected == 0) {
            log.warn("Failed to reschedule outbox message id={}: lease expired or message was claimed/updated by another worker", id);
            return false;
        }
        return true;
    }

    @Override
    public int deleteSentBefore(Instant threshold) {
        return deleteSentBefore(threshold, Duration.ofSeconds(30));
    }

    @Override
    public int deleteSentBefore(Instant threshold, Duration timeBudget) {
        return deleteSentBefore(threshold, timeBudget, 5000, 20);
    }

    /**
     * Purges sent outbox messages created before {@code threshold} in bounded statement chunks.
     * <p>
     * <b>Transaction Boundary:</b> When executed inside {@link PostgresAdvisoryLockManager#executeWithLock},
     * all statement iterations run on the advisory lock connection and commit atomically as a single unit of work.
     * The {@code batchSize} limits memory and lock overhead within individual PostgreSQL SQL statements.
     */
    @Override
    public int deleteSentBefore(Instant threshold, Duration timeBudget, int batchSize, int maxBatches) {
        int effectiveBatchSize = batchSize > 0 ? batchSize : 5000;
        int effectiveMaxBatches = maxBatches > 0 ? maxBatches : 100;
        String sql = buildBatchDeleteSql("SENT", "sent_at", effectiveBatchSize);
        int totalDeleted = 0;
        int batches = 0;
        int affected;
        OffsetDateTime param = toOffsetDateTime(threshold);
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
                    ps.setObject(1, param);
                    affected = ps.executeUpdate();
                } catch (Exception ex) {
                    throw new RuntimeException("Error executing sent cleanup on active lock connection: " + ex.getMessage(), ex);
                }
            } else if (lockTransactionTemplate != null) {
                Integer batchAffected = lockTransactionTemplate.execute(status ->
                        cleanupJdbcTemplate.update(sql, ps -> {
                            ps.setQueryTimeout(timeoutSeconds);
                            ps.setObject(1, param);
                        }));
                affected = batchAffected != null ? batchAffected : 0;
            } else {
                affected = cleanupJdbcTemplate.update(sql, ps -> {
                    ps.setQueryTimeout(timeoutSeconds);
                    ps.setObject(1, param);
                });
            }
            totalDeleted += affected;
            batches++;
        } while (affected > 0 && batches < effectiveMaxBatches && System.currentTimeMillis() < deadline);
        if (batches >= effectiveMaxBatches && affected > 0) {
            log.warn("Reached maximum batch limit ({} batches, {} records) during sent outbox cleanup. More expired records may remain.",
                    effectiveMaxBatches, totalDeleted);
        }
        return totalDeleted;
    }

    @Override
    public int deleteFailedBefore(Instant threshold) {
        return deleteFailedBefore(threshold, Duration.ofSeconds(30));
    }

    @Override
    public int deleteFailedBefore(Instant threshold, Duration timeBudget) {
        return deleteFailedBefore(threshold, timeBudget, 5000, 20);
    }

    @Override
    public int deleteFailedBefore(Instant threshold, Duration timeBudget, int batchSize, int maxBatches) {
        int effectiveBatchSize = batchSize > 0 ? batchSize : 5000;
        int effectiveMaxBatches = maxBatches > 0 ? maxBatches : 100;
        String sql = buildBatchDeleteSql("FAILED", "failed_at", effectiveBatchSize);
        int totalDeleted = 0;
        int batches = 0;
        int affected;
        OffsetDateTime param = toOffsetDateTime(threshold);
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
                    ps.setObject(1, param);
                    affected = ps.executeUpdate();
                } catch (Exception ex) {
                    throw new RuntimeException("Error executing failed cleanup on active lock connection: " + ex.getMessage(), ex);
                }
            } else if (lockTransactionTemplate != null) {
                Integer batchAffected = lockTransactionTemplate.execute(status ->
                        cleanupJdbcTemplate.update(sql, ps -> {
                            ps.setQueryTimeout(timeoutSeconds);
                            ps.setObject(1, param);
                        }));
                affected = batchAffected != null ? batchAffected : 0;
            } else {
                affected = cleanupJdbcTemplate.update(sql, ps -> {
                    ps.setQueryTimeout(timeoutSeconds);
                    ps.setObject(1, param);
                });
            }
            totalDeleted += affected;
            batches++;
        } while (affected > 0 && batches < effectiveMaxBatches && System.currentTimeMillis() < deadline);
        if (batches >= effectiveMaxBatches && affected > 0) {
            log.warn("Reached maximum batch limit ({} batches, {} records) during failed outbox cleanup. More expired records may remain.",
                    effectiveMaxBatches, totalDeleted);
        }
        return totalDeleted;
    }

    private String buildBatchDeleteSql(String status, String timestampColumn, int batchSize) {
        return String.format(
                "%s to_delete AS (%s id FROM %s WHERE status = '%s' AND COALESCE(%s, created_at) < ? " +
                        "ORDER BY COALESCE(%s, created_at) ASC, id ASC LIMIT %d FOR UPDATE SKIP LOCKED) " +
                        "%s FROM %s m USING to_delete d WHERE m.id = d.id",
                "WITH", "SELECT", quotedTableName, status, timestampColumn,
                timestampColumn, batchSize, "DELETE", quotedTableName
        );
    }

    private volatile long cachedPendingCount = 0L;
    private volatile long cacheExpiresAtMs = 0L;
    private volatile boolean hasCachedValue = false;
    private static final long CACHE_TTL_MS = 5000L;
    private final AtomicBoolean isRefreshingPendingCount = new AtomicBoolean(false);
    private volatile CompletableFuture<Long> pendingRefreshFuture = null;

    public void invalidatePendingCountCache() {
        this.hasCachedValue = false;
        this.cacheExpiresAtMs = 0L;
    }

    @Override
    public long getPendingMessagesCount() {
        long now = System.currentTimeMillis();
        if (now < cacheExpiresAtMs && hasCachedValue) {
            return cachedPendingCount;
        }
        if (isRefreshingPendingCount.compareAndSet(false, true)) {
            CompletableFuture<Long> future = new CompletableFuture<>();
            this.pendingRefreshFuture = future;
            try {
                Long count = jdbcTemplate.queryForObject(
                        "SELECT count(*) FROM " + quotedTableName + " WHERE status IN ('PENDING', 'IN_PROGRESS')",
                        Long.class
                );
                long res = (count != null && count >= 0L) ? count : 0L;
                cachedPendingCount = res;
                hasCachedValue = true;
                cacheExpiresAtMs = System.currentTimeMillis() + CACHE_TTL_MS;
                future.complete(res);
                return res;
            } catch (Exception e) {
                log.trace("Failed to query pending outbox messages count for gauge metric", e);
                cacheExpiresAtMs = System.currentTimeMillis() + CACHE_TTL_MS;
                if (!hasCachedValue) {
                    cachedPendingCount = 0L;
                }
                hasCachedValue = true;
                future.complete(cachedPendingCount);
                return cachedPendingCount;
            } finally {
                isRefreshingPendingCount.set(false);
            }
        }
        if (!hasCachedValue) {
            CompletableFuture<Long> future = this.pendingRefreshFuture;
            if (future != null) {
                try {
                    Long res = future.get(2, TimeUnit.SECONDS);
                    return (res != null && res >= 0L) ? res : 0L;
                } catch (Exception e) {
                    log.trace("Failed waiting for in-flight pending outbox messages count query", e);
                }
            }
        }
        return Math.max(0L, cachedPendingCount);
    }

    String serializeHeaders(Map<String, ?> headers) {
        if (headers == null || headers.isEmpty()) {
            return "{}";
        }
        try {
            Map<String, Object> output = new java.util.LinkedHashMap<>(headers.size());
            headers.forEach((k, v) -> {
                if (k != null) {
                    if (v instanceof List<?> list) {
                        if (list.isEmpty()) {
                            output.put(k, java.util.Collections.emptyList());
                        } else if (list.size() == 1) {
                            output.put(k, list.get(0));
                        } else {
                            output.put(k, list);
                        }
                    } else {
                        output.put(k, java.util.Objects.requireNonNullElse(v, java.util.Collections.emptyList()));
                    }
                }
            });
            String json = objectMapper.writeValueAsString(output);
            return json.contains("\\u0000") || json.indexOf('\0') >= 0
                    ? json.replace("\\u0000", "").replace("\0", "")
                    : json;
        } catch (JsonProcessingException e) {
            log.warn("Failed to serialize outbox headers as JSON, writing empty map", e);
            return "{}";
        }
    }

    String serializeHeaders(Map<String, String> headers, Map<String, List<String>> multiHeaders) {
        if ((headers == null || headers.isEmpty()) && (multiHeaders == null || multiHeaders.isEmpty())) {
            return "{}";
        }
        try {
            Map<String, Object> output = new java.util.LinkedHashMap<>();
            if (headers != null) {
                headers.forEach((k, v) -> {
                    if (k != null && v != null) {
                        output.put(k, v);
                    }
                });
            }
            if (multiHeaders != null) {
                multiHeaders.forEach((k, v) -> {
                    if (k != null) {
                        if (v == null) {
                            output.put(k, java.util.Collections.emptyList());
                        } else if (v.isEmpty()) {
                            if (!output.containsKey(k)) {
                                output.put(k, java.util.Collections.emptyList());
                            }
                        } else if (v.size() == 1) {
                            output.put(k, v.get(0));
                        } else {
                            output.put(k, v);
                        }
                    }
                });
            }
            String json = objectMapper.writeValueAsString(output);
            return json.contains("\\u0000") || json.indexOf('\0') >= 0
                    ? json.replace("\\u0000", "").replace("\0", "")
                    : json;
        } catch (JsonProcessingException e) {
            log.warn("Failed to serialize outbox headers as JSON, writing empty map", e);
            return "{}";
        }
    }

    @Override
    public boolean executeWithLock(String lockName, Runnable action) {
        return PostgresAdvisoryLockManager.executeWithLock(jdbcTemplate, tableName, lockName, action);
    }
}
