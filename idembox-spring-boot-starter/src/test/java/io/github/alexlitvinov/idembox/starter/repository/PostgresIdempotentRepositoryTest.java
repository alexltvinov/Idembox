package io.github.alexlitvinov.idembox.starter.repository;

import io.github.alexlitvinov.idembox.core.model.AcquireOutcome;
import io.github.alexlitvinov.idembox.core.model.AcquireResult;
import io.github.alexlitvinov.idembox.core.model.ProcessingState;
import io.github.alexlitvinov.idembox.core.model.ProcessingStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import java.sql.PreparedStatement;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PostgresIdempotentRepositoryTest {

    @Mock
    private JdbcTemplate jdbcTemplate;

    private PostgresIdempotentRepository repository;

    private static <T> RowMapper<T> anyRowMapper() {
        return any();
    }

    @BeforeEach
    void setUp() {
        repository = new PostgresIdempotentRepository(jdbcTemplate);
    }

    @Test
    @DisplayName("Senior Refinements v21: tryAcquire SQL excludes unconditional FAILED status and uses RETURNING locked_until")
    void tryAcquireSqlShouldAllowOvertakingFailedStatusForDlqRedrives() {
        Instant expectedDeadline = Instant.now().plusSeconds(120);
        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        when(jdbcTemplate.query(sqlCaptor.capture(), anyRowMapper(), eq("key-1"), eq("group-1"), eq(120000L), eq(604800000L)))
                .thenReturn(java.util.List.of(expectedDeadline));

        AcquireOutcome outcome = repository.tryAcquireWithLease("key-1", "group-1", Duration.ofMinutes(2), Duration.ofDays(7));

        assertThat(outcome.result()).isEqualTo(AcquireResult.ACQUIRED);
        assertThat(outcome.lockedUntil()).isEqualTo(expectedDeadline);
        assertThat(sqlCaptor.getValue()).contains("RETURNING locked_until");
        assertThat(sqlCaptor.getValue()).doesNotContain("OR \"idempotent_records\".status = 'FAILED'");
        assertThat(sqlCaptor.getValue()).contains("\"idempotent_records\".expires_at <= CURRENT_TIMESTAMP");
        assertThat(sqlCaptor.getValue()).contains("\"idempotent_records\".locked_until <= CURRENT_TIMESTAMP");
        assertThat(sqlCaptor.getValue()).contains("ON CONFLICT (consumer_group, event_key) DO UPDATE");
    }

    @Test
    @DisplayName("Senior Refinements v8: fallback query returns DUPLICATE_COMPLETED for COMPLETED and IN_FLIGHT_LOCKED for PROCESSING")
    void fallbackQueryStatusHandling() {
        when(jdbcTemplate.query(anyString(), anyRowMapper(), anyString(), anyString(), anyLong(), anyLong()))
                .thenReturn(java.util.Collections.emptyList()); // conflict occurred

        when(jdbcTemplate.queryForObject(anyString(), anyRowMapper(), eq("comp-key"), eq("group-1")))
                .thenAnswer(invocation -> {
                    RowMapper<?> mapper = invocation.getArgument(1);
                    java.sql.ResultSet rs = org.mockito.Mockito.mock(java.sql.ResultSet.class);
                    when(rs.getString("status")).thenReturn("COMPLETED");
                    when(rs.getBoolean("is_locked")).thenReturn(false);
                    return mapper.mapRow(rs, 1);
                });

        AcquireResult result = repository.tryAcquire("comp-key", "group-1", Duration.ofMinutes(2), Duration.ofDays(7));
        assertThat(result).isEqualTo(AcquireResult.DUPLICATE_COMPLETED);
    }

    @Test
    @DisplayName("NTP Drift Fix: tryAcquire query delegates lock validity evaluation to PostgreSQL CURRENT_TIMESTAMP")
    void tryAcquireQueryDelegatesLockValidityToPostgresCurrentTimestamp() {
        when(jdbcTemplate.query(anyString(), anyRowMapper(), anyString(), anyString(), anyLong(), anyLong()))
                .thenReturn(java.util.Collections.emptyList());

        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        when(jdbcTemplate.queryForObject(sqlCaptor.capture(), anyRowMapper(), eq("key-lock"), eq("group-1")))
                .thenAnswer(invocation -> {
                    RowMapper<?> mapper = invocation.getArgument(1);
                    java.sql.ResultSet rs = org.mockito.Mockito.mock(java.sql.ResultSet.class);
                    when(rs.getString("status")).thenReturn("PROCESSING");
                    when(rs.getBoolean("is_locked")).thenReturn(true);
                    return mapper.mapRow(rs, 1);
                });

        AcquireResult result = repository.tryAcquire("key-lock", "group-1", Duration.ofMinutes(2), Duration.ofDays(7));

        assertThat(result).isEqualTo(AcquireResult.IN_FLIGHT_LOCKED);
        assertThat(sqlCaptor.getValue())
                .contains("(locked_until IS NOT NULL AND locked_until > CURRENT_TIMESTAMP) AS is_locked");
    }

    @Test
    @DisplayName("Senior Refinements v13: cleanupExpired should delete in batches until no affected rows or time budget exhausted")
    void cleanupExpiredShouldObeyTimeBudget() {
        // Return 5000, 2500, then 0 affected rows
        when(jdbcTemplate.update(anyString(), any(org.springframework.jdbc.core.PreparedStatementSetter.class)))
                .thenReturn(5000, 2500, 0);

        int total = repository.cleanupExpired(Instant.now(), Duration.ofSeconds(10));

        assertThat(total).isEqualTo(7500);
        verify(jdbcTemplate, times(3)).update(anyString(), any(org.springframework.jdbc.core.PreparedStatementSetter.class));
    }

    @Test
    @DisplayName("Senior Refinements v13: cleanupExpired halts when time budget is exceeded")
    void cleanupExpiredShouldHaltWhenTimeBudgetExceeded() {
        when(jdbcTemplate.update(anyString(), any(org.springframework.jdbc.core.PreparedStatementSetter.class)))
                .thenAnswer(invocation -> {
                    Thread.sleep(15);
                    return 5000;
                });

        int total = repository.cleanupExpired(Instant.now(), Duration.ofMillis(10));

        assertThat(total).isGreaterThanOrEqualTo(5000);
    }

    @Test
    @DisplayName("Refinement 1: cleanupExpired should sort ASC for FIFO vacuuming and use SKIP LOCKED CTE")
    void cleanupExpiredShouldIncludeOrderByExpiresAtAsc() {
        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        when(jdbcTemplate.update(sqlCaptor.capture(), any(org.springframework.jdbc.core.PreparedStatementSetter.class))).thenReturn(0);

        repository.cleanupExpired(Instant.now());

        assertThat(sqlCaptor.getValue()).contains("ORDER BY expires_at ASC, consumer_group ASC, event_key ASC");
        assertThat(sqlCaptor.getValue()).contains("FOR UPDATE SKIP LOCKED");
        assertThat(sqlCaptor.getValue()).contains("USING to_delete d");
        assertThat(sqlCaptor.getValue()).contains("r.consumer_group = d.consumer_group AND r.event_key = d.event_key");
        assertThat(sqlCaptor.getValue()).contains("AND (status <> 'PROCESSING' OR (locked_until IS NOT NULL AND locked_until <= CURRENT_TIMESTAMP))");
    }

    @Test
    @DisplayName("Senior Refinements Round 60: cleanupExpired protects active PROCESSING leases when TTL expires before lockTimeout")
    void cleanupExpiredShouldProtectActiveProcessingLeasesFromDeletion() {
        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        when(jdbcTemplate.update(sqlCaptor.capture(), any(org.springframework.jdbc.core.PreparedStatementSetter.class))).thenReturn(0);

        repository.cleanupExpired(Instant.now());

        assertThat(sqlCaptor.getValue())
                .contains("WHERE expires_at < ?")
                .contains("AND (status <> 'PROCESSING' OR (locked_until IS NOT NULL AND locked_until <= CURRENT_TIMESTAMP))");
    }

    @Test
    @DisplayName("NTP Drift Fix: getProcessingState query delegates lock validity to CURRENT_TIMESTAMP")
    void getProcessingStateShouldIncludeIsLockedFromPostgres() {
        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        when(jdbcTemplate.queryForObject(sqlCaptor.capture(), anyRowMapper(), eq("key-state"), eq("group-1")))
                .thenAnswer(invocation -> {
                    RowMapper<?> mapper = invocation.getArgument(1);
                    java.sql.ResultSet rs = org.mockito.Mockito.mock(java.sql.ResultSet.class);
                    when(rs.getString("status")).thenReturn("PROCESSING");
                    when(rs.getBoolean("is_locked")).thenReturn(true);
                    return mapper.mapRow(rs, 1);
                });

        io.github.alexlitvinov.idembox.core.model.ProcessingState state = repository.getProcessingState("key-state", "group-1");

        assertThat(state).isNotNull();
        assertThat(state.isLocked()).isTrue();
        assertThat(sqlCaptor.getValue())
                .contains("(locked_until IS NOT NULL AND locked_until > CURRENT_TIMESTAMP) AS is_locked");
    }

    @Test
    @DisplayName("Senior Refinements v12: markCompleted with lockedUntil updates record only when locked_until matches lease")
    void markCompletedWithLockedUntilShouldIncludeLeaseCheckInSql() {
        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        Instant leaseDeadline = Instant.now().plusSeconds(60);

        repository.markCompleted("comp-key-1", "comp-group-1", Duration.ofDays(7), leaseDeadline);

        verify(jdbcTemplate).update(sqlCaptor.capture(), eq(604800000L), eq("comp-key-1"), eq("comp-group-1"),
                any(OffsetDateTime.class), any(OffsetDateTime.class), any(OffsetDateTime.class));
        assertThat(sqlCaptor.getValue())
                .contains("UPDATE \"idempotent_records\"")
                .contains("SET status = 'COMPLETED'")
                .contains("WHERE event_key = ? AND consumer_group = ? AND status = 'PROCESSING'")
                .contains("(locked_until = ? OR (locked_until >= ? - INTERVAL '1 microsecond' AND locked_until <= ? + INTERVAL '1 microsecond'))");
    }

    @Test
    @DisplayName("Senior Refinements v11 & v48 & Round 67: markFailed with lockedUntil deletes record within symmetric microsecond tolerance")
    void markFailedWithLockedUntilShouldIncludeLeaseCheckInSql() {
        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        Instant leaseDeadline = Instant.now().plusSeconds(60);

        repository.markFailed("fail-key-1", "fail-group-1", "test failure", leaseDeadline);

        verify(jdbcTemplate).update(sqlCaptor.capture(), eq("fail-key-1"), eq("fail-group-1"), any(OffsetDateTime.class), any(OffsetDateTime.class), any(OffsetDateTime.class));
        assertThat(sqlCaptor.getValue())
                .contains("DELETE FROM \"idempotent_records\"")
                .contains("WHERE event_key = ? AND consumer_group = ? AND status = 'PROCESSING'")
                .contains("AND (locked_until = ? OR (locked_until >= ? - INTERVAL '1 microsecond' AND locked_until <= ? + INTERVAL '1 microsecond'))");
    }

    @Test
    @DisplayName("Senior Refinements v16 & v48 & Round 67: markTerminalFailed updates record to FAILED status with symmetric microsecond tolerance")
    void markTerminalFailedShouldUpdateRecordToFailedStatus() {
        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        Instant leaseDeadline = Instant.now().plusSeconds(60);
        when(jdbcTemplate.update(sqlCaptor.capture(), eq(604800000L), eq("poison-key"), eq("poison-group"), any(OffsetDateTime.class), any(OffsetDateTime.class), any(OffsetDateTime.class)))
                .thenReturn(1);

        repository.markTerminalFailed("poison-key", "poison-group", "poison pill error", Duration.ofDays(7), leaseDeadline);

        assertThat(sqlCaptor.getValue())
                .contains("UPDATE \"idempotent_records\"")
                .contains("SET status = 'FAILED'")
                .contains("WHERE event_key = ? AND consumer_group = ? AND status <> 'COMPLETED'")
                .contains("AND (locked_until = ? OR (locked_until >= ? - INTERVAL '1 microsecond' AND locked_until <= ? + INTERVAL '1 microsecond') OR locked_until IS NULL OR locked_until <= CURRENT_TIMESTAMP)");
    }

    @Test
    @DisplayName("Senior Refinements v14: tryAcquire fallback query returns IN_FLIGHT_LOCKED when status is PROCESSING but lock is expired")
    void tryAcquireFallbackQueryReturnsInFlightLockedWhenProcessingAndNotLocked() {
        when(jdbcTemplate.query(anyString(), anyRowMapper(), anyString(), anyString(), anyLong(), anyLong()))
                .thenReturn(java.util.Collections.emptyList());

        when(jdbcTemplate.queryForObject(anyString(), anyRowMapper(), eq("key-expired-proc"), eq("group-1")))
                .thenAnswer(invocation -> {
                    RowMapper<?> mapper = invocation.getArgument(1);
                    java.sql.ResultSet rs = org.mockito.Mockito.mock(java.sql.ResultSet.class);
                    when(rs.getString("status")).thenReturn("PROCESSING");
                    when(rs.getBoolean("is_locked")).thenReturn(false);
                    return mapper.mapRow(rs, 1);
                });

        AcquireResult result = repository.tryAcquire("key-expired-proc", "group-1", Duration.ofMinutes(2), Duration.ofDays(7));
        assertThat(result).isEqualTo(AcquireResult.IN_FLIGHT_LOCKED);
    }

    @Test
    @DisplayName("executeWithLock executes action directly")
    void executeWithLockShouldFallbackOnException() {
        boolean[] actionExecuted = new boolean[1];
        boolean result = repository.executeWithLock("test_lock", () -> actionExecuted[0] = true);

        assertThat(result).isTrue();
        assertThat(actionExecuted[0]).isTrue();
    }

    @Test
    @DisplayName("Senior Refinements v18: cleanupExpired executes batch deletion within time budget")
    void cleanupExpiredShouldLoopWithinTimeBudget() {
        // Return 5000 on first invocation, 1200 on second, 0 on third
        when(jdbcTemplate.update(anyString(), any(org.springframework.jdbc.core.PreparedStatementSetter.class)))
                .thenReturn(5000)
                .thenReturn(1200)
                .thenReturn(0);

        int totalDeleted = repository.cleanupExpired(Instant.now(), Duration.ofSeconds(5));

        assertThat(totalDeleted).isEqualTo(6200);
        verify(jdbcTemplate, times(3)).update(anyString(), any(org.springframework.jdbc.core.PreparedStatementSetter.class));
    }

    @Test
    @DisplayName("executeWithLock executes action directly and returns true")
    void executeWithLockShouldReturnFalseWhenLockNotAcquired() {
        boolean[] actionRan = new boolean[1];
        boolean acquired = repository.executeWithLock("my_lock", () -> actionRan[0] = true);

        assertThat(acquired).isTrue();
        assertThat(actionRan[0]).isTrue();
    }

    @Test
    @DisplayName("Senior Refinements v18: constructor must not mutate global jdbcTemplate queryTimeout")
    void constructorShouldNotMutateJdbcTemplateQueryTimeout() {
        JdbcTemplate mockTemplate = org.mockito.Mockito.mock(JdbcTemplate.class);
        new PostgresIdempotentRepository(mockTemplate);
        verify(mockTemplate, org.mockito.Mockito.never()).setQueryTimeout(org.mockito.ArgumentMatchers.anyInt());
    }

    @Test
    @DisplayName("Senior Refinements v19: markFailed without lockedUntil executes DELETE query only for PROCESSING records")
    void markFailedWithoutLockedUntilShouldDeleteRecord() {
        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);

        repository.markFailed("fail-key-no-lease", "fail-group-no-lease", "some transient failure");

        verify(jdbcTemplate).update(sqlCaptor.capture(), eq("fail-key-no-lease"), eq("fail-group-no-lease"));
        assertThat(sqlCaptor.getValue())
                .contains("DELETE FROM \"idempotent_records\"")
                .contains("WHERE event_key = ? AND consumer_group = ? AND status = 'PROCESSING'");
    }

    @Test
    @DisplayName("Senior Refinements v19: markTerminalFailed without lockedUntil updates status = FAILED only if status <> COMPLETED")
    void markTerminalFailedWithoutLockedUntilWhenRowExists() {
        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        when(jdbcTemplate.update(sqlCaptor.capture(), eq(604800000L), eq("poison-key-1"), eq("poison-group-1")))
                .thenReturn(1); // 1 row updated

        repository.markTerminalFailed("poison-key-1", "poison-group-1", "poison pill error", Duration.ofDays(7));

        assertThat(sqlCaptor.getValue())
                .contains("UPDATE \"idempotent_records\"")
                .contains("SET status = 'FAILED'")
                .contains("WHERE event_key = ? AND consumer_group = ? AND status <> 'COMPLETED'");
        // Verify only 1 update executed and fallback insert was skipped
        verify(jdbcTemplate, times(1)).update(anyString(), any(), any(), any());
    }

    @Test
    @DisplayName("Senior Refinements v19: markTerminalFailed without lockedUntil fallback INSERT does not overwrite COMPLETED")
    void markTerminalFailedWithoutLockedUntilFallbackInsertWhenRowDeleted() {
        // First update returns 0 affected rows (row was deleted during transient retry)
        when(jdbcTemplate.update(anyString(), eq(604800000L), eq("poison-deleted-key"), eq("poison-group-2")))
                .thenReturn(0);

        repository.markTerminalFailed("poison-deleted-key", "poison-group-2", "poison pill error", Duration.ofDays(7));

        ArgumentCaptor<String> insertSqlCaptor = ArgumentCaptor.forClass(String.class);
        verify(jdbcTemplate).update(insertSqlCaptor.capture(), eq("poison-deleted-key"), eq("poison-group-2"), eq(604800000L));
        assertThat(insertSqlCaptor.getValue())
                .contains("INSERT INTO \"idempotent_records\"")
                .contains("VALUES (")
                .contains("ON CONFLICT (consumer_group, event_key) DO UPDATE")
                .contains("SET status = 'FAILED'")
                .contains("WHERE \"idempotent_records\".status <> 'COMPLETED'");
    }

    @Test
    @DisplayName("Senior Refinements v20: markCompleted without lockedUntil updates status to COMPLETED only when status = 'PROCESSING'")
    void markCompletedWithoutLockedUntilShouldUpdateStatus() {
        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);

        repository.markCompleted("comp-key-no-lease", "comp-group-no-lease", Duration.ofDays(7));

        verify(jdbcTemplate).update(sqlCaptor.capture(), eq(604800000L), eq("comp-key-no-lease"), eq("comp-group-no-lease"));
        assertThat(sqlCaptor.getValue())
                .contains("UPDATE \"idempotent_records\"")
                .contains("SET status = 'COMPLETED'")
                .contains("WHERE event_key = ? AND consumer_group = ? AND status = 'PROCESSING'");
    }

    @Test
    @DisplayName("Senior Refinements v21: tryAcquireWithLease UPSERT excludes unconditional status = 'FAILED' and fallback query returns DUPLICATE_COMPLETED for FAILED status")
    void tryAcquireWithLeaseShouldExcludeUnconditionalFailedAndReturnDuplicateCompleted() {
        ArgumentCaptor<String> upsertCaptor = ArgumentCaptor.forClass(String.class);
        when(jdbcTemplate.query(upsertCaptor.capture(), anyRowMapper(), anyString(), anyString(), anyLong(), anyLong()))
                .thenReturn(java.util.Collections.emptyList()); // UPSERT did not acquire

        when(jdbcTemplate.queryForObject(anyString(), anyRowMapper(), eq("failed-key"), eq("failed-group")))
                .thenAnswer(invocation -> {
                    RowMapper<?> mapper = invocation.getArgument(1);
                    java.sql.ResultSet rs = org.mockito.Mockito.mock(java.sql.ResultSet.class);
                    when(rs.getString("status")).thenReturn("FAILED");
                    when(rs.getBoolean("is_locked")).thenReturn(false);
                    return mapper.mapRow(rs, 1);
                });

        AcquireOutcome outcome = repository.tryAcquireWithLease("failed-key", "failed-group", Duration.ofMinutes(2), Duration.ofDays(7));

        assertThat(outcome.result()).isEqualTo(AcquireResult.DUPLICATE_FAILED);

        // Verify UPSERT does NOT have unconditional OR idempotent_records.status = 'FAILED'
        assertThat(upsertCaptor.getValue())
                .doesNotContain("OR \"idempotent_records\".status = 'FAILED'")
                .contains("\"idempotent_records\".expires_at <= CURRENT_TIMESTAMP")
                .contains("(\"idempotent_records\".status = 'PROCESSING' AND \"idempotent_records\".locked_until <= CURRENT_TIMESTAMP)");
    }

    @Test
    @DisplayName("tryAcquire should enforce minimum 1000ms floor for lockTimeout and deduplicationTtl")
    void tryAcquireShouldEnforceMinimum1000MsFloor() {
        when(jdbcTemplate.query(anyString(), anyRowMapper(), anyString(), anyString(), anyLong(), anyLong()))
                .thenReturn(java.util.List.of(Instant.now().plusSeconds(1)));

        repository.tryAcquire("floor-key", "floor-group", Duration.ofMillis(5), Duration.ofMillis(10));

        verify(jdbcTemplate).query(anyString(), anyRowMapper(), eq("floor-key"), eq("floor-group"), eq(1000L), eq(1000L));
    }

    @Test
    @DisplayName("markCompleted should enforce minimum 1000ms floor for deduplicationTtl")
    void markCompletedShouldEnforceMinimum1000MsFloor() {
        repository.markCompleted("completed-floor-key", "completed-floor-group", Duration.ofMillis(50));

        verify(jdbcTemplate).update(anyString(), eq(1000L), eq("completed-floor-key"), eq("completed-floor-group"));
    }

    @Test
    @DisplayName("markFailed with null lockedUntil should delegate to standard delete without lease filter")
    void markFailedWithNullLockedUntilShouldDelegateToStandardDelete() {
        repository.markFailed("null-lease-fail-key", "group-1", "some error", null);

        verify(jdbcTemplate).update(
                eq("DELETE FROM \"idempotent_records\" WHERE event_key = ? AND consumer_group = ? AND status = 'PROCESSING'"),
                eq("null-lease-fail-key"),
                eq("group-1")
        );
    }

    @Test
    @DisplayName("executeWithLock executes action directly without polluting TransactionSynchronizationManager")
    void executeWithLockShouldAcquireSessionAdvisoryLockWithoutPollutingTransactionSynchronizationManager() {
        javax.sql.DataSource ds = org.mockito.Mockito.mock(javax.sql.DataSource.class);
        boolean[] ran = new boolean[1];
        boolean[] wasBoundDuringAction = new boolean[1];
        boolean result = repository.executeWithLock("idempotent_lock", () -> {
            ran[0] = true;
            wasBoundDuringAction[0] = org.springframework.transaction.support.TransactionSynchronizationManager.hasResource(ds);
        });

        assertThat(result).isTrue();
        assertThat(ran[0]).isTrue();
        assertThat(wasBoundDuringAction[0]).isFalse();
        assertThat(org.springframework.transaction.support.TransactionSynchronizationManager.hasResource(ds)).isFalse();
    }

    @Test
    @DisplayName("executeWithLock rethrows action exception directly")
    void executeWithLockShouldNotReExecuteActionWhenActionThrowsException() {
        int[] executions = new int[1];
        org.assertj.core.api.Assertions.assertThatThrownBy(() ->
                repository.executeWithLock("idempotent_lock", () -> {
                    executions[0]++;
                    throw new RuntimeException("Idempotency cleanup failed");
                })
        ).isInstanceOf(RuntimeException.class).hasMessage("Idempotency cleanup failed");

        assertThat(executions[0]).isEqualTo(1);
    }

    @Test
    @DisplayName("Senior Refinements v24: sanitizeText strips null bytes and literal \\u0000 to prevent PostgreSQL UTF-8 column abort")
    void sanitizeTextShouldRemoveNullBytesAndLiterals() {
        assertThat(PostgresIdempotentRepository.sanitizeText(null)).isNull();
        assertThat(PostgresIdempotentRepository.sanitizeText("valid-key-123")).isEqualTo("valid-key-123");
        assertThat(PostgresIdempotentRepository.sanitizeText("order-\0-999")).isEqualTo("order--999");
        assertThat(PostgresIdempotentRepository.sanitizeText("group-\\u0000-alpha")).isEqualTo("group--alpha");
        assertThat(PostgresIdempotentRepository.sanitizeText("\0\0\\u0000")).isEqualTo("");
    }

    @Test
    @DisplayName("Senior Refinements v24: tryAcquireWithLease passes sanitized eventKey and consumerGroup to JdbcTemplate")
    void tryAcquireWithLeaseShouldSanitizeNullBytesInParameters() {
        when(jdbcTemplate.query(anyString(), anyRowMapper(), eq("order--123"), eq("group--A"), anyLong(), anyLong()))
                .thenReturn(java.util.List.of(Instant.now().plusSeconds(60)));

        AcquireOutcome outcome = repository.tryAcquireWithLease("order-\0-123", "group-\\u0000-A", Duration.ofMinutes(2), Duration.ofDays(7));

        assertThat(outcome.result()).isEqualTo(AcquireResult.ACQUIRED);
        verify(jdbcTemplate).query(anyString(), anyRowMapper(), eq("order--123"), eq("group--A"), anyLong(), anyLong());
    }

    @Test
    @DisplayName("executeWithLock executes action directly without session advisory locks")
    void executeWithLockShouldScopeLockWithSchemaPrefix() {
        boolean[] ran = new boolean[1];
        boolean result = repository.executeWithLock("idembox_idempotent_cleanup", () -> ran[0] = true);

        assertThat(result).isTrue();
        assertThat(ran[0]).isTrue();
    }

    @Test
    @DisplayName("executeWithLock is safe under PgBouncer transaction pooling")
    void executeWithLockShouldCloseConnectionWhenUnlockFails() {
        boolean[] ran = new boolean[1];
        boolean result = repository.executeWithLock("cleanup", () -> ran[0] = true);

        assertThat(result).isTrue();
        assertThat(ran[0]).isTrue();
    }

    @Test
    @DisplayName("Senior Refinements v38: PostgresIdempotentRepository uses custom schema and table prefix in SQL queries")
    void shouldSupportCustomSchemaAndTablePrefix() {
        PostgresIdempotentRepository customRepo = new PostgresIdempotentRepository(
                jdbcTemplate,
                "tenant_1.app_idempotent_records"
        );

        assertThat(customRepo.getTableName()).isEqualTo("tenant_1.app_idempotent_records");

        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        customRepo.markCompleted("key-1", "group-1", Duration.ofMinutes(10));

        verify(jdbcTemplate).update(sqlCaptor.capture(), eq(600000L), eq("key-1"), eq("group-1"));
        assertThat(sqlCaptor.getValue()).contains("UPDATE \"tenant_1\".\"app_idempotent_records\"");
    }

    @Test
    @DisplayName("Senior Refinements v40: markCompleted returns true on success and false on stolen lease")
    void markCompletedShouldReturnBoolean() {
        when(jdbcTemplate.update(anyString(), anyLong(), eq("key-success"), eq("group-1"))).thenReturn(1);
        when(jdbcTemplate.update(anyString(), anyLong(), eq("key-stolen"), eq("group-1"))).thenReturn(0);

        boolean success = repository.markCompleted("key-success", "group-1", Duration.ofMinutes(10));
        boolean stolen = repository.markCompleted("key-stolen", "group-1", Duration.ofMinutes(10));

        assertThat(success).isTrue();
        assertThat(stolen).isFalse();
    }

    @Test
    @DisplayName("Senior Refinements v40: resetFailed updates FAILED record and returns boolean")
    void resetFailedShouldUpdateStatusAndReturnBoolean() {
        when(jdbcTemplate.update(anyString(), eq("key-failed"), eq("group-1"))).thenReturn(1);
        when(jdbcTemplate.update(anyString(), eq("key-not-found"), eq("group-1"))).thenReturn(0);

        boolean resetSuccess = repository.resetFailed("key-failed", "group-1");
        boolean resetNotFound = repository.resetFailed("key-not-found", "group-1");

        assertThat(resetSuccess).isTrue();
        assertThat(resetNotFound).isFalse();

        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        verify(jdbcTemplate).update(sqlCaptor.capture(), eq("key-failed"), eq("group-1"));
        assertThat(sqlCaptor.getValue())
                .contains("UPDATE \"idempotent_records\"")
                .contains("SET status = 'PROCESSING'")
                .contains("WHERE event_key = ? AND consumer_group = ? AND status = 'FAILED'");
    }

    @Test
    @DisplayName("Senior Refinements Round 58: cleanupExpired configures statement timeout locally on PreparedStatementSetter without mutating shared JdbcTemplate")
    void cleanupExpiredShouldSetQueryTimeoutLocallyWithoutMutatingSharedTemplate() throws java.sql.SQLException {
        ArgumentCaptor<org.springframework.jdbc.core.PreparedStatementSetter> pssCaptor =
                ArgumentCaptor.forClass(org.springframework.jdbc.core.PreparedStatementSetter.class);
        when(jdbcTemplate.update(anyString(), pssCaptor.capture())).thenReturn(0);

        repository.cleanupExpired(Instant.now(), Duration.ofSeconds(20));
        verify(jdbcTemplate, never()).setQueryTimeout(anyInt());

        PreparedStatement mockPs = mock(PreparedStatement.class);
        pssCaptor.getValue().setValues(mockPs);
        verify(mockPs).setQueryTimeout(20);
        verify(mockPs).setObject(eq(1), any(OffsetDateTime.class));
    }

    @Test
    @DisplayName("Senior Refinements v41: markCompleted truncates Instant to microsecond precision")
    void markCompletedShouldTruncateToMicroseconds() {
        ArgumentCaptor<OffsetDateTime> dateCaptor = ArgumentCaptor.forClass(OffsetDateTime.class);
        Instant nanoInstant = Instant.parse("2026-10-04T12:00:00.123456789Z");

        repository.markCompleted("key-nano", "group-1", Duration.ofMinutes(10), nanoInstant);

        verify(jdbcTemplate).update(anyString(), anyLong(), eq("key-nano"), eq("group-1"),
                dateCaptor.capture(), any(OffsetDateTime.class), any(OffsetDateTime.class));

        assertThat(dateCaptor.getValue().getNano()).isEqualTo(123456000);
    }

    @Test
    @DisplayName("executeWithLock allows concurrent execution across different schemas without conflict")
    void executeWithLockShouldPrioritizeTableSchemaOverConnectionSchema() {
        PostgresIdempotentRepository tenantRepo = new PostgresIdempotentRepository(
                jdbcTemplate,
                "tenant_b.app_idempotent_records"
        );
        boolean[] ran = new boolean[1];
        boolean result = tenantRepo.executeWithLock("idembox_idempotent_cleanup", () -> ran[0] = true);
        assertThat(result).isTrue();
        assertThat(ran[0]).isTrue();
    }

    @Test
    @DisplayName("executeWithLock allows independent execution across services with different table prefixes")
    void executeWithLockShouldDifferentiateServicesWithDifferentTablePrefixesInSameSchema() {
        PostgresIdempotentRepository orderRepo = new PostgresIdempotentRepository(
                jdbcTemplate,
                "public.order_idempotent_records"
        );
        PostgresIdempotentRepository paymentRepo = new PostgresIdempotentRepository(
                jdbcTemplate,
                "public.payment_idempotent_records"
        );
        boolean[] orderRan = new boolean[1];
        boolean[] paymentRan = new boolean[1];

        boolean r1 = orderRepo.executeWithLock("idembox_idempotent_cleanup", () -> orderRan[0] = true);
        boolean r2 = paymentRepo.executeWithLock("idembox_idempotent_cleanup", () -> paymentRan[0] = true);

        assertThat(r1).isTrue();
        assertThat(r2).isTrue();
        assertThat(orderRan[0]).isTrue();
        assertThat(paymentRan[0]).isTrue();
    }

    @Test
    @DisplayName("Senior Refinements v43: markFailed and markTerminalFailed execute within isolated REQUIRES_NEW transaction template")
    void markFailedAndTerminalFailedShouldExecuteInRequiresNew() {
        org.springframework.transaction.PlatformTransactionManager txManager = mock(org.springframework.transaction.PlatformTransactionManager.class);
        org.springframework.transaction.support.TransactionTemplate txTemplate = new org.springframework.transaction.support.TransactionTemplate(txManager);
        txTemplate.setPropagationBehavior(org.springframework.transaction.TransactionDefinition.PROPAGATION_REQUIRES_NEW);

        PostgresIdempotentRepository repoWithTx = new PostgresIdempotentRepository(jdbcTemplate, txTemplate);

        assertThat(repoWithTx.getRequiresNewTransactionTemplate()).isNotNull();
        assertThat(repoWithTx.getRequiresNewTransactionTemplate().getPropagationBehavior())
                .isEqualTo(org.springframework.transaction.TransactionDefinition.PROPAGATION_REQUIRES_NEW);

        when(txManager.getTransaction(any())).thenReturn(mock(org.springframework.transaction.TransactionStatus.class));
        when(jdbcTemplate.update(anyString(), eq("order-1"), eq("order-group"), any(OffsetDateTime.class), any(OffsetDateTime.class), any(OffsetDateTime.class))).thenReturn(1);
        when(jdbcTemplate.update(anyString(), eq(604800000L), eq("order-2"), eq("order-group"), any(OffsetDateTime.class), any(OffsetDateTime.class), any(OffsetDateTime.class))).thenReturn(1);

        Instant lockedUntil = Instant.now().plusSeconds(30);
        repoWithTx.markFailed("order-1", "order-group", "transient error", lockedUntil);

        verify(txManager, atLeastOnce()).getTransaction(any());
        verify(txManager, atLeastOnce()).commit(any());
        verify(jdbcTemplate).update(contains("DELETE FROM \"idempotent_records\""), eq("order-1"), eq("order-group"), any(), any(), any());

        repoWithTx.markTerminalFailed("order-2", "order-group", "fatal error", Duration.ofDays(7), lockedUntil);
        verify(jdbcTemplate).update(contains("UPDATE \"idempotent_records\""), eq(604800000L), eq("order-2"), eq("order-group"), any(), any(), any());
    }

    @Test
    @DisplayName("Senior Refinements v48: cleanupExpired respects maxBatches limit of 20")
    void cleanupExpiredShouldRespectMaxBatchesCap() {
        when(jdbcTemplate.update(anyString(), any(org.springframework.jdbc.core.PreparedStatementSetter.class))).thenReturn(5000);

        int totalDeleted = repository.cleanupExpired(Instant.now(), Duration.ofSeconds(60));

        assertThat(totalDeleted).isEqualTo(5000 * 20);
        verify(jdbcTemplate, times(20)).update(anyString(), any(org.springframework.jdbc.core.PreparedStatementSetter.class));
    }

    @Test
    @DisplayName("Senior Refinements Round 68: cleanupExpired respects configurable batchSize and maxBatches")
    void shouldRespectConfigurableBatchSizeAndMaxBatchesInCleanup() {
        org.mockito.ArgumentCaptor<String> sqlCaptor = org.mockito.ArgumentCaptor.forClass(String.class);
        when(jdbcTemplate.update(sqlCaptor.capture(), any(org.springframework.jdbc.core.PreparedStatementSetter.class)))
                .thenReturn(2000, 2000, 0);

        int totalDeleted = repository.cleanupExpired(Instant.now(), Duration.ofSeconds(60), 2000, 50);

        assertThat(totalDeleted).isEqualTo(4000);
        assertThat(sqlCaptor.getValue()).contains("LIMIT 2000");
        verify(jdbcTemplate, times(3)).update(anyString(), any(org.springframework.jdbc.core.PreparedStatementSetter.class));
    }

    @Test
    @DisplayName("Senior Refinements v43: resetFailed executes within isolated REQUIRES_NEW transaction template")
    void resetFailedShouldExecuteInRequiresNew() {
        org.springframework.transaction.PlatformTransactionManager txManager = mock(org.springframework.transaction.PlatformTransactionManager.class);
        org.springframework.transaction.support.TransactionTemplate txTemplate = new org.springframework.transaction.support.TransactionTemplate(txManager);

        PostgresIdempotentRepository repoWithTx = new PostgresIdempotentRepository(jdbcTemplate, txTemplate);
        when(txManager.getTransaction(any())).thenReturn(mock(org.springframework.transaction.TransactionStatus.class));
        when(jdbcTemplate.update(anyString(), eq("order-1"), eq("order-group"))).thenReturn(1);

        boolean reset = repoWithTx.resetFailed("order-1", "order-group");

        assertThat(reset).isTrue();
        verify(txManager).getTransaction(any());
        verify(txManager).commit(any());
        verify(jdbcTemplate).update(contains("UPDATE \"idempotent_records\" SET status = 'PROCESSING'"), eq("order-1"), eq("order-group"));
    }

    @Test
    @DisplayName("Senior Refinements v45: PostgresIdempotentRepository safely quotes reserved keywords in schema and table")
    void shouldSafelyQuoteReservedKeywordsInSchemaAndTable() {
        PostgresIdempotentRepository reservedRepo = new PostgresIdempotentRepository(
                jdbcTemplate,
                "user.order"
        );

        assertThat(reservedRepo.getTableName()).isEqualTo("user.order");

        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        reservedRepo.markCompleted("key-1", "group-1", Duration.ofMinutes(10));

        verify(jdbcTemplate).update(sqlCaptor.capture(), eq(600000L), eq("key-1"), eq("group-1"));
        assertThat(sqlCaptor.getValue()).contains("UPDATE \"user\".\"order\"");
    }

    @Test
    @DisplayName("Senior Refinements Round 51: markCompleted executes within executeInRequiresNew transaction")
    void markCompletedShouldExecuteInRequiresNew() {
        org.springframework.transaction.PlatformTransactionManager txManager = org.mockito.Mockito.mock(org.springframework.transaction.PlatformTransactionManager.class);
        org.springframework.transaction.TransactionStatus txStatus = org.mockito.Mockito.mock(org.springframework.transaction.TransactionStatus.class);
        when(txManager.getTransaction(any())).thenReturn(txStatus);

        org.springframework.transaction.support.TransactionTemplate tt = new org.springframework.transaction.support.TransactionTemplate(txManager);
        PostgresIdempotentRepository repoWithTx = new PostgresIdempotentRepository(jdbcTemplate, tt, "idempotent_records");

        when(jdbcTemplate.update(anyString(), anyLong(), eq("key-tx"), eq("group-tx"))).thenReturn(1);

        boolean result = repoWithTx.markCompleted("key-tx", "group-tx", Duration.ofMinutes(10));

        assertThat(result).isTrue();
        verify(txManager).getTransaction(argThat(def -> def.getPropagationBehavior() == org.springframework.transaction.TransactionDefinition.PROPAGATION_REQUIRES_NEW));
        verify(txManager).commit(txStatus);
    }

    @Test
    @DisplayName("cleanupExpired executes deletion directly using cleanupJdbcTemplate")
    void cleanupExpiredShouldReuseLockedConnection() {
        when(jdbcTemplate.update(anyString(), any(org.springframework.jdbc.core.PreparedStatementSetter.class)))
                .thenReturn(0);

        boolean result = repository.executeWithLock("idembox_idempotency_cleanup", () -> repository.cleanupExpired(Instant.now()));

        assertThat(result).isTrue();
        verify(jdbcTemplate).update(anyString(), any(org.springframework.jdbc.core.PreparedStatementSetter.class));
    }

    @Test
    @DisplayName("Senior Refinements Round 54: tryAcquireWithLease immediately takes over expired lease when is_locked is false")
    void tryAcquireWithLeaseShouldImmediatelyTakeOverExpiredLeaseWhenIsLockedIsFalse() {
        Instant expectedDeadline = Instant.now().plusSeconds(120);

        // Initial upsert returns empty list (conflict)
        when(jdbcTemplate.query(contains("INSERT INTO"), anyRowMapper(), eq("key-expired"), eq("group-1"), anyLong(), anyLong()))
                .thenReturn(java.util.Collections.emptyList());

        // queryForObject returns PROCESSING with is_locked = false
        when(jdbcTemplate.queryForObject(contains("SELECT status"), anyRowMapper(), eq("key-expired"), eq("group-1")))
                .thenAnswer(invocation -> {
                    RowMapper<?> mapper = invocation.getArgument(1);
                    java.sql.ResultSet rs = org.mockito.Mockito.mock(java.sql.ResultSet.class);
                    when(rs.getString("status")).thenReturn("PROCESSING");
                    when(rs.getBoolean("is_locked")).thenReturn(false);
                    return mapper.mapRow(rs, 1);
                });

        // takeover query returns new locked_until
        when(jdbcTemplate.query(contains("UPDATE"), anyRowMapper(), anyLong(), anyLong(), eq("key-expired"), eq("group-1")))
                .thenReturn(List.of(expectedDeadline));

        AcquireOutcome outcome = repository.tryAcquireWithLease("key-expired", "group-1", Duration.ofMinutes(2), Duration.ofDays(7));

        assertThat(outcome.result()).isEqualTo(AcquireResult.ACQUIRED);
        assertThat(outcome.lockedUntil()).isEqualTo(expectedDeadline);
    }

    @Test
    @DisplayName("Round 57: tryAcquireWithLease takeOverSql strictly requires status = 'PROCESSING' and does not overwrite COMPLETED")
    void tryAcquireWithLeaseTakeoverShouldRequireStatusProcessing() {
        // Initial upsert returns empty list (conflict)
        when(jdbcTemplate.query(contains("INSERT INTO"), anyRowMapper(), eq("key-comp-race"), eq("group-1"), anyLong(), anyLong()))
                .thenReturn(java.util.Collections.emptyList());

        // queryForObject returns PROCESSING with is_locked = false (initial read before worker #1 commits)
        when(jdbcTemplate.queryForObject(contains("SELECT status, (locked_until IS NOT NULL"), anyRowMapper(), eq("key-comp-race"), eq("group-1")))
                .thenAnswer(invocation -> {
                    RowMapper<?> mapper = invocation.getArgument(1);
                    java.sql.ResultSet rs = org.mockito.Mockito.mock(java.sql.ResultSet.class);
                    when(rs.getString("status")).thenReturn("PROCESSING");
                    when(rs.getBoolean("is_locked")).thenReturn(false);
                    return mapper.mapRow(rs, 1);
                });

        // takeover query fails because record became COMPLETED (taken is empty)
        // verify takeOverSql includes status = 'PROCESSING'
        when(jdbcTemplate.query(argThat((String sql) -> sql != null && sql.contains("UPDATE") && sql.contains("status = 'PROCESSING'")),
                anyRowMapper(), anyLong(), anyLong(), eq("key-comp-race"), eq("group-1")))
                .thenReturn(Collections.emptyList());

        // fresh state query sees COMPLETED
        when(jdbcTemplate.queryForObject(contains("SELECT status, locked_until, expires_at"), anyRowMapper(), eq("key-comp-race"), eq("group-1")))
                .thenReturn(new ProcessingState(ProcessingStatus.COMPLETED, null, Instant.now().plusSeconds(3600), false));

        AcquireOutcome outcome = repository.tryAcquireWithLease("key-comp-race", "group-1", Duration.ofMinutes(2), Duration.ofDays(7));

        assertThat(outcome.result()).isEqualTo(AcquireResult.DUPLICATE_COMPLETED);
    }

    @Test
    @DisplayName("Senior Refinements Round 58: normalizeEventKey hashes keys > 2000 bytes with SHA-256 for B-Tree index safety")
    void normalizeEventKeyShouldHashLongKeys() {
        String shortKey = "order-12345";
        assertThat(PostgresIdempotentRepository.normalizeEventKey(shortKey)).isEqualTo(shortKey);

        String longKey = "x".repeat(2500);
        String normalized = PostgresIdempotentRepository.normalizeEventKey(longKey);

        assertThat(normalized).startsWith("SHA256:");
        assertThat(normalized.length()).isEqualTo(7 + 64);

        // tryAcquireWithLease passes normalized key to JDBC
        Instant expectedDeadline = Instant.now().plusSeconds(120);
        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);
        when(jdbcTemplate.query(anyString(), anyRowMapper(), keyCaptor.capture(), eq("group-1"), anyLong(), anyLong()))
                .thenReturn(List.of(expectedDeadline));

        repository.tryAcquireWithLease(longKey, "group-1", Duration.ofMinutes(2), Duration.ofDays(7));
        assertThat(keyCaptor.getValue()).isEqualTo(normalized);
    }

    @Test
    @DisplayName("Senior Refinements Round 61: tryAcquireWithLease retries and acquires lease when record was deleted by previous worker")
    void takeOverMissedAndRowDeletedConcurrentlyShouldRetryAndAcquireLease() {
        Instant expectedDeadline = Instant.now().plusSeconds(120);

        // 1st attempt upsert returns empty list (conflict occurred)
        // 2nd attempt upsert returns expectedDeadline (success on retry)
        when(jdbcTemplate.query(argThat((String sql) -> sql != null && sql.contains("INSERT INTO")),
                anyRowMapper(), eq("key-del-race"), eq("group-1"), anyLong(), anyLong()))
                .thenReturn(Collections.emptyList())
                .thenReturn(List.of(expectedDeadline));

        // queryForObject sees PROCESSING not locked
        when(jdbcTemplate.queryForObject(contains("SELECT status, (locked_until IS NOT NULL AND locked_until > CURRENT_TIMESTAMP)"),
                anyRowMapper(), eq("key-del-race"), eq("group-1")))
                .thenAnswer(inv -> {
                    RowMapper<?> mapper = inv.getArgument(1);
                    java.sql.ResultSet rs = mock(java.sql.ResultSet.class);
                    when(rs.getString("status")).thenReturn("PROCESSING");
                    when(rs.getBoolean("is_locked")).thenReturn(false);
                    return mapper.mapRow(rs, 1);
                });

        // takeOverSql updates 0 rows because row was deleted by markFailed concurrently
        when(jdbcTemplate.query(argThat((String sql) -> sql != null && sql.contains("UPDATE") && sql.contains("status = 'PROCESSING'")),
                anyRowMapper(), anyLong(), anyLong(), eq("key-del-race"), eq("group-1")))
                .thenReturn(Collections.emptyList());

        // getProcessingState returns null because record is physically deleted
        when(jdbcTemplate.queryForObject(contains("SELECT status, locked_until, expires_at"),
                anyRowMapper(), eq("key-del-race"), eq("group-1")))
                .thenThrow(new org.springframework.dao.EmptyResultDataAccessException(1));

        AcquireOutcome outcome = repository.tryAcquireWithLease("key-del-race", "group-1", Duration.ofMinutes(2), Duration.ofDays(7));

        // Must succeed and acquire lease on retry instead of returning IN_FLIGHT_LOCKED
        assertThat(outcome.result()).isEqualTo(AcquireResult.ACQUIRED);
        assertThat(outcome.lockedUntil()).isEqualTo(expectedDeadline);
    }

    @Test
    @DisplayName("Senior Refinements Round 62 & Round 77: markTerminalFailed triggers fallback INSERT with locked_until fencing when affected == 0")
    void markTerminalFailedShouldTriggerFallbackInsertWhenAffectedZeroWithLockedUntil() {
        Instant lockedUntil = Instant.now().plusSeconds(60);

        when(jdbcTemplate.update(contains("UPDATE \"idempotent_records\" SET status = 'FAILED'"),
                anyLong(), eq("key-failed"), eq("group-1"), any(), any(), any()))
                .thenReturn(0);

        when(jdbcTemplate.update(contains("INSERT INTO \"idempotent_records\""),
                eq("key-failed"), eq("group-1"), anyLong(), any(), any(), any()))
                .thenReturn(1);

        repository.markTerminalFailed("key-failed", "group-1", "test-reason", Duration.ofDays(7), lockedUntil);

        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        verify(jdbcTemplate).update(sqlCaptor.capture(),
                eq("key-failed"), eq("group-1"), anyLong(), any(), any(), any());
        assertThat(sqlCaptor.getValue())
                .contains("ON CONFLICT (consumer_group, event_key) DO UPDATE")
                .contains("\"idempotent_records\".locked_until = ?");
    }

    @Test
    @DisplayName("Senior Refinements Round 64: markCompleted participates in ambient transaction when active")
    void markCompletedShouldParticipateInActiveTransaction() {
        org.springframework.transaction.support.TransactionSynchronizationManager.setActualTransactionActive(true);
        try {
            when(jdbcTemplate.update(anyString(), anyLong(), anyString(), anyString())).thenReturn(1);
            boolean completed = repository.markCompleted("tx-key-1", "tx-group-1", Duration.ofMinutes(5));
            assertThat(completed).isTrue();
            verify(jdbcTemplate).update(contains("UPDATE \"idempotent_records\" SET status = 'COMPLETED'"),
                    anyLong(), eq("tx-key-1"), eq("tx-group-1"));
        } finally {
            org.springframework.transaction.support.TransactionSynchronizationManager.setActualTransactionActive(false);
        }
    }

    @Test
    @DisplayName("Senior Refinements Round 70: tryAcquireWithLease executes in PROPAGATION_REQUIRES_NEW even when ambient transaction is active")
    void tryAcquireWithLeaseShouldExecuteInRequiresNew() {
        org.springframework.transaction.PlatformTransactionManager txManager = mock(org.springframework.transaction.PlatformTransactionManager.class);
        org.springframework.transaction.TransactionStatus txStatus = mock(org.springframework.transaction.TransactionStatus.class);
        when(txManager.getTransaction(any())).thenReturn(txStatus);

        org.springframework.transaction.support.TransactionTemplate tt = new org.springframework.transaction.support.TransactionTemplate(txManager);
        PostgresIdempotentRepository repoWithTx = new PostgresIdempotentRepository(jdbcTemplate, tt, "idempotent_records");

        Instant expectedDeadline = Instant.now().plusSeconds(120);
        when(jdbcTemplate.query(anyString(), anyRowMapper(), eq("key-req-new"), eq("group-req-new"), anyLong(), anyLong()))
                .thenReturn(List.of(expectedDeadline));

        org.springframework.transaction.support.TransactionSynchronizationManager.setActualTransactionActive(true);
        try {
            AcquireOutcome outcome = repoWithTx.tryAcquireWithLease("key-req-new", "group-req-new", Duration.ofMinutes(2), Duration.ofDays(7));
            assertThat(outcome.result()).isEqualTo(AcquireResult.ACQUIRED);
            assertThat(outcome.lockedUntil()).isEqualTo(expectedDeadline);
            verify(txManager).getTransaction(argThat(def -> def.getPropagationBehavior() == org.springframework.transaction.TransactionDefinition.PROPAGATION_REQUIRES_NEW));
            verify(txManager).commit(txStatus);
        } finally {
            org.springframework.transaction.support.TransactionSynchronizationManager.setActualTransactionActive(false);
        }
    }

    @Test
    @DisplayName("cleanupExpired executes batches using cleanupJdbcTemplate and FOR UPDATE SKIP LOCKED without connection hijacking")
    void shouldExecuteBatchesUsingCleanupJdbcTemplateWithoutConnectionHijacking() {
        when(jdbcTemplate.update(contains("WITH to_delete AS"), any(org.springframework.jdbc.core.PreparedStatementSetter.class)))
                .thenReturn(100)
                .thenReturn(50)
                .thenReturn(0);

        int deleted = repository.cleanupExpired(Instant.now());

        assertThat(deleted).isEqualTo(150);
        verify(jdbcTemplate, times(3)).update(contains("WITH to_delete AS"), any(org.springframework.jdbc.core.PreparedStatementSetter.class));
    }

    @Test
    @DisplayName("cleanupExpired inside active Spring transaction executes batches in requiresNewTransactionTemplate without hijacking outer connection")
    void shouldExecuteBatchesInRequiresNewWithoutHijackingOuterConnectionWhenCalledInsideActiveTransaction() {
        org.springframework.transaction.PlatformTransactionManager txManager = mock(org.springframework.transaction.PlatformTransactionManager.class);
        org.springframework.transaction.TransactionStatus txStatus = mock(org.springframework.transaction.TransactionStatus.class);
        when(txManager.getTransaction(any())).thenReturn(txStatus);
        org.springframework.transaction.support.TransactionTemplate tt = new org.springframework.transaction.support.TransactionTemplate(txManager);
        PostgresIdempotentRepository repoWithTx = new PostgresIdempotentRepository(jdbcTemplate, tt, "idempotent_records");

        when(jdbcTemplate.update(contains("WITH to_delete AS"), any(org.springframework.jdbc.core.PreparedStatementSetter.class)))
                .thenReturn(20)
                .thenReturn(0);

        org.springframework.transaction.support.TransactionSynchronizationManager.setActualTransactionActive(true);
        try {
            int deleted = repoWithTx.cleanupExpired(Instant.now());
            assertThat(deleted).isEqualTo(20);
            verify(txManager, atLeastOnce()).getTransaction(org.mockito.ArgumentMatchers.argThat(def ->
                    def.getPropagationBehavior() == org.springframework.transaction.TransactionDefinition.PROPAGATION_REQUIRES_NEW));
        } finally {
            org.springframework.transaction.support.TransactionSynchronizationManager.setActualTransactionActive(false);
        }
    }

    @Test
    @DisplayName("Senior Refinements Round 77: markTerminalFailed without lockedUntil checks that record is not actively locked by another worker")
    void markTerminalFailedWithoutLockedUntilShouldRespectActiveLocks() {
        when(jdbcTemplate.update(contains("UPDATE \"idempotent_records\" SET status = 'FAILED'"),
                anyLong(), eq("key-null-lock"), eq("group-1")))
                .thenReturn(0);
        when(jdbcTemplate.update(contains("INSERT INTO \"idempotent_records\""),
                eq("key-null-lock"), eq("group-1"), anyLong()))
                .thenReturn(1);

        repository.markTerminalFailed("key-null-lock", "group-1", "reason", Duration.ofDays(7), null);

        ArgumentCaptor<String> updateSqlCaptor = ArgumentCaptor.forClass(String.class);
        verify(jdbcTemplate).update(updateSqlCaptor.capture(), anyLong(), eq("key-null-lock"), eq("group-1"));
        assertThat(updateSqlCaptor.getValue())
                .contains("(locked_until IS NULL OR locked_until <= CURRENT_TIMESTAMP)");

        ArgumentCaptor<String> insertSqlCaptor = ArgumentCaptor.forClass(String.class);
        verify(jdbcTemplate).update(insertSqlCaptor.capture(), eq("key-null-lock"), eq("group-1"), anyLong());
        assertThat(insertSqlCaptor.getValue())
                .contains("(\"idempotent_records\".locked_until IS NULL OR \"idempotent_records\".locked_until <= CURRENT_TIMESTAMP)");
    }

    @Test
    @DisplayName("Senior Refinements Round 78: markTerminalFailed with non-null lockedUntil allows updating expired or unlocked records while protecting active leases of other workers")
    void markTerminalFailedWithNonNullLockedUntilShouldAlsoAllowExpiredOrNullLock() {
        Instant lease = Instant.now().minusSeconds(10).truncatedTo(java.time.temporal.ChronoUnit.MICROS);
        OffsetDateTime leaseOdt = OffsetDateTime.ofInstant(lease, java.time.ZoneOffset.UTC);

        when(jdbcTemplate.update(contains("UPDATE \"idempotent_records\" SET status = 'FAILED'"),
                anyLong(), eq("key-expired-lock"), eq("group-1"), eq(leaseOdt), eq(leaseOdt), eq(leaseOdt)))
                .thenReturn(0);
        when(jdbcTemplate.update(contains("INSERT INTO \"idempotent_records\""),
                eq("key-expired-lock"), eq("group-1"), anyLong(), eq(leaseOdt), eq(leaseOdt), eq(leaseOdt)))
                .thenReturn(1);

        repository.markTerminalFailed("key-expired-lock", "group-1", "reason", Duration.ofDays(7), lease);

        ArgumentCaptor<String> updateSqlCaptor = ArgumentCaptor.forClass(String.class);
        verify(jdbcTemplate).update(updateSqlCaptor.capture(), anyLong(), eq("key-expired-lock"), eq("group-1"), eq(leaseOdt), eq(leaseOdt), eq(leaseOdt));
        assertThat(updateSqlCaptor.getValue())
                .contains("OR locked_until IS NULL OR locked_until <= CURRENT_TIMESTAMP)");

        ArgumentCaptor<String> insertSqlCaptor = ArgumentCaptor.forClass(String.class);
        verify(jdbcTemplate).update(insertSqlCaptor.capture(), eq("key-expired-lock"), eq("group-1"), anyLong(), eq(leaseOdt), eq(leaseOdt), eq(leaseOdt));
        assertThat(insertSqlCaptor.getValue())
                .contains("OR \"idempotent_records\".locked_until IS NULL OR \"idempotent_records\".locked_until <= CURRENT_TIMESTAMP)");
    }

    @Test
    @DisplayName("Constructor with PlatformTransactionManager initializes requiresNewTransactionTemplate using the provided manager")
    void shouldInitializeRequiresNewTransactionTemplateFromPlatformTransactionManager() {
        org.springframework.transaction.PlatformTransactionManager txManager = mock(org.springframework.transaction.PlatformTransactionManager.class);
        PostgresIdempotentRepository repo = new PostgresIdempotentRepository(jdbcTemplate, txManager);

        assertThat(repo.getRequiresNewTransactionTemplate()).isNotNull();
        assertThat(repo.getRequiresNewTransactionTemplate().getTransactionManager()).isSameAs(txManager);
        assertThat(repo.getRequiresNewTransactionTemplate().getPropagationBehavior())
                .isEqualTo(org.springframework.transaction.TransactionDefinition.PROPAGATION_REQUIRES_NEW);
    }

    @Test
    @DisplayName("Default constructor should not create duplicate DataSourceTransactionManager when no transaction manager is provided")
    void defaultConstructorShouldNotCreateDuplicateDataSourceTransactionManager() {
        javax.sql.DataSource ds = mock(javax.sql.DataSource.class);
        JdbcTemplate jdbcWithDs = new JdbcTemplate(ds);

        PostgresIdempotentRepository repo = new PostgresIdempotentRepository(jdbcWithDs);

        assertThat(repo.getRequiresNewTransactionTemplate()).isNull();
    }

    @Test
    @DisplayName("Deep Audit Issue 1: markCompleted within active read-only transaction uses requiresNewTransactionTemplate")
    void markCompletedWithinActiveReadOnlyTransactionUsesRequiresNew() {
        org.springframework.transaction.PlatformTransactionManager txManager = mock(org.springframework.transaction.PlatformTransactionManager.class);
        PostgresIdempotentRepository repoWithTx = new PostgresIdempotentRepository(jdbcTemplate, txManager);

        org.springframework.transaction.TransactionStatus status = mock(org.springframework.transaction.TransactionStatus.class);
        when(txManager.getTransaction(argThat(def -> def != null && def.getPropagationBehavior() == org.springframework.transaction.TransactionDefinition.PROPAGATION_REQUIRES_NEW && !def.isReadOnly())))
                .thenReturn(status);

        org.springframework.transaction.support.TransactionSynchronizationManager.setActualTransactionActive(true);
        org.springframework.transaction.support.TransactionSynchronizationManager.setCurrentTransactionReadOnly(true);

        try {
            when(jdbcTemplate.update(anyString(), any(), any(), any())).thenReturn(1);
            boolean result = repoWithTx.markCompleted("ro-key-1", "ro-group-1", Duration.ofDays(7));
            assertThat(result).isTrue();

            verify(txManager).getTransaction(argThat(def -> def != null && def.getPropagationBehavior() == org.springframework.transaction.TransactionDefinition.PROPAGATION_REQUIRES_NEW && !def.isReadOnly()));
            verify(txManager).commit(status);
        } finally {
            org.springframework.transaction.support.TransactionSynchronizationManager.setActualTransactionActive(false);
            org.springframework.transaction.support.TransactionSynchronizationManager.setCurrentTransactionReadOnly(false);
            org.springframework.transaction.support.TransactionSynchronizationManager.clear();
        }
    }

    @Test
    @DisplayName("Deep Audit Issue 1: resetFailed with consumer group updates with consumer_group condition")
    void resetFailedWithConsumerGroupShouldIncludeGroupCondition() {
        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        when(jdbcTemplate.update(sqlCaptor.capture(), eq("event-1"), eq("group-1"))).thenReturn(1);

        boolean result = repository.resetFailed("event-1", "group-1");

        assertThat(result).isTrue();
        assertThat(sqlCaptor.getValue())
                .contains("WHERE event_key = ? AND consumer_group = ? AND status = 'FAILED'");
        verify(jdbcTemplate).update(anyString(), eq("event-1"), eq("group-1"));
    }

    @Test
    @DisplayName("Deep Audit Issue 1: resetFailed with null or blank consumer group updates without consumer_group condition avoiding SQL 3-valued logic failure")
    void resetFailedWithoutConsumerGroupShouldOmitGroupCondition() {
        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        when(jdbcTemplate.update(sqlCaptor.capture(), eq("event-2"))).thenReturn(1);

        boolean resultNull = repository.resetFailed("event-2", null);
        assertThat(resultNull).isTrue();
        assertThat(sqlCaptor.getValue())
                .contains("WHERE event_key = ? AND status = 'FAILED'")
                .doesNotContain("consumer_group = ?");

        boolean resultBlank = repository.resetFailed("event-2", "   ");
        assertThat(resultBlank).isTrue();
        verify(jdbcTemplate, times(2)).update(anyString(), eq("event-2"));
    }
}

