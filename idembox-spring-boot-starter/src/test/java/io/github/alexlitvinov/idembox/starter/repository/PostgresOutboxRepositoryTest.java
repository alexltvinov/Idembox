package io.github.alexlitvinov.idembox.starter.repository;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.alexlitvinov.idembox.core.model.OutboxMessage;
import io.github.alexlitvinov.idembox.core.model.OutboxStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.quality.Strictness;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import java.sql.PreparedStatement;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PostgresOutboxRepositoryTest {

    @Mock
    private JdbcTemplate jdbcTemplate;

    @Captor
    private ArgumentCaptor<RowMapper<OutboxMessage>> mapperCaptor;

    private PostgresOutboxRepository repository;

    private static <T> RowMapper<T> anyRowMapper() {
        return any();
    }

    private static <T> ConnectionCallback<T> anyConnectionCallback() {
        return any();
    }

    @BeforeEach
    void setUp() {
        repository = new PostgresOutboxRepository(jdbcTemplate, new ObjectMapper());
    }

    @Test
    @DisplayName("Refinement 1: releaseClaims updates IN_PROGRESS messages back to PENDING immediately using ANY(?::uuid[])")
    void releaseClaimsShouldUpdateStatusBackToPending() {
        UUID id1 = UUID.randomUUID();
        UUID id2 = UUID.randomUUID();

        repository.releaseClaims(List.of(id1, id2));

        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<Object> paramCaptor = ArgumentCaptor.forClass(Object.class);
        verify(jdbcTemplate).update(sqlCaptor.capture(), eq(0L), paramCaptor.capture());

        assertThat(sqlCaptor.getValue())
                .contains("UPDATE \"outbox_messages\"")
                .contains("SET status = 'PENDING'")
                .contains("next_retry_at = GREATEST(COALESCE(next_retry_at, CURRENT_TIMESTAMP), CURRENT_TIMESTAMP + (? * INTERVAL '1 millisecond'))")
                .contains("WHERE status = 'IN_PROGRESS' AND id = ANY(?::uuid[])");
        assertThat((UUID[]) paramCaptor.getValue()).containsExactly(id1, id2);
    }

    @Test
    @DisplayName("Refinement 3: claimBatchForProcessing sorts in-memory deterministically by createdAt and id")
    void claimBatchShouldSortDeterministicallyInMemory() {
        Instant t0 = Instant.parse("2026-09-19T10:00:00Z");
        Instant t1 = Instant.parse("2026-09-19T10:00:01Z");
        UUID idA = UUID.fromString("00000000-0000-0000-0000-000000000001");
        UUID idB = UUID.fromString("00000000-0000-0000-0000-000000000002");

        OutboxMessage msgEarlier = new OutboxMessage(idA, "t", "k", "{}", Collections.emptyMap(), OutboxStatus.IN_PROGRESS, 0, t0, null, null, null);
        OutboxMessage msgLater = new OutboxMessage(idB, "t", "k", "{}", Collections.emptyMap(), OutboxStatus.IN_PROGRESS, 0, t1, null, null, null);

        // Mock DB returning rows in reverse/arbitrary tuple order (msgLater first)
        when(jdbcTemplate.query(anyString(), anyRowMapper(), anyLong(), anyInt(), anyInt(), anyInt(), anyInt(), anyLong()))
                .thenReturn(List.of(msgLater, msgEarlier));

        List<OutboxMessage> result = repository.claimBatchForProcessing(10, Duration.ofMinutes(1));

        // Result MUST be sorted: msgEarlier first, msgLater second!
        assertThat(result).containsExactly(msgEarlier, msgLater);
    }

    @Test
    @DisplayName("Refinement 6: deleteSentBefore must obey Safety Cap (max 20 batches)")
    void deleteSentBeforeShouldObeySafetyCap() {
        when(jdbcTemplate.update(anyString(), any(org.springframework.jdbc.core.PreparedStatementSetter.class)))
                .thenReturn(5000);

        int total = repository.deleteSentBefore(Instant.now());

        // Max 20 batches * 5000 = 100,000 records
        assertThat(total).isEqualTo(100_000);
        verify(jdbcTemplate, times(20)).update(anyString(), any(org.springframework.jdbc.core.PreparedStatementSetter.class));
    }

    @Test
    @DisplayName("Refinement 6: deleteFailedBefore must obey Safety Cap (max 20 batches)")
    void deleteFailedBeforeShouldObeySafetyCap() {
        when(jdbcTemplate.update(anyString(), any(org.springframework.jdbc.core.PreparedStatementSetter.class)))
                .thenReturn(5000);

        int total = repository.deleteFailedBefore(Instant.now());

        assertThat(total).isEqualTo(100_000);
        verify(jdbcTemplate, times(20)).update(anyString(), any(org.springframework.jdbc.core.PreparedStatementSetter.class));
    }

    @Test
    @DisplayName("Senior Refinements Round 51: deleteSentBefore uses COALESCE(sent_at, created_at) to purge records with null sent_at")
    void deleteSentBeforeShouldIncludeOrderBySentAtAsc() {
        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        when(jdbcTemplate.update(sqlCaptor.capture(), any(org.springframework.jdbc.core.PreparedStatementSetter.class))).thenReturn(0);

        repository.deleteSentBefore(Instant.now());

        assertThat(sqlCaptor.getValue()).contains("WHERE status = 'SENT' AND COALESCE(sent_at, created_at) < ?");
        assertThat(sqlCaptor.getValue()).contains("ORDER BY COALESCE(sent_at, created_at) ASC, id ASC");
        assertThat(sqlCaptor.getValue()).contains("FOR UPDATE SKIP LOCKED");
        assertThat(sqlCaptor.getValue()).contains("USING to_delete d");
    }

    @Test
    @DisplayName("Senior Refinements Round 51: deleteFailedBefore uses COALESCE(failed_at, created_at) to purge records with null failed_at")
    void deleteFailedBeforeShouldIncludeOrderByFailedAtAsc() {
        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        when(jdbcTemplate.update(sqlCaptor.capture(), any(org.springframework.jdbc.core.PreparedStatementSetter.class))).thenReturn(0);

        repository.deleteFailedBefore(Instant.now());

        assertThat(sqlCaptor.getValue()).contains("WHERE status = 'FAILED' AND COALESCE(failed_at, created_at) < ?");
        assertThat(sqlCaptor.getValue()).contains("ORDER BY COALESCE(failed_at, created_at) ASC, id ASC");
        assertThat(sqlCaptor.getValue()).contains("FOR UPDATE SKIP LOCKED");
        assertThat(sqlCaptor.getValue()).contains("USING to_delete d");
    }

    @Test
    @DisplayName("Thundering herd: releaseClaims with deferral should set next_retry_at parameterized offset")
    void releaseClaimsWithDeferralShouldSetNextRetryAt() {
        UUID id1 = UUID.randomUUID();
        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        when(jdbcTemplate.update(sqlCaptor.capture(), eq(15000L), any())).thenReturn(1);

        repository.releaseClaims(List.of(id1), Duration.ofSeconds(15));

        assertThat(sqlCaptor.getValue()).contains("next_retry_at = GREATEST(COALESCE(next_retry_at, CURRENT_TIMESTAMP), CURRENT_TIMESTAMP + (? * INTERVAL '1 millisecond'))");
        verify(jdbcTemplate).update(anyString(), eq(15000L), any());
    }

    @Test
    @DisplayName("Fix 7 & v25: claimBatchForProcessing SQL contains partition_key IS NULL and earlier.topic = m.topic check")
    void claimBatchSqlShouldContainShortCircuit() {
        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        when(jdbcTemplate.query(sqlCaptor.capture(), anyRowMapper(), anyLong(), anyInt(), anyInt(), anyInt(), anyInt(), anyLong()))
                .thenReturn(Collections.emptyList());

        repository.claimBatchForProcessing(5, Duration.ofSeconds(30));

        assertThat(sqlCaptor.getValue())
                .contains("AND (m.partition_key IS NULL OR m.partition_key = '' OR NOT EXISTS (")
                .contains("WHERE earlier.topic = m.topic")
                .contains("AND earlier.partition_key = m.partition_key");
    }

    @Test
    @DisplayName("Fix 3: getPendingMessagesCount executes SELECT count(*) query for PENDING and IN_PROGRESS")
    void getPendingMessagesCountShouldQueryDatabase() {
        when(jdbcTemplate.queryForObject(
                eq("SELECT count(*) FROM \"outbox_messages\" WHERE status IN ('PENDING', 'IN_PROGRESS')"),
                eq(Long.class)
        )).thenReturn(142L);

        long count = repository.getPendingMessagesCount();

        assertThat(count).isEqualTo(142L);
    }

    @Test
    @DisplayName("Senior Refinements Round 69: getPendingMessagesCount catches database exception and returns 0L without failing scraper")
    void getPendingMessagesCountShouldReturnZeroOnDatabaseException() {
        when(jdbcTemplate.queryForObject(
                anyString(),
                eq(Long.class)
        )).thenThrow(new org.springframework.jdbc.CannotGetJdbcConnectionException("Hikari pool exhausted"));

        long count = repository.getPendingMessagesCount();

        assertThat(count).isEqualTo(0L);
    }

    @Test
    @DisplayName("Fix 1: rescheduleMessage updates status to PENDING with backoff without incrementing retry_count")
    void rescheduleMessageShouldUpdateStatusToPendingWithoutIncrementingRetryCount() {
        UUID msgId = UUID.randomUUID();
        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);

        when(jdbcTemplate.update(sqlCaptor.capture(), eq(5000L), eq("Broker timeout"), eq(msgId)))
                .thenReturn(1);

        repository.rescheduleMessage(msgId, "Broker timeout", Duration.ofSeconds(5));

        String sql = sqlCaptor.getValue();
        assertThat(sql).contains("SET status = 'PENDING'");
        assertThat(sql).contains("next_retry_at = CURRENT_TIMESTAMP + (? * INTERVAL '1 millisecond')");
        assertThat(sql).doesNotContain("retry_count = retry_count + 1");
        verify(jdbcTemplate).update(anyString(), eq(5000L), eq("Broker timeout"), eq(msgId));
    }

    @Test
    @DisplayName("Senior Refinements v8: claimBatchForProcessing executes within TransactionTemplate when provided")
    void claimBatchShouldExecuteWithinTransactionTemplateWhenProvided() {
        org.springframework.transaction.support.TransactionTemplate txTemplate = org.mockito.Mockito.mock(org.springframework.transaction.support.TransactionTemplate.class);
        PostgresOutboxRepository txRepo = new PostgresOutboxRepository(jdbcTemplate, new ObjectMapper(), txTemplate);

        when(txTemplate.execute(any())).thenAnswer(invocation -> {
            org.springframework.transaction.support.TransactionCallback<?> callback = invocation.getArgument(0);
            return callback.doInTransaction(org.mockito.Mockito.mock(org.springframework.transaction.TransactionStatus.class));
        });
        when(jdbcTemplate.query(anyString(), anyRowMapper(), anyLong(), anyInt(), anyInt(), anyInt(), anyInt(), anyLong()))
                .thenReturn(Collections.emptyList());

        List<OutboxMessage> result = txRepo.claimBatchForProcessing(10, Duration.ofMinutes(1));

        assertThat(result).isNotNull();
        verify(txTemplate).execute(any());
    }

    @Test
    @DisplayName("Senior Refinements v8: OutboxRowMapper extracts UUID from String or UUID object safely")
    void outboxRowMapperShouldExtractUuidFromStringOrUuid() throws java.sql.SQLException {
        java.sql.ResultSet rs = org.mockito.Mockito.mock(java.sql.ResultSet.class, org.mockito.Mockito.withSettings().strictness(Strictness.LENIENT));
        UUID expectedId = UUID.randomUUID();

        // 1. Return String from rs.getObject("id")
        when(rs.getObject("id")).thenReturn(expectedId.toString());
        when(rs.getString("topic")).thenReturn("test-topic");
        when(rs.getString("status")).thenReturn("PENDING");
        when(rs.getObject("created_at", OffsetDateTime.class)).thenReturn(OffsetDateTime.now());

        when(jdbcTemplate.query(anyString(), mapperCaptor.capture(), anyLong(), anyInt(), anyInt(), anyInt(), anyInt(), anyLong()))
                .thenReturn(Collections.emptyList());

        repository.claimBatchForProcessing(1, Duration.ofMinutes(1));
        RowMapper<OutboxMessage> mapper = mapperCaptor.getValue();

        OutboxMessage msg = mapper.mapRow(rs, 1);
        assertThat(msg).isNotNull();
        assertThat(msg.getId()).isEqualTo(expectedId);

        // 2. Return UUID from rs.getObject("id")
        when(rs.getObject("id")).thenReturn(expectedId);
        OutboxMessage msg2 = mapper.mapRow(rs, 1);
        assertThat(msg2).isNotNull();
        assertThat(msg2.getId()).isEqualTo(expectedId);
    }

    @Test
    @DisplayName("Senior Refinements v10: Cascading OOM Crash Loop Prevention - Expired IN_PROGRESS claims increment retry_count and transition to FAILED")
    void shouldIncrementRetryCountAndFilterPoisonousMessagesTransitionedToFailed() {
        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);

        UUID activeId = UUID.randomUUID();
        UUID poisonId = UUID.randomUUID();

        OutboxMessage activeMsg = new OutboxMessage(activeId, "topic", "k1", "{}", Collections.emptyMap(), OutboxStatus.IN_PROGRESS, 1, Instant.now(), null, null, null);
        OutboxMessage failedMsg = new OutboxMessage(poisonId, "topic", "k2", "{}", Collections.emptyMap(), OutboxStatus.FAILED, 5, Instant.now(), null, null, "Worker lease expired");

        when(jdbcTemplate.query(sqlCaptor.capture(), anyRowMapper(), eq(86400000L), eq(10), eq(5), eq(5), eq(5), eq(120000L)))
                .thenReturn(List.of(activeMsg, failedMsg));

        List<OutboxMessage> claimed = repository.claimBatchForProcessing(10, Duration.ofMinutes(2), 5);

        String sql = sqlCaptor.getValue();
        assertThat(sql).contains("status = CASE WHEN (status = 'IN_PROGRESS' AND retry_count + 1 >= ?) THEN 'FAILED' ELSE 'IN_PROGRESS' END");
        assertThat(sql).contains("retry_count = CASE WHEN status = 'IN_PROGRESS' THEN retry_count + 1 ELSE retry_count END");
        assertThat(sql).contains("last_error = CASE WHEN status = 'IN_PROGRESS' THEN 'Worker lease expired / ungraceful shutdown' ELSE last_error END");

        // The failed message must be filtered out so worker does not attempt it!
        assertThat(claimed).containsExactly(activeMsg);
    }

    @Test
    @DisplayName("Senior Refinements v14: OutboxRowMapper parses numeric and boolean headers safely into strings")
    void outboxRowMapperShouldParseNonStringHeaders() throws java.sql.SQLException {
        java.sql.ResultSet rs = org.mockito.Mockito.mock(java.sql.ResultSet.class, org.mockito.Mockito.withSettings().strictness(Strictness.LENIENT));
        UUID expectedId = UUID.randomUUID();

        when(rs.getObject("id")).thenReturn(expectedId);
        when(rs.getString("topic")).thenReturn("test-topic");
        when(rs.getString("status")).thenReturn("PENDING");
        when(rs.getObject("created_at", OffsetDateTime.class)).thenReturn(OffsetDateTime.now());
        when(rs.getString("headers")).thenReturn("{\"traceId\":\"abc-123\",\"retryCount\":3,\"isRetry\":true,\"weight\":4.5}");

        when(jdbcTemplate.query(anyString(), mapperCaptor.capture(), anyLong(), anyInt(), anyInt(), anyInt(), anyInt(), anyLong()))
                .thenReturn(Collections.emptyList());

        repository.claimBatchForProcessing(1, Duration.ofMinutes(1));
        RowMapper<OutboxMessage> mapper = mapperCaptor.getValue();

        OutboxMessage msg = mapper.mapRow(rs, 1);
        assertThat(msg).isNotNull();
        assertThat(msg.getHeaders()).containsEntry("traceId", "abc-123");
        assertThat(msg.getHeaders()).containsEntry("retryCount", "3");
        assertThat(msg.getHeaders()).containsEntry("isRetry", "true");
        assertThat(msg.getHeaders()).containsEntry("weight", "4.5");
    }

    @Test
    @DisplayName("Senior Refinements v46: OutboxRowMapper preserves structured JSON objects and arrays in headers")
    void outboxRowMapperShouldPreserveStructuredHeaders() throws java.sql.SQLException {
        java.sql.ResultSet rs = org.mockito.Mockito.mock(java.sql.ResultSet.class, org.mockito.Mockito.withSettings().strictness(Strictness.LENIENT));
        UUID expectedId = UUID.randomUUID();

        when(rs.getObject("id")).thenReturn(expectedId);
        when(rs.getString("topic")).thenReturn("test-topic");
        when(rs.getString("status")).thenReturn("PENDING");
        when(rs.getObject("created_at", OffsetDateTime.class)).thenReturn(OffsetDateTime.now());
        when(rs.getString("headers")).thenReturn("{\"trace\":{\"id\":\"123\",\"sampled\":true},\"tags\":[\"order\",{\"region\":\"eu\"}]}");

        when(jdbcTemplate.query(anyString(), mapperCaptor.capture(), anyLong(), anyInt(), anyInt(), anyInt(), anyInt(), anyLong()))
                .thenReturn(Collections.emptyList());

        repository.claimBatchForProcessing(1, Duration.ofMinutes(1));
        RowMapper<OutboxMessage> mapper = mapperCaptor.getValue();

        OutboxMessage msg = mapper.mapRow(rs, 1);
        assertThat(msg).isNotNull();
        // Object node preserved as JSON string instead of empty string
        assertThat(msg.getHeaders().get("trace")).isEqualTo("{\"id\":\"123\",\"sampled\":true}");
        // Array node elements: textual and object preserved
        assertThat(msg.getMultiHeaders().get("tags")).containsExactly("order", "{\"region\":\"eu\"}");
    }

    @Test
    @DisplayName("executeWithLock falls back to direct execution when database throws DataAccessException")
    void executeWithLockShouldFallbackOnDataAccessException() {
        when(jdbcTemplate.execute(anyConnectionCallback()))
                .thenThrow(new org.springframework.dao.DataRetrievalFailureException("Lock unavailable"));
        boolean[] actionExecuted = new boolean[1];
        boolean result = repository.executeWithLock("test_lock", () -> actionExecuted[0] = true);

        assertThat(result).isTrue();
        assertThat(actionExecuted[0]).isTrue();
    }

    @Test
    @DisplayName("Senior Refinements v15: serializeHeaders sanitizes NUL byte \\u0000 for PostgreSQL JSONB")
    void serializeHeadersShouldSanitizeNulByte() {
        Map<String, String> headers = Map.of(
                "binaryHeader", "bad \u0000data",
                "cleanHeader", "good-data"
        );

        String json = repository.serializeHeaders(headers);

        assertThat(json).doesNotContain("\u0000");
        assertThat(json).doesNotContain("\\u0000");
        assertThat(json).contains("bad data");
        assertThat(json).contains("good-data");
    }

    @Test
    @DisplayName("Senior Refinements v18/v41: rescheduleMessage updates status to PENDING with backoff interval and handles PENDING or IN_PROGRESS")
    void rescheduleMessageShouldUpdateStatusToPendingAndSetRetryInterval() {
        UUID id = UUID.randomUUID();
        repository.rescheduleMessage(id, "Broker timeout", Duration.ofSeconds(5));

        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        verify(jdbcTemplate).update(sqlCaptor.capture(), eq(5000L), eq("Broker timeout"), eq(id));

        assertThat(sqlCaptor.getValue())
                .contains("UPDATE \"outbox_messages\"")
                .contains("SET status = 'PENDING'")
                .contains("next_retry_at = CURRENT_TIMESTAMP + (? * INTERVAL '1 millisecond')")
                .contains("last_error = ?")
                .contains("WHERE id = ? AND status IN ('PENDING', 'IN_PROGRESS')");
    }

    @Test
    @DisplayName("Senior Refinements v18: markAsSent includes optimistic status guard WHERE id = ? AND status IN ('PENDING', 'IN_PROGRESS')")
    void markAsSentShouldIncludeStatusGuard() {
        UUID id = UUID.randomUUID();
        Instant sentAt = Instant.now();
        repository.markAsSent(id, sentAt);

        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        verify(jdbcTemplate).update(sqlCaptor.capture(), any(OffsetDateTime.class), eq(id));

        assertThat(sqlCaptor.getValue())
                .contains("UPDATE \"outbox_messages\"")
                .contains("SET status = 'SENT'")
                .contains("WHERE id = ? AND status IN ('PENDING', 'IN_PROGRESS')");
    }

    @Test
    @DisplayName("Senior Refinements v18/v41: markAsFailed includes optimistic status guard WHERE id = ? AND status IN ('PENDING', 'IN_PROGRESS')")
    void markAsFailedShouldIncludeStatusGuard() {
        UUID id = UUID.randomUUID();
        repository.markAsFailed(id, "Permanent error", 3, Duration.ofSeconds(2));

        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        verify(jdbcTemplate).update(sqlCaptor.capture(), eq(3), eq(3), eq(3), eq(2000L), eq("Permanent error"), eq(id));

        assertThat(sqlCaptor.getValue())
                .contains("UPDATE \"outbox_messages\"")
                .contains("WHERE id = ? AND status IN ('PENDING', 'IN_PROGRESS')");
    }

    @Test
    @DisplayName("Senior Refinements v18: save sanitizes \\u0000 in payload and headers before inserting into PostgreSQL JSONB")
    void saveShouldSanitizeNulByteInPayloadAndHeaders() {
        UUID id = UUID.randomUUID();
        OutboxMessage message = new OutboxMessage(
                id, "orders-topic", "ORD-1", "{\"bad\":\"foo \u0000bar \\u0000baz\"}",
                Map.of("h1", "val\u00001"), OutboxStatus.PENDING, 0,
                Instant.now(), null, null, null
        );

        repository.save(message);

        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        verify(jdbcTemplate).update(
                sqlCaptor.capture(),
                eq(id),
                eq("orders-topic"),
                eq("ORD-1"),
                eq("{\"bad\":\"foo bar baz\"}"),
                eq("{\"h1\":\"val1\"}"),
                eq("PENDING"),
                eq(0),
                any(),
                any(),
                any(),
                any(),
                any()
        );

        assertThat(sqlCaptor.getValue()).contains("INSERT INTO \"outbox_messages\"");
    }

    @Test
    @DisplayName("Senior Refinements v18: constructor must not mutate global jdbcTemplate queryTimeout")
    void constructorShouldNotMutateJdbcTemplateQueryTimeout() {
        JdbcTemplate mockTemplate = org.mockito.Mockito.mock(JdbcTemplate.class);
        new PostgresOutboxRepository(mockTemplate, new ObjectMapper());
        verify(mockTemplate, org.mockito.Mockito.never()).setQueryTimeout(anyInt());
    }

    @Test
    @DisplayName("Senior Refinements Round 69: getPendingMessagesCount caches failure on database exception returning 0L without querying DB repeatedly")
    void getPendingMessagesCountShouldHandleExceptionsGracefully() {
        when(jdbcTemplate.queryForObject(anyString(), eq(Long.class)))
                .thenThrow(new org.springframework.dao.DataAccessException("Connection refused") {});

        long count1 = repository.getPendingMessagesCount();
        long count2 = repository.getPendingMessagesCount();

        assertThat(count1).isEqualTo(0L);
        assertThat(count2).isEqualTo(0L);
        verify(jdbcTemplate, times(1)).queryForObject(anyString(), eq(Long.class));
    }

    @Test
    @DisplayName("Senior Refinements Round 50: getPendingMessagesCount preserves stale cached count and negatively caches when DB fails after successful read")
    void getPendingMessagesCountShouldPreserveStaleCacheOnDbFailure() throws Exception {
        when(jdbcTemplate.queryForObject(anyString(), eq(Long.class)))
                .thenReturn(42L)
                .thenThrow(new org.springframework.dao.DataAccessException("Connection refused") {});

        long count1 = repository.getPendingMessagesCount();
        assertThat(count1).isEqualTo(42L);

        java.lang.reflect.Field field = PostgresOutboxRepository.class.getDeclaredField("cacheExpiresAtMs");
        field.setAccessible(true);
        field.setLong(repository, 0L);

        long count2 = repository.getPendingMessagesCount();
        long count3 = repository.getPendingMessagesCount();

        assertThat(count2).isEqualTo(42L);
        assertThat(count3).isEqualTo(42L);
        verify(jdbcTemplate, times(2)).queryForObject(anyString(), eq(Long.class));
    }

    @Test
    @DisplayName("Senior Refinements v19: getPendingMessagesCount caches count for subsequent calls within TTL")
    void getPendingMessagesCountShouldCacheValueWithinTtl() {
        when(jdbcTemplate.queryForObject(anyString(), eq(Long.class)))
                .thenReturn(42L);

        long count1 = repository.getPendingMessagesCount();
        long count2 = repository.getPendingMessagesCount();

        assertThat(count1).isEqualTo(42L);
        assertThat(count2).isEqualTo(42L);
        // Only queried once due to caching
        verify(jdbcTemplate, times(1)).queryForObject(anyString(), eq(Long.class));
    }

    @Test
    @DisplayName("Senior Refinements v19: markAsFailed and rescheduleMessage sanitize null bytes in errorReason")
    void markAsFailedAndRescheduleMessageShouldSanitizeNullBytes() {
        UUID msgId = UUID.randomUUID();

        repository.markAsFailed(msgId, "Error \u0000with \0nulls", 5, Duration.ofSeconds(1));
        repository.rescheduleMessage(msgId, "Reschedule \u0000with \0nulls", Duration.ofSeconds(1));

        // Verify that sanitizeText strips \u0000 and \0
        assertThat(PostgresOutboxRepository.sanitizeText("Error \u0000with \0nulls")).isEqualTo("Error with nulls");
    }

    @Test
    @DisplayName("executeWithLock executes without polluting TransactionSynchronizationManager")
    void executeWithLockShouldExecuteWithoutPollutingTransactionSynchronizationManager() {
        javax.sql.DataSource ds = org.mockito.Mockito.mock(javax.sql.DataSource.class);
        boolean[] ran = new boolean[1];
        boolean[] wasBoundDuringAction = new boolean[1];
        boolean result = repository.executeWithLock("conn_lock", () -> {
            ran[0] = true;
            wasBoundDuringAction[0] = org.springframework.transaction.support.TransactionSynchronizationManager.hasResource(ds);
        });

        assertThat(result).isTrue();
        assertThat(ran[0]).isTrue();
        assertThat(wasBoundDuringAction[0]).isFalse();
        assertThat(org.springframework.transaction.support.TransactionSynchronizationManager.hasResource(ds)).isFalse();
    }

    @Test
    @DisplayName("Senior Refinements v20 / Round 62: getPendingMessagesCount waits for in-flight query on cold start and serves stale cache when warm")
    void getPendingMessagesCountShouldProtectAgainstCacheStampede() throws Exception {
        java.util.concurrent.CountDownLatch queryStartedLatch = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.CountDownLatch allowFinishLatch = new java.util.concurrent.CountDownLatch(1);

        when(jdbcTemplate.queryForObject(anyString(), eq(Long.class))).thenAnswer(inv -> {
            queryStartedLatch.countDown();
            boolean finished = allowFinishLatch.await(2, java.util.concurrent.TimeUnit.SECONDS);
            assertThat(finished).isTrue();
            return 99L;
        });

        java.util.concurrent.CompletableFuture<Long> future1 = java.util.concurrent.CompletableFuture.supplyAsync(() -> repository.getPendingMessagesCount());

        // Wait until first thread is actively executing DB query
        assertThat(queryStartedLatch.await(2, java.util.concurrent.TimeUnit.SECONDS)).isTrue();

        // Second concurrent thread calls during cold start: should wait for in-flight query rather than returning false 0L
        java.util.concurrent.CompletableFuture<Long> future2 = java.util.concurrent.CompletableFuture.supplyAsync(() -> repository.getPendingMessagesCount());

        // Release latch so in-flight query finishes
        allowFinishLatch.countDown();

        long firstResult = future1.get(2, java.util.concurrent.TimeUnit.SECONDS);
        long secondResult = future2.get(2, java.util.concurrent.TimeUnit.SECONDS);

        assertThat(firstResult).isEqualTo(99L);
        assertThat(secondResult).isEqualTo(99L);

        // Only 1 DB query was executed
        verify(jdbcTemplate, times(1)).queryForObject(anyString(), eq(Long.class));
    }

    @Test
    @DisplayName("Senior Refinements v22: releaseClaims does nothing when messageIds is null or empty")
    void releaseClaimsShouldNoOpWhenIdsListIsNullOrEmpty() {
        repository.releaseClaims(null);
        repository.releaseClaims(java.util.Collections.emptyList());
        repository.releaseClaims(null, Duration.ofMillis(500));
        repository.releaseClaims(java.util.Collections.emptyList(), Duration.ofMillis(500));

        verify(jdbcTemplate, never()).update(anyString(), any(Object[].class));
    }

    @Test
    @DisplayName("Senior Refinements v22: deleteSentBefore and deleteFailedBefore return 0 when no rows deleted")
    void deleteSentAndFailedBeforeShouldReturnZeroWhenEmpty() {
        when(jdbcTemplate.update(anyString(), any(org.springframework.jdbc.core.PreparedStatementSetter.class))).thenReturn(0);

        int deletedSent = repository.deleteSentBefore(Instant.now());
        int deletedFailed = repository.deleteFailedBefore(Instant.now());

        assertThat(deletedSent).isEqualTo(0);
        assertThat(deletedFailed).isEqualTo(0);
        verify(jdbcTemplate, times(2)).update(anyString(), any(org.springframework.jdbc.core.PreparedStatementSetter.class));
    }

    @Test
    @DisplayName("Senior Refinements v24: serializeHeaders handles null, empty, and sanitizes null chars")
    void serializeHeadersShouldHandleNullEmptyAndSanitizeNullChars() {
        assertThat(repository.serializeHeaders(null)).isEqualTo("{}");
        assertThat(repository.serializeHeaders(Collections.emptyMap())).isEqualTo("{}");

        Map<String, String> headers = Map.of("key1", "val\0ue1", "key2", "val\0ue2");
        String serialized = repository.serializeHeaders(headers);
        assertThat(serialized).doesNotContain("\\u0000");
        assertThat(serialized).doesNotContain("\0");
        assertThat(serialized).contains("value1");
        assertThat(serialized).contains("value2");
    }

    @Test
    @DisplayName("Senior Refinements v24: sanitizeText and sanitizeJsonb strip null bytes and handle null gracefully")
    void sanitizeTextShouldStripNullBytesAndHandleNullGracefully() {
        assertThat(PostgresOutboxRepository.sanitizeText(null)).isNull();
        assertThat(PostgresOutboxRepository.sanitizeText("safe text")).isEqualTo("safe text");
        assertThat(PostgresOutboxRepository.sanitizeText("hello \\u0000world\0")).isEqualTo("hello world");

        assertThat(PostgresOutboxRepository.sanitizeJsonb(null)).isNull();
        assertThat(PostgresOutboxRepository.sanitizeJsonb("{\"safe\":true}")).isEqualTo("{\"safe\":true}");
        assertThat(PostgresOutboxRepository.sanitizeJsonb("{\"null \\u0000byte\":true}")).isEqualTo("{\"null byte\":true}");
    }

    @Test
    @DisplayName("markAsFailed should enforce minimum backoff of 100ms when backoff duration is smaller")
    void markAsFailedShouldEnforceMinimumBackoffFloor() {
        UUID msgId = UUID.randomUUID();

        repository.markAsFailed(msgId, "timeout error", 3, Duration.ofMillis(10));

        verify(jdbcTemplate).update(anyString(), eq(3), eq(3), eq(3), eq(100L), eq("timeout error"), eq(msgId));
    }

    @Test
    @DisplayName("rescheduleMessage should enforce minimum backoff of 100ms when backoff duration is smaller")
    void rescheduleMessageShouldEnforceMinimumBackoffFloor() {
        UUID msgId = UUID.randomUUID();

        repository.rescheduleMessage(msgId, "transient error", Duration.ofMillis(20));

        verify(jdbcTemplate).update(anyString(), eq(100L), eq("transient error"), eq(msgId));
    }

    @Test
    @DisplayName("claimBatchForProcessing should enforce minimum lockDuration of 1000ms and default maxRetries to 5")
    void claimBatchShouldEnforceMinimumLockDurationAndDefaultMaxRetries() {
        when(jdbcTemplate.query(anyString(), anyRowMapper(), anyLong(), anyInt(), anyInt(), anyInt(), anyInt(), anyLong()))
                .thenReturn(Collections.emptyList());

        List<OutboxMessage> messages = repository.claimBatchForProcessing(15, Duration.ofMillis(50));

        assertThat(messages).isEmpty();
        verify(jdbcTemplate).query(anyString(), anyRowMapper(), eq(86400000L), eq(15), eq(5), eq(5), eq(5), eq(1000L));
    }

    @Test
    @DisplayName("executeWithLock rethrows exception from action directly")
    void executeWithLockShouldNotReExecuteActionWhenActionThrowsException() {
        int[] executions = new int[1];
        org.assertj.core.api.Assertions.assertThatThrownBy(() ->
                repository.executeWithLock("conn_lock", () -> {
                    executions[0]++;
                    throw new IllegalStateException("Direct action error");
                })
        ).isInstanceOf(IllegalStateException.class).hasMessage("Direct action error");

        assertThat(executions[0]).isEqualTo(1);
    }

    @Test
    @DisplayName("Senior Refinements v26: save sanitizes null bytes in topic and partitionKey to protect against UTF-8 0x00 DB abort")
    void saveShouldSanitizeNullBytesInTopicAndPartitionKey() {
        UUID id = UUID.randomUUID();
        OutboxMessage message = new OutboxMessage(
                id, "orders\0-topic\u0000", "PART\0-KEY\u0000-1", "{\"data\":\"test\"}",
                Map.of("h1", "v1"), OutboxStatus.PENDING, 0,
                Instant.now(), null, null, null
        );

        repository.save(message);

        verify(jdbcTemplate).update(
                anyString(),
                eq(id),
                eq("orders-topic"),
                eq("PART-KEY-1"),
                eq("{\"data\":\"test\"}"),
                eq("{\"h1\":\"v1\"}"),
                eq("PENDING"),
                eq(0),
                any(OffsetDateTime.class),
                org.mockito.ArgumentMatchers.isNull(),
                org.mockito.ArgumentMatchers.isNull(),
                org.mockito.ArgumentMatchers.isNull(),
                org.mockito.ArgumentMatchers.isNull()
        );
    }

    @Test
    @DisplayName("executeWithLock acquires PostgreSQL session advisory lock and unlocks in finally")
    void executeWithLockShouldAcquireSessionAdvisoryLock() throws Exception {
        try (java.sql.Connection con = org.mockito.Mockito.mock(java.sql.Connection.class)) {
            java.sql.PreparedStatement ps = org.mockito.Mockito.mock(java.sql.PreparedStatement.class);
            java.sql.ResultSet rs = org.mockito.Mockito.mock(java.sql.ResultSet.class);

            when(con.prepareStatement(anyString())).thenReturn(ps);
            when(ps.executeQuery()).thenReturn(rs);
            when(rs.next()).thenReturn(true);
            when(rs.getBoolean(1)).thenReturn(true);

            when(jdbcTemplate.execute(anyConnectionCallback())).thenAnswer(inv -> {
                ConnectionCallback<Boolean> callback = inv.getArgument(0);
                return callback.doInConnection(con);
            });

            boolean[] ran = new boolean[1];
            boolean result = repository.executeWithLock("idembox_outbox_cleanup", () -> ran[0] = true);

            assertThat(result).isTrue();
            assertThat(ran[0]).isTrue();
            verify(ps, org.mockito.Mockito.atLeastOnce()).setString(1, "outbox_messages:idembox_outbox_cleanup");
        }
    }

    @Test
    @DisplayName("Multiple repository instances do not share or corrupt any connection state")
    void multipleRepositoryInstancesShouldNotShareOrCorruptThreadLocalState() {
        PostgresOutboxRepository repo1 = new PostgresOutboxRepository(jdbcTemplate, new ObjectMapper(), "tenant_a.outbox");
        PostgresOutboxRepository repo2 = new PostgresOutboxRepository(jdbcTemplate, new ObjectMapper(), "tenant_b.outbox");

        boolean[] ran1 = new boolean[1];
        boolean[] ran2 = new boolean[1];

        repo1.executeWithLock("lock_a", () -> {
            ran1[0] = true;
            repo2.executeWithLock("lock_b", () -> ran2[0] = true);
        });

        assertThat(ran1[0]).isTrue();
        assertThat(ran2[0]).isTrue();
    }

    @Test
    @DisplayName("Senior Refinements v38: PostgresOutboxRepository uses custom schema and table prefix in SQL queries")
    void shouldSupportCustomSchemaAndTablePrefix() {
        PostgresOutboxRepository customRepo = new PostgresOutboxRepository(
                jdbcTemplate,
                new com.fasterxml.jackson.databind.ObjectMapper(),
                null,
                null,
                io.github.alexlitvinov.idembox.starter.metrics.IdemboxMetrics.noOp(),
                "tenant_1.app_outbox_messages"
        );

        assertThat(customRepo.getTableName()).isEqualTo("tenant_1.app_outbox_messages");

        UUID id = UUID.randomUUID();
        OutboxMessage message = new OutboxMessage(
                id, "orders", "key", "{}", Map.of(), OutboxStatus.PENDING, 0,
                Instant.now(), null, null, null
        );

        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        customRepo.save(message);

        verify(jdbcTemplate).update(sqlCaptor.capture(), eq(id), any(), any(), any(), any(), any(), anyInt(), any(), any(), any(), any(), any());
        assertThat(sqlCaptor.getValue()).contains("INSERT INTO \"tenant_1\".\"app_outbox_messages\" (");
    }

    @Test
    @DisplayName("Senior Refinements v40: deleteSentBefore respects time budget")
    void deleteSentBeforeShouldRespectTimeBudget() {
        when(jdbcTemplate.update(anyString(), any(org.springframework.jdbc.core.PreparedStatementSetter.class))).thenAnswer(inv -> {
            try {
                Thread.sleep(15);
            } catch (InterruptedException ignored) {}
            return 5000;
        });

        int deleted = repository.deleteSentBefore(Instant.now(), Duration.ofMillis(5));
        assertThat(deleted).isGreaterThanOrEqualTo(5000);
        verify(jdbcTemplate, atMost(5)).update(anyString(), any(org.springframework.jdbc.core.PreparedStatementSetter.class));
    }

    @Test
    @DisplayName("Senior Refinements v40: deleteFailedBefore respects time budget")
    void deleteFailedBeforeShouldRespectTimeBudget() {
        when(jdbcTemplate.update(anyString(), any(org.springframework.jdbc.core.PreparedStatementSetter.class))).thenAnswer(inv -> {
            try {
                Thread.sleep(15);
            } catch (InterruptedException ignored) {}
            return 5000;
        });

        int deleted = repository.deleteFailedBefore(Instant.now(), Duration.ofMillis(5));
        assertThat(deleted).isGreaterThanOrEqualTo(5000);
        verify(jdbcTemplate, atMost(5)).update(anyString(), any(org.springframework.jdbc.core.PreparedStatementSetter.class));
    }

    @Test
    @DisplayName("Senior Refinements v41: rescheduleMessage with maxRetries increments retry_count and handles PENDING/IN_PROGRESS")
    void rescheduleMessageWithMaxRetriesShouldIncrementRetryCount() {
        UUID id = UUID.randomUUID();
        repository.rescheduleMessage(id, "Persistent broker outage", Duration.ofSeconds(5), 10);

        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        verify(jdbcTemplate).update(sqlCaptor.capture(), eq(10), eq(10), eq(10), eq(5000L), eq("Persistent broker outage"), eq(id));

        assertThat(sqlCaptor.getValue())
                .contains("UPDATE \"outbox_messages\"")
                .contains("retry_count = retry_count + 1")
                .contains("status = CASE WHEN (retry_count + 1) >= ? THEN 'FAILED' ELSE 'PENDING' END")
                .contains("WHERE id = ? AND status IN ('PENDING', 'IN_PROGRESS')");
    }

    @Test
    @DisplayName("Senior Refinements Round 58: deleteSentBefore and deleteFailedBefore configure statement timeout locally on PreparedStatementSetter without mutating shared JdbcTemplate")
    void cleanupShouldSetQueryTimeoutLocallyWithoutMutatingSharedTemplate() throws java.sql.SQLException {
        ArgumentCaptor<org.springframework.jdbc.core.PreparedStatementSetter> pssCaptor =
                ArgumentCaptor.forClass(org.springframework.jdbc.core.PreparedStatementSetter.class);
        when(jdbcTemplate.update(anyString(), pssCaptor.capture())).thenReturn(0);

        repository.deleteSentBefore(Instant.now(), Duration.ofSeconds(10));
        verify(jdbcTemplate, never()).setQueryTimeout(anyInt());

        PreparedStatement mockPs = mock(PreparedStatement.class);
        pssCaptor.getValue().setValues(mockPs);
        verify(mockPs).setQueryTimeout(10);
        verify(mockPs).setObject(eq(1), any(OffsetDateTime.class));
    }

    @Test
    @DisplayName("executeWithLock executes successfully for schema-qualified table")
    void executeWithLockShouldPrioritizeTableSchemaOverConnectionSchema() {
        PostgresOutboxRepository tenantRepo = new PostgresOutboxRepository(
                jdbcTemplate,
                new com.fasterxml.jackson.databind.ObjectMapper(),
                null,
                null,
                io.github.alexlitvinov.idembox.starter.metrics.IdemboxMetrics.noOp(),
                "tenant_b.app_outbox_messages"
        );

        boolean[] ran = new boolean[1];
        boolean result = tenantRepo.executeWithLock("idembox_outbox_cleanup", () -> ran[0] = true);

        assertThat(result).isTrue();
        assertThat(ran[0]).isTrue();
        assertThat(tenantRepo.getTableName()).isEqualTo("tenant_b.app_outbox_messages");
    }

    @Test
    @DisplayName("executeWithLock executes cleanly for different repositories sharing schema with different tablePrefix")
    void executeWithLockShouldDifferentiateServicesWithDifferentTablePrefixesInSameSchema() {
        PostgresOutboxRepository orderRepo = new PostgresOutboxRepository(
                jdbcTemplate,
                new ObjectMapper(),
                "public.order_outbox_messages"
        );
        PostgresOutboxRepository paymentRepo = new PostgresOutboxRepository(
                jdbcTemplate,
                new ObjectMapper(),
                "public.payment_outbox_messages"
        );

        boolean[] orderRan = new boolean[1];
        boolean[] paymentRan = new boolean[1];

        orderRepo.executeWithLock("idembox_outbox_cleanup", () -> orderRan[0] = true);
        paymentRepo.executeWithLock("idembox_outbox_cleanup", () -> paymentRan[0] = true);

        assertThat(orderRan[0]).isTrue();
        assertThat(paymentRan[0]).isTrue();
    }

    @Test
    @DisplayName("Senior Refinements v43: serializeHeaders preserves multi-value headers as JSON arrays")
    void serializeHeadersShouldPreserveMultiValueHeaders() {
        Map<String, java.util.List<String>> multi = new java.util.LinkedHashMap<>();
        multi.put("X-Trace", java.util.List.of("step-1", "step-2"));
        multi.put("content-type", java.util.List.of("application/json"));

        String serialized = repository.serializeHeaders(null, multi);
        assertThat(serialized).contains("\"X-Trace\":[\"step-1\",\"step-2\"]");
        assertThat(serialized).contains("\"content-type\":\"application/json\"");
    }

    @Test
    @DisplayName("Senior Refinements v45: PostgresOutboxRepository safely quotes reserved keywords in schema and table")
    void shouldSafelyQuoteReservedKeywordsInSchemaAndTable() {
        PostgresOutboxRepository reservedRepo = new PostgresOutboxRepository(
                jdbcTemplate,
                new com.fasterxml.jackson.databind.ObjectMapper(),
                null,
                null,
                io.github.alexlitvinov.idembox.starter.metrics.IdemboxMetrics.noOp(),
                "user.order"
        );

        assertThat(reservedRepo.getTableName()).isEqualTo("user.order");

        UUID id = UUID.randomUUID();
        OutboxMessage message = new OutboxMessage(
                id, "orders", "key", "{}", Map.of(), OutboxStatus.PENDING, 0,
                Instant.now(), null, null, null
        );

        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        reservedRepo.save(message);

        verify(jdbcTemplate).update(sqlCaptor.capture(), eq(id), any(), any(), any(), any(), any(), anyInt(), any(), any(), any(), any(), any());
        assertThat(sqlCaptor.getValue()).contains("INSERT INTO \"user\".\"order\" (");
    }

    @Test
    @DisplayName("Senior Refinements v48: claimBatchForProcessing passes maxOutageDuration parameter to SQL query")
    void claimBatchShouldPassMaxOutageDurationParameter() {
        when(jdbcTemplate.query(anyString(), anyRowMapper(), anyLong(), anyInt(), anyInt(), anyInt(), anyInt(), anyLong()))
                .thenReturn(Collections.emptyList());

        List<OutboxMessage> messages = repository.claimBatchForProcessing(10, Duration.ofMinutes(1), 5, Duration.ofHours(2));

        assertThat(messages).isEmpty();
        // 2 hours = 7,200,000 ms
        verify(jdbcTemplate).query(anyString(), anyRowMapper(), eq(7200000L), eq(10), eq(5), eq(5), eq(5), eq(60000L));
    }

    @Test
    @DisplayName("Senior Refinements v49: claimBatchForProcessing passes 100 years sentinel when maxOutageDuration is ZERO or negative")
    void claimBatchShouldUseSentinelWhenMaxOutageDurationZeroOrNegative() {
        when(jdbcTemplate.query(anyString(), anyRowMapper(), anyLong(), anyInt(), anyInt(), anyInt(), anyInt(), anyLong()))
                .thenReturn(Collections.emptyList());

        // Test Duration.ZERO
        repository.claimBatchForProcessing(10, Duration.ofMinutes(1), 5, Duration.ZERO);
        verify(jdbcTemplate).query(anyString(), anyRowMapper(), eq(3153600000000L), eq(10), eq(5), eq(5), eq(5), eq(60000L));

        // Test negative duration
        repository.claimBatchForProcessing(10, Duration.ofMinutes(1), 5, Duration.ofMinutes(-1));
        verify(jdbcTemplate, times(2)).query(anyString(), anyRowMapper(), eq(3153600000000L), eq(10), eq(5), eq(5), eq(5), eq(60000L));
    }

    @Test
    @DisplayName("deleteSentBefore and deleteFailedBefore execute batches using cleanupJdbcTemplate directly")
    void cleanupShouldReuseLockedConnection() {
        when(jdbcTemplate.update(anyString(), any(org.springframework.jdbc.core.PreparedStatementSetter.class)))
                .thenReturn(10)
                .thenReturn(0);

        boolean result = repository.executeWithLock("idembox_outbox_cleanup", () -> {
            repository.deleteSentBefore(Instant.now());
            repository.deleteFailedBefore(Instant.now());
        });

        assertThat(result).isTrue();
        verify(jdbcTemplate, org.mockito.Mockito.atLeast(2)).update(anyString(), any(org.springframework.jdbc.core.PreparedStatementSetter.class));
    }

    @Test
    @DisplayName("executeWithLock executes action directly without session advisory locks to support PgBouncer transaction pooling")
    void executeWithLockExecutesDirectlyWithoutAdvisoryLocks() {
        boolean[] executed = new boolean[1];
        boolean result = repository.executeWithLock("idembox_outbox_cleanup", () -> executed[0] = true);
        assertThat(result).isTrue();
        assertThat(executed[0]).isTrue();
    }

    @Test
    @DisplayName("Senior Refinements Round 64: serializeHeaders preserves empty lists as [] and honors flat fallback")
    void shouldPreserveEmptyListsInMultiHeadersDuringSerialization() throws Exception {
        Map<String, String> headers = Map.of("key1", "val1", "emptyKey", "fallbackVal");
        Map<String, List<String>> multiHeaders = Map.of(
                "key1", List.of("val1-multi"),
                "emptyKey", Collections.emptyList(),
                "pureEmpty", Collections.emptyList()
        );

        String json = repository.serializeHeaders(headers, multiHeaders);
        com.fasterxml.jackson.databind.JsonNode node = new ObjectMapper().readTree(json);

        assertThat(node.has("key1")).isTrue();
        assertThat(node.get("key1").asText()).isEqualTo("val1-multi");
        assertThat(node.has("emptyKey")).isTrue();
        assertThat(node.get("emptyKey").asText()).isEqualTo("fallbackVal");
        assertThat(node.has("pureEmpty")).isTrue();
        assertThat(node.get("pureEmpty").isArray()).isTrue();
        assertThat(node.get("pureEmpty").isEmpty()).isTrue();
    }

    @Test
    @DisplayName("Senior Refinements Round 60 & Round 67: schema-postgresql.sql defines functional expression indexes for COALESCE cleanup")
    void schemaShouldDefineFunctionalCoalesceIndexesForCleanup() throws Exception {
        org.springframework.core.io.ClassPathResource resource =
                new org.springframework.core.io.ClassPathResource("db/migration/idembox/schema-postgresql.sql");
        String sql = new String(resource.getInputStream().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);

        assertThat(sql).contains("CREATE INDEX IF NOT EXISTS idx_outbox_sent_cleanup");
        assertThat(sql).contains("((COALESCE(sent_at, created_at)), id)");
        assertThat(sql).contains("CREATE INDEX IF NOT EXISTS idx_outbox_failed_cleanup");
        assertThat(sql).contains("((COALESCE(failed_at, created_at)), id)");
    }

    @Test
    @DisplayName("Senior Refinements Round 64: deleteSentBefore and deleteFailedBefore support configurable batchSize and maxBatches")
    void shouldSupportConfigurableBatchSizeAndMaxBatchesInCleanup() {
        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        when(jdbcTemplate.update(sqlCaptor.capture(), any(org.springframework.jdbc.core.PreparedStatementSetter.class)))
                .thenReturn(0);

        repository.deleteSentBefore(Instant.now().minusSeconds(3600), Duration.ofSeconds(10), 2000, 50);
        assertThat(sqlCaptor.getValue())
                .contains("LIMIT 2000")
                .contains("WHERE status = 'SENT'");

        repository.deleteFailedBefore(Instant.now().minusSeconds(3600), Duration.ofSeconds(10), 1000, 30);
        assertThat(sqlCaptor.getValue())
                .contains("LIMIT 1000")
                .contains("WHERE status = 'FAILED'");
    }

    @Test
    @DisplayName("Senior Refinements Round 68: markAsSent with lockedUntil includes lease verification in SQL")
    void markAsSentWithLockedUntilShouldIncludeLeaseCheckInSql() {
        UUID id = UUID.randomUUID();
        Instant sentAt = Instant.now();
        Instant lockedUntil = Instant.now().plusSeconds(120);
        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        when(jdbcTemplate.update(sqlCaptor.capture(), any(), eq(id), any(), any(), any())).thenReturn(1);

        boolean updated = repository.markAsSent(id, sentAt, lockedUntil);

        assertThat(updated).isTrue();
        assertThat(sqlCaptor.getValue())
                .contains("WHERE id = ? AND status = 'IN_PROGRESS'")
                .contains("AND (next_retry_at = ? OR (next_retry_at >= ? - INTERVAL '1 microsecond' AND next_retry_at <= ? + INTERVAL '1 microsecond'))");
    }

    @Test
    @DisplayName("Senior Refinements Round 68: markAsSent returns false when lease expired or stolen")
    void markAsSentShouldReturnFalseWhenLeaseExpiredOrStolen() {
        UUID id = UUID.randomUUID();
        when(jdbcTemplate.update(anyString(), any(), eq(id), any(), any(), any())).thenReturn(0);

        boolean updated = repository.markAsSent(id, Instant.now(), Instant.now().plusSeconds(60));

        assertThat(updated).isFalse();
    }

    @Test
    @DisplayName("Senior Refinements Round 68: markAsFailed with lockedUntil includes lease verification in SQL")
    void markAsFailedWithLockedUntilShouldIncludeLeaseCheckInSql() {
        UUID id = UUID.randomUUID();
        Instant lockedUntil = Instant.now().plusSeconds(120);
        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        when(jdbcTemplate.update(sqlCaptor.capture(), eq(3), eq(3), eq(3), anyLong(), eq("timeout"), eq(id), any(), any(), any())).thenReturn(1);

        boolean updated = repository.markAsFailed(id, "timeout", 3, Duration.ofSeconds(5), lockedUntil);

        assertThat(updated).isTrue();
        assertThat(sqlCaptor.getValue())
                .contains("WHERE id = ? AND status = 'IN_PROGRESS'")
                .contains("AND (next_retry_at = ? OR (next_retry_at >= ? - INTERVAL '1 microsecond' AND next_retry_at <= ? + INTERVAL '1 microsecond'))");
    }

    @Test
    @DisplayName("Senior Refinements Round 68: markAsFailed returns false when lease expired or stolen")
    void markAsFailedShouldReturnFalseWhenLeaseExpiredOrStolen() {
        UUID id = UUID.randomUUID();
        when(jdbcTemplate.update(anyString(), anyInt(), anyInt(), anyInt(), anyLong(), anyString(), eq(id), any(), any(), any())).thenReturn(0);

        boolean updated = repository.markAsFailed(id, "error", 3, Duration.ofSeconds(1), Instant.now());

        assertThat(updated).isFalse();
    }

    @Test
    @DisplayName("Senior Refinements Round 68: rescheduleMessage with lockedUntil includes lease verification in SQL")
    void rescheduleMessageWithLockedUntilShouldIncludeLeaseCheckInSql() {
        UUID id = UUID.randomUUID();
        Instant lockedUntil = Instant.now().plusSeconds(120);
        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        when(jdbcTemplate.update(sqlCaptor.capture(), anyLong(), eq("broker down"), eq(id), any(), any(), any())).thenReturn(1);

        boolean updated = repository.rescheduleMessage(id, "broker down", Duration.ofSeconds(5), lockedUntil);

        assertThat(updated).isTrue();
        assertThat(sqlCaptor.getValue())
                .contains("WHERE id = ? AND status = 'IN_PROGRESS'")
                .contains("AND (next_retry_at = ? OR (next_retry_at >= ? - INTERVAL '1 microsecond' AND next_retry_at <= ? + INTERVAL '1 microsecond'))");
    }

    @Test
    @DisplayName("Senior Refinements Round 68: rescheduleMessage returns false when lease expired or stolen")
    void rescheduleMessageShouldReturnFalseWhenLeaseExpiredOrStolen() {
        UUID id = UUID.randomUUID();
        when(jdbcTemplate.update(anyString(), anyLong(), anyString(), eq(id), any(), any(), any())).thenReturn(0);

        boolean updated = repository.rescheduleMessage(id, "broker down", Duration.ofSeconds(5), Instant.now());

        assertThat(updated).isFalse();
    }

    @Test
    @DisplayName("Senior Refinements Round 68: releaseClaims with lockedUntil includes lease verification in SQL")
    void releaseClaimsWithLockedUntilShouldIncludeLeaseCheckInSql() {
        UUID id = UUID.randomUUID();
        Instant lockedUntil = Instant.now().plusSeconds(120);
        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        when(jdbcTemplate.update(sqlCaptor.capture(), eq(5000L), any(), any(), any(), any())).thenReturn(1);

        repository.releaseClaims(List.of(id), Duration.ofSeconds(5), lockedUntil);

        assertThat(sqlCaptor.getValue())
                .contains("WHERE status = 'IN_PROGRESS' AND id = ANY(?::uuid[])")
                .contains("AND (next_retry_at = ? OR (next_retry_at >= ? - INTERVAL '1 microsecond' AND next_retry_at <= ? + INTERVAL '1 microsecond'))");
    }

    @Test
    @DisplayName("Senior Refinements Round 68: claimBatch replenishes up to batchSize when candidates transition to FAILED")
    void claimBatchShouldReplenishWhenCandidatesTransitionToFailed() {
        UUID failedId1 = UUID.randomUUID();
        UUID failedId2 = UUID.randomUUID();
        UUID activeId1 = UUID.randomUUID();
        UUID activeId2 = UUID.randomUUID();
        UUID activeId3 = UUID.randomUUID();
        Instant now = Instant.now();

        OutboxMessage failedMsg1 = new OutboxMessage(failedId1, "t", "k1", "{}", Collections.emptyMap(), OutboxStatus.FAILED, 5, now, null, null, null);
        OutboxMessage failedMsg2 = new OutboxMessage(failedId2, "t", "k2", "{}", Collections.emptyMap(), OutboxStatus.FAILED, 5, now, null, null, null);
        OutboxMessage activeMsg1 = new OutboxMessage(activeId1, "t", "k3", "{}", Collections.emptyMap(), OutboxStatus.IN_PROGRESS, 1, now, null, null, null);
        OutboxMessage activeMsg2 = new OutboxMessage(activeId2, "t", "k4", "{}", Collections.emptyMap(), OutboxStatus.IN_PROGRESS, 1, now.plusMillis(10), null, null, null);
        OutboxMessage activeMsg3 = new OutboxMessage(activeId3, "t", "k5", "{}", Collections.emptyMap(), OutboxStatus.IN_PROGRESS, 1, now.plusMillis(20), null, null, null);

        // First query: batchSize=3, returns 2 FAILED messages and 1 IN_PROGRESS message (total 3, hits limit)
        // Second query: remainingNeeded=2, returns 2 IN_PROGRESS messages (total 2, hits limit)
        when(jdbcTemplate.query(anyString(), anyRowMapper(), anyLong(), eq(3), anyInt(), anyInt(), anyInt(), anyLong()))
                .thenReturn(List.of(failedMsg1, failedMsg2, activeMsg1));
        when(jdbcTemplate.query(anyString(), anyRowMapper(), anyLong(), eq(2), anyInt(), anyInt(), anyInt(), anyLong()))
                .thenReturn(List.of(activeMsg2, activeMsg3));

        List<OutboxMessage> result = repository.claimBatchForProcessing(3, Duration.ofMinutes(2), 5);

        // Result must replenish up to batchSize=3 with active messages, skipping FAILED
        assertThat(result).containsExactly(activeMsg1, activeMsg2, activeMsg3);
    }

    @Test
    @DisplayName("Senior Refinements Round 70: claimBatchForProcessing FIFO outage bypass does not require earlier.last_error IS NOT NULL")
    void claimBatchFifoBypassShouldNotRequireEarlierLastErrorNotNull() {
        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        when(jdbcTemplate.query(sqlCaptor.capture(), anyRowMapper(), anyLong(), anyInt(), anyInt(), anyInt(), anyInt(), anyLong()))
                .thenReturn(Collections.emptyList());

        repository.claimBatchForProcessing(10, Duration.ofMinutes(2), 5, Duration.ofHours(24));

        assertThat(sqlCaptor.getValue())
                .contains("AND NOT (earlier.created_at <= CURRENT_TIMESTAMP - (? * INTERVAL '1 millisecond'))")
                .doesNotContain("earlier.last_error IS NOT NULL");
    }

    @Test
    @DisplayName("deleteSentBefore executes batches using cleanupJdbcTemplate and FOR UPDATE SKIP LOCKED without connection hijacking")
    void shouldExecuteBatchesUsingCleanupJdbcTemplateWithoutConnectionHijacking() {
        when(jdbcTemplate.update(contains("WITH to_delete AS"), any(org.springframework.jdbc.core.PreparedStatementSetter.class)))
                .thenReturn(100)
                .thenReturn(50)
                .thenReturn(0);

        int deleted = repository.deleteSentBefore(Instant.now());

        assertThat(deleted).isEqualTo(150);
        verify(jdbcTemplate, times(3)).update(contains("WITH to_delete AS"), any(org.springframework.jdbc.core.PreparedStatementSetter.class));
    }

    @Test
    @DisplayName("deleteSentBefore inside active Spring transaction executes batches in lockTransactionTemplate without hijacking outer connection")
    void shouldExecuteBatchesInRequiresNewWithoutHijackingOuterConnectionWhenCalledInsideActiveTransaction() {
        org.springframework.transaction.PlatformTransactionManager txManager = mock(org.springframework.transaction.PlatformTransactionManager.class);
        org.springframework.transaction.TransactionStatus txStatus = mock(org.springframework.transaction.TransactionStatus.class);
        when(txManager.getTransaction(any())).thenReturn(txStatus);
        org.springframework.transaction.support.TransactionTemplate tt = new org.springframework.transaction.support.TransactionTemplate(txManager);
        PostgresOutboxRepository repoWithTx = new PostgresOutboxRepository(jdbcTemplate, new ObjectMapper(), tt);

        when(jdbcTemplate.update(contains("WITH to_delete AS"), any(org.springframework.jdbc.core.PreparedStatementSetter.class)))
                .thenReturn(20)
                .thenReturn(0);

        org.springframework.transaction.support.TransactionSynchronizationManager.setActualTransactionActive(true);
        try {
            int deleted = repoWithTx.deleteSentBefore(Instant.now());
            assertThat(deleted).isEqualTo(20);
            verify(txManager, atLeastOnce()).getTransaction(org.mockito.ArgumentMatchers.argThat(def ->
                    def.getPropagationBehavior() == org.springframework.transaction.TransactionDefinition.PROPAGATION_REQUIRES_NEW));
        } finally {
            org.springframework.transaction.support.TransactionSynchronizationManager.setActualTransactionActive(false);
        }
    }

    @Test
    @DisplayName("Constructor with PlatformTransactionManager initializes claim and lock TransactionTemplates using the provided manager")
    void shouldInitializeTransactionTemplatesFromPlatformTransactionManager() {
        org.springframework.transaction.PlatformTransactionManager txManager = mock(org.springframework.transaction.PlatformTransactionManager.class);
        PostgresOutboxRepository repo = new PostgresOutboxRepository(jdbcTemplate, new ObjectMapper(), txManager);

        assertThat(repo.getTransactionTemplate()).isNotNull();
        assertThat(repo.getTransactionTemplate().getTransactionManager()).isSameAs(txManager);
        assertThat(repo.getTransactionTemplate().getPropagationBehavior())
                .isEqualTo(org.springframework.transaction.TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        assertThat(repo.getTransactionTemplate().getTimeout()).isEqualTo(5);

        assertThat(repo.getLockTransactionTemplate()).isNotNull();
        assertThat(repo.getLockTransactionTemplate().getTransactionManager()).isSameAs(txManager);
        assertThat(repo.getLockTransactionTemplate().getPropagationBehavior())
                .isEqualTo(org.springframework.transaction.TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        assertThat(repo.getLockTransactionTemplate().getTimeout())
                .isEqualTo(org.springframework.transaction.TransactionDefinition.TIMEOUT_DEFAULT);
    }

    @Test
    @DisplayName("Default constructor should not create duplicate DataSourceTransactionManager when no transaction manager is provided")
    void defaultConstructorShouldNotCreateDuplicateDataSourceTransactionManager() {
        javax.sql.DataSource ds = mock(javax.sql.DataSource.class);
        JdbcTemplate jdbcWithDs = new JdbcTemplate(ds);

        PostgresOutboxRepository repo = new PostgresOutboxRepository(jdbcWithDs, new ObjectMapper());

        assertThat(repo.getTransactionTemplate()).isNull();
        assertThat(repo.getLockTransactionTemplate()).isNull();
    }

    @Test
    @DisplayName("executeWithLock executes action directly and propagates errors without connection leaks")
    void shouldPropagateErrorsDirectlyWithoutConnectionLeaks() {
        org.assertj.core.api.Assertions.assertThatThrownBy(() ->
                repository.executeWithLock("error_lock", () -> {
                    throw new AssertionError("Simulated JVM / assertion error");
                })
        ).isInstanceOf(AssertionError.class).hasMessage("Simulated JVM / assertion error");
    }

    @Test
    @DisplayName("Senior Refinements Round 77: markAsSent, markAsFailed, rescheduleMessage, and releaseClaims execute in PROPAGATION_REQUIRES_NEW TransactionTemplate")
    void statusTransitionsShouldExecuteInRequiresNewTransactionTemplate() {
        org.springframework.transaction.PlatformTransactionManager txManager = mock(org.springframework.transaction.PlatformTransactionManager.class);
        org.springframework.transaction.TransactionStatus txStatus = mock(org.springframework.transaction.TransactionStatus.class);
        when(txManager.getTransaction(any())).thenReturn(txStatus);

        org.springframework.transaction.support.TransactionTemplate claimTt = new org.springframework.transaction.support.TransactionTemplate(txManager);
        claimTt.setPropagationBehavior(org.springframework.transaction.TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        PostgresOutboxRepository repoWithTx = new PostgresOutboxRepository(jdbcTemplate, new ObjectMapper(), claimTt);

        UUID id = UUID.randomUUID();
        Instant now = Instant.now();
        when(jdbcTemplate.update(anyString(), any(Object[].class))).thenReturn(1);

        assertThat(repoWithTx.markAsSent(id, now, now)).isTrue();
        assertThat(repoWithTx.markAsFailed(id, "err", 3, Duration.ofSeconds(1), now)).isTrue();
        assertThat(repoWithTx.rescheduleMessage(id, "err", Duration.ofSeconds(1), now)).isTrue();
        assertThat(repoWithTx.rescheduleMessage(id, "err", Duration.ofSeconds(1), 3, now)).isTrue();
        repoWithTx.releaseClaims(List.of(id), Duration.ofSeconds(1), now);

        verify(txManager, times(5)).getTransaction(any());
        verify(txManager, times(5)).commit(txStatus);
    }

    @Test
    @DisplayName("Senior Refinements Round 78: Custom TransactionTemplates enforce PROPAGATION_REQUIRES_NEW and executeInRequiresNew uses lockTransactionTemplate without 5s timeout")
    void customTransactionTemplatesShouldEnforceRequiresNewAndAvoid5sTimeoutOnStatusUpdates() {
        org.springframework.transaction.PlatformTransactionManager txManager = mock(org.springframework.transaction.PlatformTransactionManager.class);
        org.springframework.transaction.TransactionStatus txStatus = mock(org.springframework.transaction.TransactionStatus.class);
        when(txManager.getTransaction(any())).thenReturn(txStatus);

        // Pass a default TransactionTemplate (which has PROPAGATION_REQUIRED, readOnly=true, timeout=5)
        org.springframework.transaction.support.TransactionTemplate defaultTt = new org.springframework.transaction.support.TransactionTemplate(txManager);
        defaultTt.setPropagationBehavior(org.springframework.transaction.TransactionDefinition.PROPAGATION_REQUIRED);
        defaultTt.setReadOnly(true);
        defaultTt.setTimeout(5);

        PostgresOutboxRepository repo = new PostgresOutboxRepository(jdbcTemplate, new ObjectMapper(), defaultTt);

        assertThat(repo.getTransactionTemplate().getPropagationBehavior())
                .isEqualTo(org.springframework.transaction.TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        assertThat(repo.getTransactionTemplate().isReadOnly()).isFalse();
        assertThat(repo.getTransactionTemplate().getTimeout()).isEqualTo(5);

        assertThat(repo.getLockTransactionTemplate().getPropagationBehavior())
                .isEqualTo(org.springframework.transaction.TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        assertThat(repo.getLockTransactionTemplate().isReadOnly()).isFalse();
        assertThat(repo.getLockTransactionTemplate().getTimeout())
                .isEqualTo(org.springframework.transaction.TransactionDefinition.TIMEOUT_DEFAULT);

        UUID id = UUID.randomUUID();
        Instant now = Instant.now();
        when(jdbcTemplate.update(anyString(), any(Object[].class))).thenReturn(1);

        assertThat(repo.markAsSent(id, now, now)).isTrue();

        ArgumentCaptor<org.springframework.transaction.TransactionDefinition> defCaptor =
                ArgumentCaptor.forClass(org.springframework.transaction.TransactionDefinition.class);
        verify(txManager).getTransaction(defCaptor.capture());
        assertThat(defCaptor.getValue().getPropagationBehavior())
                .isEqualTo(org.springframework.transaction.TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        assertThat(defCaptor.getValue().getTimeout())
                .isEqualTo(org.springframework.transaction.TransactionDefinition.TIMEOUT_DEFAULT);
    }

    @Test
    @DisplayName("Issue 1: claimBatch subquery must block subsequent partition messages if earlier is PENDING even with future next_retry_at")
    void claimBatchShouldEnforceFifoBlockingForPendingEarlierMessages() {
        ArgumentCaptor<String> sqlCaptor = ArgumentCaptor.forClass(String.class);
        when(jdbcTemplate.query(sqlCaptor.capture(), anyRowMapper(), anyLong(), anyInt(), anyInt(), anyInt(), anyInt(), anyLong()))
                .thenReturn(List.of());

        repository.claimBatchForProcessing(10, Duration.ofMinutes(1), 5, Duration.ofMinutes(5));

        String sql = sqlCaptor.getValue();
        assertThat(sql).contains("earlier.status IN ('PENDING', 'IN_PROGRESS')");
    }

    @Test
    @DisplayName("Issue 2: deleteSentBefore and deleteFailedBefore must not commit activeCon inside batch loop to preserve advisory transaction lock")
    void cleanupMustNotCommitActiveConInsideBatchLoop() throws Exception {
        java.sql.Connection mockCon = mock(java.sql.Connection.class);
        java.sql.PreparedStatement mockPs = mock(java.sql.PreparedStatement.class);
        when(mockCon.prepareStatement(anyString())).thenReturn(mockPs);
        when(mockPs.executeUpdate()).thenReturn(10, 0, 10, 0);

        PostgresAdvisoryLockManager.setActiveLockConnectionForTest(mockCon);
        try {
            int deletedSent = repository.deleteSentBefore(Instant.now().minusSeconds(3600), Duration.ofSeconds(10), 100, 5);
            assertThat(deletedSent).isEqualTo(10);
            verify(mockCon, never()).commit();

            int deletedFailed = repository.deleteFailedBefore(Instant.now().minusSeconds(3600), Duration.ofSeconds(10), 100, 5);
            assertThat(deletedFailed).isEqualTo(10);
            verify(mockCon, never()).commit();
        } finally {
            PostgresAdvisoryLockManager.setActiveLockConnectionForTest(null);
        }
    }

    @Test
    @DisplayName("Issue 1: claimBatch terminates after MAX_CLAIM_ITERATIONS (5) iterations to avoid runaway loops")
    void claimBatchShouldRespectMaxClaimIterationsCap() {
        UUID failedId = UUID.randomUUID();
        Instant now = Instant.now();
        OutboxMessage failedMsg = new OutboxMessage(failedId, "t", "k1", "{}", Collections.emptyMap(), OutboxStatus.FAILED, 5, now, null, null, null);

        // Every query invocation returns a batch satisfying remainingNeeded (1) with FAILED message
        when(jdbcTemplate.query(anyString(), anyRowMapper(), anyLong(), anyInt(), anyInt(), anyInt(), anyInt(), anyLong()))
                .thenReturn(List.of(failedMsg));

        List<OutboxMessage> result = repository.claimBatchForProcessing(1, Duration.ofMinutes(2), 5);

        // Active messages list is empty, and query was executed exactly 5 times (MAX_CLAIM_ITERATIONS)
        assertThat(result).isEmpty();
        verify(jdbcTemplate, times(5)).query(anyString(), anyRowMapper(), anyLong(), eq(1), eq(5), eq(5), eq(5), anyLong());
    }

    @Test
    @DisplayName("Issue 7: releaseClaims preserves existing future exponential backoff via GREATEST")
    void releaseClaimsShouldPreserveFutureBackoffViaGreatest() {
        UUID msgId = UUID.randomUUID();
        when(jdbcTemplate.update(anyString(), anyLong(), any(UUID[].class))).thenReturn(1);

        repository.releaseClaims(List.of(msgId), Duration.ZERO);

        verify(jdbcTemplate).update(
                argThat((String sql) -> sql.contains("GREATEST(COALESCE(next_retry_at, CURRENT_TIMESTAMP), CURRENT_TIMESTAMP + (? * INTERVAL '1 millisecond'))")),
                eq(0L),
                any(UUID[].class)
        );
    }
}
