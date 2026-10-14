package io.github.alexlitvinov.idembox.starter.poller;

import io.github.alexlitvinov.idembox.core.model.OutboxMessage;
import io.github.alexlitvinov.idembox.core.model.OutboxStatus;
import io.github.alexlitvinov.idembox.core.repository.OutboxRepository;
import io.github.alexlitvinov.idembox.starter.config.IdemboxProperties;
import io.github.alexlitvinov.idembox.starter.metrics.IdemboxMetrics;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.support.SendResult;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OutboxPollerTest {

    private interface ObjectProducerFactory extends org.springframework.kafka.core.ProducerFactory<Object, Object> {}

    private static abstract class ObjectKafkaTemplate extends org.springframework.kafka.core.KafkaTemplate<Object, Object> {
        protected ObjectKafkaTemplate() {
            super(org.mockito.Mockito.mock(ObjectProducerFactory.class));
        }
    }

    private static abstract class ObjectSendResult extends SendResult<Object, Object> {
        protected ObjectSendResult() {
            super(null, null);
        }
    }

    private static ProducerRecord<Object, Object> anyProducerRecord() {
        return any();
    }

    @Mock
    private OutboxRepository outboxRepository;

    @Mock
    private KafkaOperations<Object, Object> kafkaOperations;

    private IdemboxProperties properties;
    private OutboxPoller outboxPoller;

    @BeforeEach
    void setUp() {
        properties = new IdemboxProperties();
        properties.getOutbox().setMaxRetries(3);
        properties.getOutbox().setInitialBackoff(Duration.ofMillis(100));
        properties.getOutbox().setBatchSize(10);

        outboxPoller = new OutboxPoller(outboxRepository, kafkaOperations, properties, IdemboxMetrics.noOp(), Runnable::run);
        outboxPoller.start();
    }

    @Test
    @DisplayName("Should successfully publish claimed messages to Kafka and mark as SENT")
    void shouldPublishSuccessfully() {
        UUID msgId = UUID.randomUUID();
        OutboxMessage message = new OutboxMessage(
                msgId, "orders-topic", "ORD-1", "{\"amount\":100}",
                Collections.emptyMap(), OutboxStatus.IN_PROGRESS, 0,
                Instant.now(), null, null, null
        );

        when(outboxRepository.claimBatchForProcessing(anyInt(), any(Duration.class), anyInt(), any(Duration.class)))
                .thenReturn(List.of(message))
                .thenReturn(Collections.emptyList());

        RecordMetadata metadata = new RecordMetadata(new TopicPartition("orders-topic", 0), 0, 0, 0, 0, 0);
        SendResult<Object, Object> sendResult = new SendResult<>(new ProducerRecord<>("orders-topic", "ORD-1", "{\"amount\":100}"), metadata);
        when(kafkaOperations.send(anyProducerRecord())).thenReturn(CompletableFuture.completedFuture(sendResult));

        int processed = outboxPoller.pollAndPublishBatch();

        assertThat(processed).isEqualTo(1);
        verify(outboxRepository).markAsSent(eq(msgId), any(Instant.class));
        verify(outboxRepository, never()).markAsFailed(any(), any(), any(int.class), any());
    }

    @Test
    @DisplayName("Poison Pill & Backoff: Failed Kafka send should calculate exponential backoff and pass maxRetries")
    void shouldHandleFailedSendWithBackoffAndMaxRetries() {
        UUID msgId = UUID.randomUUID();
        OutboxMessage message = new OutboxMessage(
                msgId, "poison-topic", "ORD-FAIL", "{\"bad\":true}",
                Collections.emptyMap(), OutboxStatus.IN_PROGRESS, 2, // already retried 2 times
                Instant.now(), null, null, null
        );

        when(outboxRepository.claimBatchForProcessing(anyInt(), any(Duration.class), anyInt(), any(Duration.class)))
                .thenReturn(List.of(message))
                .thenReturn(Collections.emptyList());

        CompletableFuture<SendResult<Object, Object>> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new RuntimeException("Kafka broker down"));
        when(kafkaOperations.send(anyProducerRecord())).thenReturn(failedFuture);

        int processed = outboxPoller.pollAndPublishBatch();

        assertThat(processed).isEqualTo(1);
        verify(outboxRepository, never()).markAsSent(any(), any());

        ArgumentCaptor<Duration> backoffCaptor = ArgumentCaptor.forClass(Duration.class);
        verify(outboxRepository).markAsFailed(
                eq(msgId),
                any(String.class),
                eq(3), // maxRetries
                backoffCaptor.capture()
        );

        // initialBackoff = 100ms, retryCount = 2, multiplier = 2.0 -> 100 * 2^2 = 400ms (Jitter 80%-120% -> 320ms - 480ms)
        assertThat(backoffCaptor.getValue().toMillis()).isBetween(320L, 480L);
    }

    @Test
    @DisplayName("Fatal Kafka Error: RecordTooLargeException should transition directly to FAILED on 1st attempt")
    void shouldTransitionDirectlyToFailedOnFatalProducerErrors() {
        UUID msgId = UUID.randomUUID();
        OutboxMessage message = new OutboxMessage(
                msgId, "large-topic", "ORD-HUGE", "{\"data\":\"large\"}",
                Collections.emptyMap(), OutboxStatus.IN_PROGRESS, 0,
                Instant.now(), null, null, null
        );

        when(outboxRepository.claimBatchForProcessing(anyInt(), any(Duration.class), anyInt(), any(Duration.class)))
                .thenReturn(List.of(message))
                .thenReturn(Collections.emptyList());

        CompletableFuture<SendResult<Object, Object>> fatalFuture = new CompletableFuture<>();
        fatalFuture.completeExceptionally(new org.apache.kafka.common.errors.RecordTooLargeException("Message size is 5MB"));
        when(kafkaOperations.send(anyProducerRecord())).thenReturn(fatalFuture);

        int processed = outboxPoller.pollAndPublishBatch();

        assertThat(processed).isEqualTo(1);
        verify(outboxRepository, never()).markAsSent(any(), any());
        verify(outboxRepository).markAsFailed(
                eq(msgId),
                anyString(),
                eq(1), // immediate transition to FAILED
                eq(Duration.ZERO)
        );
    }

    @Test
    @DisplayName("calculateBackoff: should not truncate lower 20% of jitter distribution on initial backoff")
    void shouldNotTruncateJitterDistribution() {
        properties.getOutbox().setInitialBackoff(Duration.ofSeconds(1)); // 1000ms
        properties.getOutbox().setMaxBackoff(Duration.ofMinutes(10));
        properties.getOutbox().setBackoffMultiplier(2.0);

        boolean sawBelow1000 = false;
        boolean sawAbove1000 = false;

        // With 1000ms base and 800..1200ms jitter, half the values are < 1000ms and half > 1000ms
        for (int i = 0; i < 100; i++) {
            long ms = outboxPoller.calculateBackoff(0).toMillis();
            assertThat(ms).isBetween(800L, 1200L);
            if (ms < 1000L) {
                sawBelow1000 = true;
            }
            if (ms > 1000L) {
                sawAbove1000 = true;
            }
        }
        assertThat(sawBelow1000).isTrue();
        assertThat(sawAbove1000).isTrue();
    }

    @Test
    @DisplayName("Fail-Fast: Broker outage on first message should halt batch and protect subsequent messages")
    void shouldHaltBatchProcessingOnBrokerOutageAndProtectRemainingMessages() {
        UUID msgId1 = UUID.randomUUID();
        UUID msgId2 = UUID.randomUUID();
        OutboxMessage msg1 = new OutboxMessage(
                msgId1, "orders-topic", "KEY-1", "{\"n\":1}",
                Collections.emptyMap(), OutboxStatus.IN_PROGRESS, 0,
                Instant.now(), null, null, null
        );
        OutboxMessage msg2 = new OutboxMessage(
                msgId2, "orders-topic", "KEY-2", "{\"n\":2}",
                Collections.emptyMap(), OutboxStatus.IN_PROGRESS, 0,
                Instant.now(), null, null, null
        );

        when(outboxRepository.claimBatchForProcessing(anyInt(), any(Duration.class), anyInt(), any(Duration.class)))
                .thenReturn(List.of(msg1, msg2));

        CompletableFuture<SendResult<Object, Object>> timeoutFuture = new CompletableFuture<>();
        timeoutFuture.completeExceptionally(new org.apache.kafka.common.errors.TimeoutException("Kafka broker disconnected"));
        when(kafkaOperations.send(anyProducerRecord())).thenReturn(timeoutFuture);

        int processed = outboxPoller.pollAndPublishBatch();

        // Exactly 1 message was attempted, msg1 was rescheduled without retry penalty, msg2 claims released with cooldown deferral
        assertThat(processed).isEqualTo(1);
        ArgumentCaptor<Duration> deferralCaptor = ArgumentCaptor.forClass(Duration.class);
        verify(outboxRepository).rescheduleMessage(eq(msgId1), any(), deferralCaptor.capture());
        assertThat(deferralCaptor.getValue().toMillis()).isGreaterThanOrEqualTo((long) (properties.getOutbox().getBrokerCooldown().toMillis() * 0.8));
        verify(outboxRepository, never()).markAsFailed(eq(msgId1), any(), any(int.class), any());
        verify(outboxRepository, never()).markAsFailed(eq(msgId2), any(), any(int.class), any());
        verify(outboxRepository, never()).markAsSent(any(), any());
        // Unattempted message was immediately released back to PENDING with jittered broker cooldown deferral
        ArgumentCaptor<Duration> releaseCooldownCaptor = ArgumentCaptor.forClass(Duration.class);
        verify(outboxRepository).releaseClaims(eq(List.of(msgId2)), releaseCooldownCaptor.capture());
        assertThat(releaseCooldownCaptor.getValue().toMillis()).isBetween(
                (long) (properties.getOutbox().getBrokerCooldown().toMillis() * 0.8),
                (long) (properties.getOutbox().getBrokerCooldown().toMillis() * 1.2) + 1
        );
        // Circuit breaker cooldown activated
        assertThat(outboxPoller.getBrokerCooldownUntilMs()).isGreaterThan(System.currentTimeMillis());
    }

    @Test
    @DisplayName("Backlog Draining: Should drain consecutive full batches up to maxDrainBatches")
    void shouldDrainConsecutiveFullBatches() {
        properties.getOutbox().setBatchSize(2);
        properties.getOutbox().setMaxDrainBatches(3);

        UUID id1 = UUID.randomUUID();
        UUID id2 = UUID.randomUUID();
        UUID id3 = UUID.randomUUID();

        OutboxMessage m1 = new OutboxMessage(id1, "t", "k1", "{}", Collections.emptyMap(), OutboxStatus.IN_PROGRESS, 0, Instant.now(), null, null, null);
        OutboxMessage m2 = new OutboxMessage(id2, "t", "k2", "{}", Collections.emptyMap(), OutboxStatus.IN_PROGRESS, 0, Instant.now(), null, null, null);
        OutboxMessage m3 = new OutboxMessage(id3, "t", "k3", "{}", Collections.emptyMap(), OutboxStatus.IN_PROGRESS, 0, Instant.now(), null, null, null);

        // Batch 1 (full: size 2) -> Batch 2 (partial: size 1) -> Batch 3 (empty: size 0)
        when(outboxRepository.claimBatchForProcessing(eq(2), any(Duration.class), anyInt(), any(Duration.class)))
                .thenReturn(List.of(m1, m2))
                .thenReturn(List.of(m3))
                .thenReturn(Collections.emptyList());

        RecordMetadata metadata = new RecordMetadata(new TopicPartition("t", 0), 0, 0, 0, 0, 0);
        SendResult<Object, Object> sendResult = new SendResult<>(new ProducerRecord<>("t", "k", "{}"), metadata);
        when(kafkaOperations.send(anyProducerRecord())).thenReturn(CompletableFuture.completedFuture(sendResult));

        int totalProcessed = outboxPoller.pollAndPublishBatch();

        assertThat(totalProcessed).isEqualTo(3);
        verify(outboxRepository).markAsSent(eq(id1), any(Instant.class));
        verify(outboxRepository).markAsSent(eq(id2), any(Instant.class));
        verify(outboxRepository).markAsSent(eq(id3), any(Instant.class));
    }

    @Test
    @DisplayName("Fix 5: calculateBackoff must not overflow to negative numbers when retryCount is large")
    void shouldNotOverflowBackoffWhenRetryCountIsHigh() {
        properties.getOutbox().setInitialBackoff(Duration.ofSeconds(1));
        properties.getOutbox().setMaxBackoff(Duration.ofMinutes(5));
        properties.getOutbox().setBackoffMultiplier(2.0);

        // Test with retryCount = 65 and 100
        Duration backoff65 = outboxPoller.calculateBackoff(65);
        Duration backoff100 = outboxPoller.calculateBackoff(100);

        long minExpected = (long) (Duration.ofMinutes(5).toMillis() * 0.8);
        long maxExpected = (long) (Duration.ofMinutes(5).toMillis() * 1.2);

        assertThat(backoff65.toMillis()).isBetween(minExpected, maxExpected);
        assertThat(backoff100.toMillis()).isBetween(minExpected, maxExpected);
    }

    @Test
    @DisplayName("Fix 3: Kafka Tombstone - OutboxPoller should deliver ProducerRecord with null value")
    void shouldSendTombstoneRecordWithNullValueToKafka() {
        UUID msgId = UUID.randomUUID();
        OutboxMessage tombstoneMsg = new OutboxMessage(
                msgId, "users-topic", "user-deleted-42", null,
                Collections.emptyMap(), OutboxStatus.IN_PROGRESS, 0,
                Instant.now(), null, null, null
        );

        when(outboxRepository.claimBatchForProcessing(anyInt(), any(Duration.class), anyInt(), any(Duration.class)))
                .thenReturn(List.of(tombstoneMsg))
                .thenReturn(Collections.emptyList());

        RecordMetadata metadata = new RecordMetadata(new TopicPartition("users-topic", 0), 0, 0, 0, 0, 0);
        SendResult<Object, Object> sendResult = new SendResult<>(new ProducerRecord<>("users-topic", "user-deleted-42", null), metadata);
        when(kafkaOperations.send(anyProducerRecord())).thenReturn(CompletableFuture.completedFuture(sendResult));

        int processed = outboxPoller.pollAndPublishBatch();

        assertThat(processed).isEqualTo(1);
        ArgumentCaptor<ProducerRecord<Object, Object>> recordCaptor = ArgumentCaptor.captor();
        verify(kafkaOperations).send(recordCaptor.capture());

        ProducerRecord<Object, Object> sentRecord = recordCaptor.getValue();
        assertThat(sentRecord.topic()).isEqualTo("users-topic");
        assertThat(sentRecord.key()).isEqualTo("user-deleted-42");
        assertThat(sentRecord.value()).isNull(); // Authentic Kafka Tombstone!
        verify(outboxRepository).markAsSent(eq(msgId), any(Instant.class));
    }

    @Test
    @DisplayName("Fix 1: Circuit Breaker - Should skip poll when broker cooldown is active")
    void shouldSkipPollWhenBrokerCooldownIsActive() {
        // Set cooldown 10 seconds into the future
        outboxPoller.setBrokerCooldownUntilMs(System.currentTimeMillis() + 10_000);

        int processed = outboxPoller.pollAndPublishBatch();

        assertThat(processed).isEqualTo(0);
        verify(outboxRepository, never()).claimBatchForProcessing(anyInt(), any(Duration.class), anyInt(), any(Duration.class));
    }

    @Test
    @DisplayName("Fix 5: Graceful Shutdown - Shutdown mid-batch should release unattempted claims immediately")
    void shouldReleaseClaimsOnGracefulShutdown() {
        UUID id1 = UUID.randomUUID();
        UUID id2 = UUID.randomUUID();

        OutboxMessage m1 = new OutboxMessage(id1, "t", "k1", "{}", Collections.emptyMap(), OutboxStatus.IN_PROGRESS, 0, Instant.now(), null, null, null);
        OutboxMessage m2 = new OutboxMessage(id2, "t", "k2", "{}", Collections.emptyMap(), OutboxStatus.IN_PROGRESS, 0, Instant.now(), null, null, null);

        when(outboxRepository.claimBatchForProcessing(anyInt(), any(Duration.class), anyInt(), any(Duration.class)))
                .thenReturn(List.of(m1, m2));

        // When first message is sent, initiate shutdown!
        when(kafkaOperations.send(anyProducerRecord())).thenAnswer(inv -> {
            outboxPoller.shutdown();
            RecordMetadata metadata = new RecordMetadata(new TopicPartition("t", 0), 0, 0, 0, 0, 0);
            return CompletableFuture.completedFuture(new SendResult<>(inv.getArgument(0), metadata));
        });

        int processed = outboxPoller.pollAndPublishBatch();

        assertThat(processed).isEqualTo(1); // Only m1 processed before shutdown
        verify(outboxRepository).markAsSent(eq(id1), any(Instant.class));
        verify(outboxRepository).releaseClaims(eq(List.of(id2)), eq(Duration.ZERO));
        assertThat(outboxPoller.isRunning()).isFalse();
    }

    @Test
    @DisplayName("Fix 6: Header Null-Safety - Null header keys must be filtered without throwing IllegalArgumentException")
    void shouldSkipNullHeaderKeysWithoutThrowingException() {
        UUID msgId = UUID.randomUUID();
        java.util.Map<String, String> headers = new java.util.HashMap<>();
        headers.put("valid-key", "valid-value");
        headers.put(null, "null-key-value");
        headers.put("null-val-key", null);

        OutboxMessage message = new OutboxMessage(
                msgId, "headers-topic", "K1", "{}",
                headers, OutboxStatus.IN_PROGRESS, 0,
                Instant.now(), null, null, null
        );

        when(outboxRepository.claimBatchForProcessing(anyInt(), any(Duration.class), anyInt(), any(Duration.class)))
                .thenReturn(List.of(message))
                .thenReturn(Collections.emptyList());

        RecordMetadata metadata = new RecordMetadata(new TopicPartition("headers-topic", 0), 0, 0, 0, 0, 0);
        when(kafkaOperations.send(anyProducerRecord()))
                .thenReturn(CompletableFuture.completedFuture(new SendResult<>(new ProducerRecord<>("headers-topic", "K1", "{}"), metadata)));

        int processed = outboxPoller.pollAndPublishBatch();

        assertThat(processed).isEqualTo(1);
        ArgumentCaptor<ProducerRecord<Object, Object>> recordCaptor = ArgumentCaptor.captor();
        verify(kafkaOperations).send(recordCaptor.capture());
        ProducerRecord<Object, Object> sentRecord = recordCaptor.getValue();
        assertThat(sentRecord.headers().lastHeader("valid-key")).isNotNull();
        assertThat(sentRecord.headers().lastHeader("X-Created-At")).isNotNull();
        assertThat(sentRecord.headers().lastHeader("null-val-key")).isNotNull();
        assertThat(sentRecord.headers().lastHeader("null-val-key").value()).isNull();
        assertThat(sentRecord.headers().toArray()).hasSize(5);
        for (org.apache.kafka.common.header.Header header : sentRecord.headers()) {
            assertThat(header.key()).isNotNull();
        }
    }

    @Test
    @DisplayName("Fix 5: Negative Retention Guard - Negative or zero retention must not delete database rows")
    void shouldIgnoreNegativeOrZeroRetentionDuringCleanup() {
        properties.getOutbox().setRetention(Duration.ofDays(-7));
        properties.getOutbox().setFailedRetention(Duration.ZERO);

        outboxPoller.cleanupOldMessages();

        verify(outboxRepository, never()).deleteSentBefore(any());
        verify(outboxRepository, never()).deleteFailedBefore(any());
    }

    @Test
    @DisplayName("Senior Refinements v22: cleanupOldMessages should immediately exit when Outbox is disabled")
    void shouldSkipCleanupWhenOutboxIsDisabled() {
        properties.getOutbox().setEnabled(false);

        outboxPoller.cleanupOldMessages();

        verify(outboxRepository, never()).executeWithLock(any(), any());
        verify(outboxRepository, never()).deleteSentBefore(any());
        verify(outboxRepository, never()).deleteFailedBefore(any());
    }

    @Test
    @DisplayName("Senior Refinements v24: cleanupOldMessages should immediately exit when Outbox cleaner is disabled")
    void shouldSkipCleanupWhenOutboxCleanerIsDisabled() {
        properties.getOutbox().setEnabled(true);
        properties.getOutbox().getCleaner().setEnabled(false);

        outboxPoller.cleanupOldMessages();

        verify(outboxRepository, never()).executeWithLock(any(), any());
        verify(outboxRepository, never()).deleteSentBefore(any());
        verify(outboxRepository, never()).deleteFailedBefore(any());
    }


    @Test
    @DisplayName("Fix 1: Full Jitter - Multiple backoff calculations should produce randomized values within 80%-120%")
    void shouldApplyJitterToBackoffCalculation() {
        properties.getOutbox().setInitialBackoff(Duration.ofSeconds(1));
        properties.getOutbox().setBackoffMultiplier(2.0);
        properties.getOutbox().setMaxBackoff(Duration.ofMinutes(10));

        // Base for retry 2 = 1000 * 2^2 = 4000ms
        // Range with 80%-120% Jitter = [3200ms, 4800ms]
        boolean foundDifference = false;
        long first = outboxPoller.calculateBackoff(2).toMillis();

        for (int i = 0; i < 20; i++) {
            long current = outboxPoller.calculateBackoff(2).toMillis();
            assertThat(current).isBetween(3200L, 4800L);
            if (current != first) {
                foundDifference = true;
            }
        }
        assertThat(foundDifference).isTrue();
    }

    @Test
    @DisplayName("Pipelined Batch: Multiple messages in batch should be sent concurrently via CompletableFuture")
    void shouldPipelineBatchMessagesConcurrently() {
        UUID id1 = UUID.randomUUID();
        UUID id2 = UUID.randomUUID();
        UUID id3 = UUID.randomUUID();

        OutboxMessage m1 = new OutboxMessage(id1, "pipe-topic", "k1", "{\"v\":1}", Collections.emptyMap(), OutboxStatus.IN_PROGRESS, 0, Instant.now(), null, null, null);
        OutboxMessage m2 = new OutboxMessage(id2, "pipe-topic", "k2", "{\"v\":2}", Collections.emptyMap(), OutboxStatus.IN_PROGRESS, 0, Instant.now(), null, null, null);
        OutboxMessage m3 = new OutboxMessage(id3, "pipe-topic", "k3", "{\"v\":3}", Collections.emptyMap(), OutboxStatus.IN_PROGRESS, 0, Instant.now(), null, null, null);

        when(outboxRepository.claimBatchForProcessing(anyInt(), any(Duration.class), anyInt(), any(Duration.class)))
                .thenReturn(List.of(m1, m2, m3))
                .thenReturn(Collections.emptyList());

        CompletableFuture<SendResult<Object, Object>> f1 = new CompletableFuture<>();
        CompletableFuture<SendResult<Object, Object>> f2 = new CompletableFuture<>();
        CompletableFuture<SendResult<Object, Object>> f3 = new CompletableFuture<>();

        when(kafkaOperations.send(anyProducerRecord()))
                .thenReturn(f1)
                .thenReturn(f2)
                .thenReturn(f3);

        RecordMetadata metadata = new RecordMetadata(new TopicPartition("pipe-topic", 0), 0, 0, 0, 0, 0);
        SendResult<Object, Object> res = new SendResult<>(new ProducerRecord<>("pipe-topic", "k", "{}"), metadata);

        // Complete futures asynchronously
        CompletableFuture.delayedExecutor(50, java.util.concurrent.TimeUnit.MILLISECONDS).execute(() -> {
            f1.complete(res);
            f2.complete(res);
            f3.complete(res);
        });

        int processed = outboxPoller.pollAndPublishBatch();

        assertThat(processed).isEqualTo(3);
        verify(outboxRepository).markAsSent(eq(id1), any(Instant.class));
        verify(outboxRepository).markAsSent(eq(id2), any(Instant.class));
        verify(outboxRepository).markAsSent(eq(id3), any(Instant.class));
    }

    @Test
    @DisplayName("Senior Refinements v8: Poller should drain backlog sequentially when fewer than batchSize messages are claimed")
    void shouldDrainBatchesSequentiallyWhenSingleMessageClaimedDueToPartitionLock() {
        UUID id1 = UUID.randomUUID();
        UUID id2 = UUID.randomUUID();
        OutboxMessage m1 = new OutboxMessage(id1, "drain-topic", "k-same", "{\"v\":1}", Collections.emptyMap(), OutboxStatus.IN_PROGRESS, 0, Instant.now(), null, null, null);
        OutboxMessage m2 = new OutboxMessage(id2, "drain-topic", "k-same", "{\"v\":2}", Collections.emptyMap(), OutboxStatus.IN_PROGRESS, 0, Instant.now(), null, null, null);

        when(outboxRepository.claimBatchForProcessing(anyInt(), any(Duration.class), anyInt(), any(Duration.class)))
                .thenReturn(List.of(m1))
                .thenReturn(List.of(m2))
                .thenReturn(Collections.emptyList());

        RecordMetadata metadata = new RecordMetadata(new TopicPartition("drain-topic", 0), 0, 0, 0, 0, 0);
        SendResult<Object, Object> res = new SendResult<>(new ProducerRecord<>("drain-topic", "k-same", "{}"), metadata);
        when(kafkaOperations.send(anyProducerRecord())).thenReturn(CompletableFuture.completedFuture(res));

        int processed = outboxPoller.pollAndPublishBatch();

        assertThat(processed).isEqualTo(2);
        verify(outboxRepository).markAsSent(eq(id1), any(Instant.class));
        verify(outboxRepository).markAsSent(eq(id2), any(Instant.class));
    }

    @Test
    @DisplayName("Senior Refinements v9: In-flight pipelined messages are not cancelled on outage and complete via whenComplete callback")
    void shouldNotCancelInFlightFuturesOnOutageAndMarkSentOnEventualSuccess() {
        UUID id1 = UUID.randomUUID();
        UUID id2 = UUID.randomUUID();

        OutboxMessage m1 = new OutboxMessage(id1, "topic", "k1", "{\"v\":1}", Collections.emptyMap(), OutboxStatus.IN_PROGRESS, 0, Instant.now(), null, null, null);
        OutboxMessage m2 = new OutboxMessage(id2, "topic", "k2", "{\"v\":2}", Collections.emptyMap(), OutboxStatus.IN_PROGRESS, 0, Instant.now(), null, null, null);

        when(outboxRepository.claimBatchForProcessing(anyInt(), any(Duration.class), anyInt(), any(Duration.class)))
                .thenReturn(List.of(m1, m2))
                .thenReturn(Collections.emptyList());

        CompletableFuture<SendResult<Object, Object>> f1 = new CompletableFuture<>();
        CompletableFuture<SendResult<Object, Object>> f2 = new CompletableFuture<>();

        when(kafkaOperations.send(anyProducerRecord()))
                .thenReturn(f1)
                .thenAnswer(inv -> {
                    f1.completeExceptionally(new org.apache.kafka.common.errors.TimeoutException("Kafka broker offline"));
                    return f2;
                });

        int processed = outboxPoller.pollAndPublishBatch();

        // Batch stopped after m1 failure
        assertThat(processed).isEqualTo(1);
        // m2 future must NOT be canceled!
        assertThat(f2.isCancelled()).isFalse();

        // With Round 63 fix, in-flight dispatched messages are NOT released to prevent duplicate sends by other replicas
        verify(outboxRepository, never()).releaseClaims(eq(List.of(id2)), any(Duration.class));

        // Now f2 eventually succeeds in Kafka producer background thread
        RecordMetadata metadata = new RecordMetadata(new TopicPartition("topic", 0), 0, 0, 0, 0, 0);
        SendResult<Object, Object> res = new SendResult<>(new ProducerRecord<>("topic", "k2", "{}"), metadata);
        f2.complete(res);

        // whenComplete attached on dispatch must mark m2 as sent!
        verify(outboxRepository).markAsSent(eq(id2), any(Instant.class));
    }

    @Test
    @DisplayName("Senior Refinements v13: onOutboxCommitted single-bit barrier prevents flooding async task executor")
    void shouldNotFloodTaskExecutorWhenMultipleCommitEventsArriveConcurrently() {
        org.springframework.core.task.TaskExecutor asyncExecutor = org.mockito.Mockito.mock(org.springframework.core.task.TaskExecutor.class);
        OutboxPoller poller = new OutboxPoller(outboxRepository, kafkaOperations, properties, IdemboxMetrics.noOp(), asyncExecutor);
        poller.start();

        // Simulate async task that stays in execution for a short while
        java.util.concurrent.CountDownLatch latch = new java.util.concurrent.CountDownLatch(1);
        org.mockito.Mockito.doAnswer(invocation -> {
            Runnable task = invocation.getArgument(0);
            new Thread(() -> {
                try {
                    latch.await();
                } catch (InterruptedException ignored) {}
                task.run();
            }).start();
            return null;
        }).when(asyncExecutor).execute(any(Runnable.class));

        // Fire 20 commit events concurrently
        for (int i = 0; i < 20; i++) {
            poller.onOutboxCommitted(new io.github.alexlitvinov.idembox.starter.publisher.DefaultOutboxPublisher.OutboxCommittedEvent(UUID.randomUUID()));
        }

        // Only 1 execution was scheduled because the barrier isDispatchScheduled blocked all subsequent calls
        verify(asyncExecutor, times(1)).execute(any(Runnable.class));

        latch.countDown();
    }

    @Test
    @DisplayName("Senior Refinements v13: schedulePoll has randomized initialDelayString to prevent buffer pin contention")
    void shouldHaveInitialDelayOnSchedulePoll() throws Exception {
        java.lang.reflect.Method method = OutboxPoller.class.getMethod("schedulePoll");
        org.springframework.scheduling.annotation.Scheduled scheduled = method.getAnnotation(org.springframework.scheduling.annotation.Scheduled.class);

        assertThat(scheduled).isNotNull();
        assertThat(scheduled.initialDelayString()).contains("ThreadLocalRandom");
        assertThat(scheduled.fixedDelayString()).contains("500");
        assertThat(scheduled.scheduler()).isEqualTo("idemboxTaskScheduler");

        java.lang.reflect.Method cleanupMethod = io.github.alexlitvinov.idembox.starter.cleaner.OutboxCleaner.class.getMethod("cleanupOldMessages");
        org.springframework.scheduling.annotation.Scheduled cleanupScheduled = cleanupMethod.getAnnotation(org.springframework.scheduling.annotation.Scheduled.class);
        assertThat(cleanupScheduled).isNotNull();
        assertThat(cleanupScheduled.scheduler()).isEqualTo("idemboxTaskScheduler");
    }

    @Test
    @DisplayName("Senior Refinements Round 61: createProducerRecord normalizes empty or blank partition key to null")
    void createProducerRecordShouldNormalizeBlankPartitionKeyToNull() {
        UUID msgId = UUID.randomUUID();
        Instant createdAt = Instant.parse("2026-09-27T10:15:30.000Z");

        // 1. Empty string key -> null
        OutboxMessage msgEmpty = new OutboxMessage(
                msgId, "orders-topic", "", "{\"amount\":250}",
                Collections.emptyMap(), OutboxStatus.PENDING, 0,
                createdAt, null, null, null
        );
        ProducerRecord<?, ?> recordEmpty = outboxPoller.createProducerRecord(msgEmpty);
        assertThat(recordEmpty.key()).isNull();

        // 2. Blank whitespace key -> null
        OutboxMessage msgBlank = new OutboxMessage(
                msgId, "orders-topic", "   \t", "{\"amount\":250}",
                Collections.emptyMap(), OutboxStatus.PENDING, 0,
                createdAt, null, null, null
        );
        ProducerRecord<?, ?> recordBlank = outboxPoller.createProducerRecord(msgBlank);
        assertThat(recordBlank.key()).isNull();

        // 3. Null key -> null
        OutboxMessage msgNull = new OutboxMessage(
                msgId, "orders-topic", null, "{\"amount\":250}",
                Collections.emptyMap(), OutboxStatus.PENDING, 0,
                createdAt, null, null, null
        );
        ProducerRecord<?, ?> recordNull = outboxPoller.createProducerRecord(msgNull);
        assertThat(recordNull.key()).isNull();

        // 4. Non-blank key -> preserved
        OutboxMessage msgValid = new OutboxMessage(
                msgId, "orders-topic", "KEY-42", "{\"amount\":250}",
                Collections.emptyMap(), OutboxStatus.PENDING, 0,
                createdAt, null, null, null
        );
        ProducerRecord<?, ?> recordValid = outboxPoller.createProducerRecord(msgValid);
        assertThat(recordValid.key()).isEqualTo("KEY-42");
    }

    @Test
    @DisplayName("Senior Refinements v21: createProducerRecord defaults to null timestamp to avoid broker rejection and populates X-Created-At header")
    void createProducerRecordShouldUseNullTimestampByDefaultAndPopulateHeader() {
        UUID msgId = UUID.randomUUID();
        Instant createdAt = Instant.parse("2026-09-27T10:15:30.000Z");
        OutboxMessage msg = new OutboxMessage(
                msgId, "orders-topic", "ORD-99", "{\"amount\":250}",
                Collections.emptyMap(), OutboxStatus.PENDING, 0,
                createdAt, null, null, null
        );

        ProducerRecord<?, ?> record = outboxPoller.createProducerRecord(msg);

        assertThat(record.topic()).isEqualTo("orders-topic");
        assertThat(record.key()).isEqualTo("ORD-99");
        assertThat(record.timestamp()).isNull();

        org.apache.kafka.common.header.Header header = record.headers().lastHeader("X-Created-At");
        assertThat(header).isNotNull();
        assertThat(new String(header.value(), StandardCharsets.UTF_8)).isEqualTo(String.valueOf(createdAt.toEpochMilli()));
    }

    @Test
    @DisplayName("Senior Refinements v21: createProducerRecord respects preserveCreationTimestamp configuration")
    void createProducerRecordShouldPreserveTimestampWhenConfigured() {
        properties.getOutbox().setPreserveCreationTimestamp(true);
        UUID msgId = UUID.randomUUID();
        Instant createdAt = Instant.parse("2026-09-27T10:15:30.000Z");
        OutboxMessage msg = new OutboxMessage(
                msgId, "orders-topic", "ORD-99", "{\"amount\":250}",
                Collections.emptyMap(), OutboxStatus.PENDING, 0,
                createdAt, null, null, null
        );

        ProducerRecord<?, ?> record = outboxPoller.createProducerRecord(msg);

        assertThat(record.timestamp()).isEqualTo(createdAt.toEpochMilli());
    }

    @Test
    @DisplayName("Senior Refinements v14: extractErrorReason formats exception safely and prevents null message wiping")
    void extractErrorReasonShouldFormatProperlyAndHandleNullMessage() {
        // 1. Exception with message
        String reason1 = outboxPoller.extractErrorReason(new IllegalStateException("Connection refused"), "default");
        assertThat(reason1).isEqualTo("IllegalStateException: Connection refused");

        // 2. Exception without message (e.g. NullPointerException)
        String reason2 = outboxPoller.extractErrorReason(new NullPointerException(), "default");
        assertThat(reason2).isEqualTo("NullPointerException");

        // 3. Null exception fallback
        String reason3 = outboxPoller.extractErrorReason(null, "Default error");
        assertThat(reason3).isEqualTo("Default error");

        // 4. Overlong message truncation
        String longMsg = "A".repeat(3000);
        String reason4 = outboxPoller.extractErrorReason(new RuntimeException(longMsg), "default");
        assertThat(reason4.length()).isLessThanOrEqualTo(2000);
        assertThat(reason4).endsWith("...");
    }

    @Test
    @DisplayName("Senior Refinements v14: cleanupOldMessages coordinates via advisory executeWithLock")
    void cleanupOldMessagesShouldCoordinateViaAdvisoryLock() {
        when(outboxRepository.executeWithLock(eq("idembox_outbox_cleanup"), any())).thenAnswer(inv -> {
            ((Runnable) inv.getArgument(1)).run();
            return true;
        });

        outboxPoller.cleanupOldMessages();

        verify(outboxRepository).executeWithLock(eq("idembox_outbox_cleanup"), any());
        verify(outboxRepository).deleteSentBefore(any(Instant.class), any(Duration.class), org.mockito.ArgumentMatchers.anyInt(), org.mockito.ArgumentMatchers.anyInt());
        verify(outboxRepository).deleteFailedBefore(any(Instant.class), any(Duration.class), org.mockito.ArgumentMatchers.anyInt(), org.mockito.ArgumentMatchers.anyInt());
    }

    @Test
    @DisplayName("Senior Refinements v15: createProducerRecord filters empty/blank header keys and trims whitespace")
    void createProducerRecordShouldFilterBlankHeaderKeys() {
        UUID msgId = UUID.randomUUID();
        Map<String, String> headers = Map.of(
                "  trace" + "parent  ", "00-123456-7890-01",
                "   ", "empty-key",
                "", "blank-key"
        );
        OutboxMessage msg = new OutboxMessage(
                msgId, "orders-topic", "ORD-1", "{}",
                headers, OutboxStatus.PENDING, 0,
                Instant.now(), null, null, null
        );

        ProducerRecord<?, ?> record = outboxPoller.createProducerRecord(msg);

        List<org.apache.kafka.common.header.Header> recordHeaders = new java.util.ArrayList<>();
        record.headers().forEach(recordHeaders::add);

        assertThat(recordHeaders).extracting(org.apache.kafka.common.header.Header::key)
                .containsExactlyInAnyOrder("trace" + "parent", "X-Created-At", "X-Outbox-Id", "X-Idempotency-Key");
        assertThat(record.headers().lastHeader("")).isNull();
        assertThat(record.headers().lastHeader("   ")).isNull();
    }

    @Test
    @DisplayName("Senior Refinements v15: Async callback records publish latency without HoL blocking")
    void shouldRecordPublishLatencyInAsyncCallback() {
        IdemboxMetrics mockMetrics = org.mockito.Mockito.mock(IdemboxMetrics.class);
        OutboxPoller poller = new OutboxPoller(outboxRepository, kafkaOperations, properties, mockMetrics);
        poller.start();

        UUID msgId = UUID.randomUUID();
        OutboxMessage msg = new OutboxMessage(
                msgId, "latency-topic", "k1", "{}",
                Collections.emptyMap(), OutboxStatus.IN_PROGRESS, 0,
                Instant.now(), null, null, null
        );

        when(outboxRepository.claimBatchForProcessing(anyInt(), any(Duration.class), anyInt(), any(Duration.class)))
                .thenReturn(List.of(msg))
                .thenReturn(Collections.emptyList());

        RecordMetadata metadata = new RecordMetadata(new TopicPartition("latency-topic", 0), 0, 0, 0, 0, 0);
        SendResult<Object, Object> sendResult = new SendResult<>(new ProducerRecord<>("latency-topic", "k1", "{}"), metadata);
        when(kafkaOperations.send(anyProducerRecord())).thenReturn(CompletableFuture.completedFuture(sendResult));

        int processed = poller.pollAndPublishBatch();

        assertThat(processed).isEqualTo(1);
        verify(mockMetrics, times(1)).recordOutboxSent(eq("latency-topic"), any(Duration.class));
    }

    @Test
    @DisplayName("Senior Refinements v19: Fatal non-retryable errors (RecordTooLargeException, SerializationException, InvalidTopicException) fail immediately")
    void shouldImmediatelyFailOnFatalProducerErrorsWithoutRetries() {
        IdemboxMetrics mockMetrics = org.mockito.Mockito.mock(IdemboxMetrics.class);
        OutboxPoller poller = new OutboxPoller(outboxRepository, kafkaOperations, properties, mockMetrics);
        poller.start();

        UUID msgId = UUID.randomUUID();
        OutboxMessage msg = new OutboxMessage(
                msgId, "fatal-topic", "k1", "{\"huge\":true}",
                Collections.emptyMap(), OutboxStatus.IN_PROGRESS, 0,
                Instant.now(), null, null, null
        );

        when(outboxRepository.claimBatchForProcessing(anyInt(), any(Duration.class), anyInt(), any(Duration.class)))
                .thenReturn(List.of(msg))
                .thenReturn(Collections.emptyList());

        CompletableFuture<SendResult<Object, Object>> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new org.apache.kafka.common.errors.RecordTooLargeException("Message size 2MB exceeds max.request.size 1MB"));
        when(kafkaOperations.send(anyProducerRecord())).thenReturn(failedFuture);

        int processed = poller.pollAndPublishBatch();

        assertThat(processed).isEqualTo(1);
        verify(mockMetrics).recordOutboxTerminalFailure("fatal-topic");
        verify(mockMetrics, never()).recordOutboxFailed(anyString());
        verify(outboxRepository).markAsFailed(eq(msgId), anyString(), eq(1), eq(Duration.ZERO));
        verify(outboxRepository, never()).rescheduleMessage(any(), any(), any());
    }

    @Test
    @DisplayName("Senior Refinements v19: isFatalProducerError identifies fatal Kafka producer exceptions directly and when wrapped")
    void isFatalProducerErrorShouldDetectDirectAndNestedExceptions() {
        assertThat(outboxPoller.isFatalProducerError(new org.apache.kafka.common.errors.RecordTooLargeException("too big"))).isTrue();
        assertThat(outboxPoller.isFatalProducerError(new org.apache.kafka.common.errors.SerializationException("bad json"))).isTrue();
        assertThat(outboxPoller.isFatalProducerError(new org.apache.kafka.common.errors.InvalidTopicException("bad topic"))).isTrue();

        // Wrapped in ExecutionException
        Exception wrapped = new java.util.concurrent.ExecutionException(new org.apache.kafka.common.errors.RecordTooLargeException("wrapped"));
        assertThat(outboxPoller.isFatalProducerError(wrapped)).isTrue();

        // Non-fatal exceptions
        assertThat(outboxPoller.isFatalProducerError(new java.util.concurrent.TimeoutException())).isFalse();
        assertThat(outboxPoller.isFatalProducerError(new RuntimeException("transient error"))).isFalse();
        assertThat(outboxPoller.isFatalProducerError(null)).isFalse();
    }

    @Test
    @DisplayName("Senior Refinements v19: calculateBackoff enforces minimum 100ms floor and applies exponential backoff with jitter")
    void calculateBackoffShouldEnforce100MsFloorAndRespectMultiplier() {
        properties.getOutbox().setInitialBackoff(Duration.ofMillis(100));
        properties.getOutbox().setMaxBackoff(Duration.ofSeconds(60));
        properties.getOutbox().setBackoffMultiplier(2.0);

        // Attempt 0: base is 100ms, with 0.8..1.2 jitter => 80..120ms, clamped to minimum 100ms
        for (int i = 0; i < 50; i++) {
            Duration backoff = outboxPoller.calculateBackoff(0);
            assertThat(backoff.toMillis()).isGreaterThanOrEqualTo(100L);
            assertThat(backoff.toMillis()).isLessThanOrEqualTo(130L);
        }

        // Attempt 3: base is 100 * 2^3 = 800ms, with 0.8..1.2 jitter => 640..960ms
        for (int i = 0; i < 50; i++) {
            Duration backoff = outboxPoller.calculateBackoff(3);
            assertThat(backoff.toMillis()).isGreaterThanOrEqualTo(600L);
            assertThat(backoff.toMillis()).isLessThanOrEqualTo(1000L);
        }

        // High attempt capped at maxBackoff (60s * 1.2 = 72s max with jitter)
        Duration capped = outboxPoller.calculateBackoff(25);
        assertThat(capped.toMillis()).isLessThanOrEqualTo(75_000L);
    }

    @Test
    @DisplayName("Senior Refinements v19: isBrokerOutage detects timeout, disconnect, retriable, and network connect errors")
    void isBrokerOutageShouldDetectDirectAndNestedExceptions() {
        assertThat(outboxPoller.isBrokerOutage(new java.util.concurrent.TimeoutException())).isTrue();
        assertThat(outboxPoller.isBrokerOutage(new org.apache.kafka.common.errors.TimeoutException())).isTrue();
        assertThat(outboxPoller.isBrokerOutage(new org.apache.kafka.common.errors.DisconnectException())).isTrue();
        assertThat(outboxPoller.isBrokerOutage(new java.net.ConnectException("connection refused"))).isTrue();

        // Nested in runtime exception
        Exception nested = new RuntimeException("Outer", new java.util.concurrent.ExecutionException(new java.net.ConnectException("broker unreachable")));
        assertThat(outboxPoller.isBrokerOutage(nested)).isTrue();

        // Ordinary business or application exceptions are not broker outages
        assertThat(outboxPoller.isBrokerOutage(new IllegalArgumentException("invalid argument"))).isFalse();
        assertThat(outboxPoller.isBrokerOutage(null)).isFalse();
    }

    @Test
    @DisplayName("Senior Refinements v19: calculateJitteredCooldownMs produces values within +/-20% range")
    void calculateJitteredCooldownMsShouldBeWithinJitterRange() {
        properties.getOutbox().setBrokerCooldown(Duration.ofSeconds(10));
        for (int i = 0; i < 50; i++) {
            long cooldown = outboxPoller.calculateJitteredCooldownMs();
            assertThat(cooldown).isGreaterThanOrEqualTo(8000L);
            assertThat(cooldown).isLessThanOrEqualTo(12000L);
        }
    }

    @Test
    @DisplayName("Senior Refinements v18: whenComplete offloads markAsSent to outboxAsyncExecutor")
    void shouldOffloadMarkAsSentToOutboxAsyncExecutor() {
        org.springframework.core.task.TaskExecutor asyncExecutor = org.mockito.Mockito.mock(org.springframework.core.task.TaskExecutor.class);
        OutboxPoller customPoller = new OutboxPoller(outboxRepository, kafkaOperations, properties, IdemboxMetrics.noOp(), asyncExecutor);
        customPoller.start();

        UUID msgId = UUID.randomUUID();
        OutboxMessage message = new OutboxMessage(
                msgId, "orders-topic", "ORD-ASYNC", "{\"amount\":100}",
                Collections.emptyMap(), OutboxStatus.IN_PROGRESS, 0,
                Instant.now(), null, null, null
        );

        when(outboxRepository.claimBatchForProcessing(anyInt(), any(Duration.class), anyInt(), any(Duration.class)))
                .thenReturn(List.of(message))
                .thenReturn(Collections.emptyList());

        CompletableFuture<SendResult<Object, Object>> future = new CompletableFuture<>();
        when(kafkaOperations.send(anyProducerRecord())).thenReturn(future);

        java.util.concurrent.atomic.AtomicInteger processedCount = new java.util.concurrent.atomic.AtomicInteger();
        Thread thread = new Thread(() -> processedCount.set(customPoller.pollAndPublishBatch()));
        thread.start();

        org.awaitility.Awaitility.await().until(() -> {
            try {
                verify(kafkaOperations).send(anyProducerRecord());
                return true;
            } catch (Throwable t) {
                return false;
            }
        });

        RecordMetadata metadata = new RecordMetadata(new TopicPartition("orders-topic", 0), 0, 0, 0, 0, 0);
        SendResult<Object, Object> sendResult = new SendResult<>(new ProducerRecord<>("orders-topic", "ORD-ASYNC", "{\"amount\":100}"), metadata);
        future.complete(sendResult);

        ArgumentCaptor<Runnable> taskCaptor = ArgumentCaptor.forClass(Runnable.class);
        verify(asyncExecutor, org.mockito.Mockito.timeout(2000)).execute(taskCaptor.capture());

        taskCaptor.getValue().run();
        verify(outboxRepository).markAsSent(eq(msgId), any(Instant.class));

        try {
            thread.join(2000);
        } catch (InterruptedException ignored) {}
        assertThat(processedCount.get()).isEqualTo(1);
    }

    @Test
    @DisplayName("Senior Refinements v18: shutdown deadline dynamically adapts to sendTimeout")
    void shouldCalculateDynamicShutdownDeadlineBasedOnSendTimeout() {
        properties.getOutbox().setSendTimeout(Duration.ofSeconds(12));
        long start = System.currentTimeMillis();
        outboxPoller.shutdown();
        long elapsed = System.currentTimeMillis() - start;
        assertThat(elapsed).isLessThan(1000L);
    }

    @Test
    @DisplayName("Senior Refinements v19: extractErrorReason sanitizes exception message containing null character")
    void extractErrorReasonShouldSanitizeNullCharacterInExceptionMessage() {
        String reason = outboxPoller.extractErrorReason(new RuntimeException("invalid \u0000 character in kafka payload"), "default");
        assertThat(reason).doesNotContain("\u0000");
        assertThat(reason).doesNotContain("\0");
        assertThat(reason).isEqualTo("RuntimeException: invalid  character in kafka payload");
    }

    @Test
    @DisplayName("Senior Refinements v19: extractErrorReason returns simple class name for blank exception messages")
    void extractErrorReasonShouldReturnClassNameWhenMessageIsBlank() {
        String reason1 = outboxPoller.extractErrorReason(new IllegalArgumentException("   "), "default");
        assertThat(reason1).isEqualTo("IllegalArgumentException");

        String reason2 = outboxPoller.extractErrorReason(new IllegalArgumentException(""), "default");
        assertThat(reason2).isEqualTo("IllegalArgumentException");
    }

    @Test
    @DisplayName("Senior Refinements v19: onOutboxCommitted sets pendingRepoll without scheduling task when poller is busy")
    void onOutboxCommittedShouldSetPendingRepollWhenPollerIsAlreadyRunning() {
        org.springframework.core.task.TaskExecutor asyncExecutor = org.mockito.Mockito.mock(org.springframework.core.task.TaskExecutor.class);
        OutboxPoller poller = new OutboxPoller(outboxRepository, kafkaOperations, properties, IdemboxMetrics.noOp(), asyncExecutor);

        java.util.concurrent.atomic.AtomicBoolean isPolling =
                (java.util.concurrent.atomic.AtomicBoolean) org.springframework.test.util.ReflectionTestUtils.getField(poller, "isPolling");
        java.util.concurrent.atomic.AtomicBoolean pendingRepoll =
                (java.util.concurrent.atomic.AtomicBoolean) org.springframework.test.util.ReflectionTestUtils.getField(poller, "pendingRepoll");

        assertThat(isPolling).isNotNull();
        assertThat(pendingRepoll).isNotNull();
        isPolling.set(true);
        assertThat(pendingRepoll.get()).isFalse();

        poller.onOutboxCommitted(new io.github.alexlitvinov.idembox.starter.publisher.DefaultOutboxPublisher.OutboxCommittedEvent(UUID.randomUUID()));

        assertThat(pendingRepoll.get()).isTrue();
        verify(asyncExecutor, never()).execute(any(Runnable.class));
    }

    @Test
    @DisplayName("Senior Refinements v19: onOutboxCommitted no-ops when immediateFlush is disabled")
    void onOutboxCommittedShouldNoOpWhenImmediateFlushIsDisabled() {
        org.springframework.core.task.TaskExecutor asyncExecutor = org.mockito.Mockito.mock(org.springframework.core.task.TaskExecutor.class);
        properties.getOutbox().setImmediateFlush(false);
        OutboxPoller poller = new OutboxPoller(outboxRepository, kafkaOperations, properties, IdemboxMetrics.noOp(), asyncExecutor);

        java.util.concurrent.atomic.AtomicBoolean pendingRepoll =
                (java.util.concurrent.atomic.AtomicBoolean) org.springframework.test.util.ReflectionTestUtils.getField(poller, "pendingRepoll");

        assertThat(pendingRepoll).isNotNull();
        poller.onOutboxCommitted(new io.github.alexlitvinov.idembox.starter.publisher.DefaultOutboxPublisher.OutboxCommittedEvent(UUID.randomUUID()));

        assertThat(pendingRepoll.get()).isFalse();
        verify(asyncExecutor, never()).execute(any(Runnable.class));
    }

    @Test
    @DisplayName("Senior Refinements v19: onOutboxCommitted no-ops when outbox or poller is disabled")
    void onOutboxCommittedShouldNoOpWhenOutboxOrPollerIsDisabled() {
        org.springframework.core.task.TaskExecutor asyncExecutor = org.mockito.Mockito.mock(org.springframework.core.task.TaskExecutor.class);
        properties.getOutbox().setEnabled(false);
        OutboxPoller poller = new OutboxPoller(outboxRepository, kafkaOperations, properties, IdemboxMetrics.noOp(), asyncExecutor);

        java.util.concurrent.atomic.AtomicBoolean pendingRepoll =
                (java.util.concurrent.atomic.AtomicBoolean) org.springframework.test.util.ReflectionTestUtils.getField(poller, "pendingRepoll");

        assertThat(pendingRepoll).isNotNull();
        poller.onOutboxCommitted(new io.github.alexlitvinov.idembox.starter.publisher.DefaultOutboxPublisher.OutboxCommittedEvent(UUID.randomUUID()));

        assertThat(pendingRepoll.get()).isFalse();
        verify(asyncExecutor, never()).execute(any(Runnable.class));

        // Re-enable outbox, disable poller
        properties.getOutbox().setEnabled(true);
        properties.getOutbox().getPoller().setEnabled(false);

        poller.onOutboxCommitted(new io.github.alexlitvinov.idembox.starter.publisher.DefaultOutboxPublisher.OutboxCommittedEvent(UUID.randomUUID()));
        assertThat(pendingRepoll.get()).isFalse();
        verify(asyncExecutor, never()).execute(any(Runnable.class));
    }

    @Test
    @DisplayName("Senior Refinements v19: Immediate Flush Race - pendingRepoll triggers repoll in finally block")
    void shouldTriggerRepollInFinallyWhenPendingRepollWasSet() {
        org.springframework.core.task.TaskExecutor asyncExecutor = org.mockito.Mockito.mock(org.springframework.core.task.TaskExecutor.class);
        OutboxPoller poller = new OutboxPoller(outboxRepository, kafkaOperations, properties, IdemboxMetrics.noOp(), asyncExecutor);
        poller.start();

        when(outboxRepository.claimBatchForProcessing(anyInt(), any(), anyInt(), any()))
                .thenAnswer(inv -> {
                    java.util.concurrent.atomic.AtomicBoolean pendingRepoll =
                            (java.util.concurrent.atomic.AtomicBoolean) org.springframework.test.util.ReflectionTestUtils.getField(poller, "pendingRepoll");
                    assertThat(pendingRepoll).isNotNull();
                    pendingRepoll.set(true);
                    return java.util.Collections.emptyList();
                });

        poller.pollAndPublishBatch();

        verify(asyncExecutor, times(1)).execute(any(Runnable.class));
    }

    @Test
    @DisplayName("Senior Refinements v20: onOutboxCommitted resets isDispatchScheduled when asyncExecutor throws RejectedExecutionException")
    void onOutboxCommittedShouldResetDispatchScheduledOnExecutorRejection() {
        org.springframework.core.task.TaskExecutor asyncExecutor = org.mockito.Mockito.mock(org.springframework.core.task.TaskExecutor.class);
        org.mockito.Mockito.doThrow(new java.util.concurrent.RejectedExecutionException("Queue full"))
                .when(asyncExecutor).execute(any(Runnable.class));

        OutboxPoller poller = new OutboxPoller(outboxRepository, kafkaOperations, properties, IdemboxMetrics.noOp(), asyncExecutor);

        java.util.concurrent.atomic.AtomicBoolean isDispatchScheduled =
                (java.util.concurrent.atomic.AtomicBoolean) org.springframework.test.util.ReflectionTestUtils.getField(poller, "isDispatchScheduled");
        java.util.concurrent.atomic.AtomicBoolean pendingRepoll =
                (java.util.concurrent.atomic.AtomicBoolean) org.springframework.test.util.ReflectionTestUtils.getField(poller, "pendingRepoll");

        assertThat(isDispatchScheduled).isNotNull();
        assertThat(pendingRepoll).isNotNull();
        poller.onOutboxCommitted(new io.github.alexlitvinov.idembox.starter.publisher.DefaultOutboxPublisher.OutboxCommittedEvent(UUID.randomUUID()));

        assertThat(isDispatchScheduled.get()).isFalse();
        assertThat(pendingRepoll.get()).isTrue();
    }

    @Test
    @DisplayName("Senior Refinements v21: whenComplete falls back to direct execution if outboxAsyncExecutor throws RejectedExecutionException")
    void shouldFallbackToDirectExecutionWhenExecutorRejectsMarkAsSent() {
        org.springframework.core.task.TaskExecutor asyncExecutor = org.mockito.Mockito.mock(org.springframework.core.task.TaskExecutor.class);
        org.mockito.Mockito.doThrow(new java.util.concurrent.RejectedExecutionException("Shutdown in progress"))
                .when(asyncExecutor).execute(any(Runnable.class));

        OutboxPoller customPoller = new OutboxPoller(outboxRepository, kafkaOperations, properties, IdemboxMetrics.noOp(), asyncExecutor);
        customPoller.start();

        UUID msgId = UUID.randomUUID();
        OutboxMessage message = new OutboxMessage(
                msgId, "orders-topic", "ORD-REJECT", "{\"amount\":100}",
                Collections.emptyMap(), OutboxStatus.IN_PROGRESS, 0,
                Instant.now(), null, null, null
        );

        when(outboxRepository.claimBatchForProcessing(anyInt(), any(Duration.class), anyInt(), any(Duration.class)))
                .thenReturn(List.of(message))
                .thenReturn(Collections.emptyList());

        RecordMetadata metadata = new RecordMetadata(new TopicPartition("orders-topic", 0), 0, 0, 0, 0, 0);
        SendResult<Object, Object> sendResult = new SendResult<>(new ProducerRecord<>("orders-topic", "ORD-REJECT", "{\"amount\":100}"), metadata);
        CompletableFuture<SendResult<Object, Object>> future = CompletableFuture.completedFuture(sendResult);
        when(kafkaOperations.send(anyProducerRecord())).thenReturn(future);

        int processed = customPoller.pollAndPublishBatch();

        assertThat(processed).isEqualTo(1);
        verify(outboxRepository).markAsSent(eq(msgId), any(Instant.class));
    }

    @Test
    @DisplayName("Senior Refinements v21: onOutboxCommitted does not synchronously execute pollAndPublishBatch when executor is null")
    void onOutboxCommittedShouldNotExecuteSynchronouslyWhenExecutorIsNull() {
        OutboxPoller poller = new OutboxPoller(outboxRepository, kafkaOperations, properties, IdemboxMetrics.noOp(), null);

        java.util.concurrent.atomic.AtomicBoolean isDispatchScheduled =
                (java.util.concurrent.atomic.AtomicBoolean) org.springframework.test.util.ReflectionTestUtils.getField(poller, "isDispatchScheduled");
        java.util.concurrent.atomic.AtomicBoolean pendingRepoll =
                (java.util.concurrent.atomic.AtomicBoolean) org.springframework.test.util.ReflectionTestUtils.getField(poller, "pendingRepoll");

        assertThat(isDispatchScheduled).isNotNull();
        assertThat(pendingRepoll).isNotNull();
        poller.onOutboxCommitted(new io.github.alexlitvinov.idembox.starter.publisher.DefaultOutboxPublisher.OutboxCommittedEvent(UUID.randomUUID()));

        assertThat(isDispatchScheduled.get()).isFalse();
        assertThat(pendingRepoll.get()).isTrue();
        verify(outboxRepository, never()).claimBatchForProcessing(anyInt(), any(), anyInt(), any());
    }

    @Test
    @DisplayName("Senior Refinements v21: Pipelined batch awaits sends with per-message timeout from pm.startNs()")
    void shouldAwaitPipelinedSendsWithPerMessageTimeout() {
        OutboxPoller poller = new OutboxPoller(outboxRepository, kafkaOperations, properties, IdemboxMetrics.noOp());
        poller.start();
        properties.getOutbox().setSendTimeout(Duration.ofSeconds(5));

        UUID msgId1 = UUID.randomUUID();
        UUID msgId2 = UUID.randomUUID();
        OutboxMessage m1 = new OutboxMessage(msgId1, "t1", "k1", "{}", Collections.emptyMap(), OutboxStatus.IN_PROGRESS, 0, Instant.now(), null, null, null);
        OutboxMessage m2 = new OutboxMessage(msgId2, "t1", "k2", "{}", Collections.emptyMap(), OutboxStatus.IN_PROGRESS, 0, Instant.now(), null, null, null);

        when(outboxRepository.claimBatchForProcessing(anyInt(), any(), anyInt(), any()))
                .thenReturn(List.of(m1, m2))
                .thenReturn(Collections.emptyList());

        RecordMetadata meta = new RecordMetadata(new TopicPartition("t1", 0), 0, 0, 0, 0, 0);
        CompletableFuture<SendResult<Object, Object>> f1 = new CompletableFuture<>();
        CompletableFuture<SendResult<Object, Object>> f2 = new CompletableFuture<>();

        when(kafkaOperations.send(anyProducerRecord()))
                .thenReturn(f1)
                .thenReturn(f2);

        // Complete f1 immediately and f2 slightly after
        f1.complete(new SendResult<>(new ProducerRecord<>("t1", "k1", "{}"), meta));
        f2.complete(new SendResult<>(new ProducerRecord<>("t1", "k2", "{}"), meta));

        int processed = poller.pollAndPublishBatch();

        assertThat(processed).isEqualTo(2);
        verify(outboxRepository).markAsSent(eq(msgId1), any(Instant.class));
        verify(outboxRepository).markAsSent(eq(msgId2), any(Instant.class));
    }

    @Test
    @DisplayName("Senior Refinements v22: createProducerRecord sets timestamp from createdAt and populates headers")
    void shouldCreateProducerRecordWithCreatedAtTimestampAndHeaders() {
        Instant now = Instant.parse("2026-09-28T12:00:00Z");
        java.util.Map<String, String> headers = new java.util.LinkedHashMap<>();
        headers.put("X-Custom-Header", "custom-value");
        headers.put("X-Trace-Id", "trace-abc-123");
        headers.put("   ", "ignored");
        headers.put(null, "ignored-null");

        OutboxMessage msg = new OutboxMessage(
                UUID.randomUUID(), "orders-topic", "ORD-KEY-1", "{\"orderId\":\"ORD-1\"}",
                headers, OutboxStatus.IN_PROGRESS, 0, now, null, null, null
        );

        // By default, preserveCreationTimestamp is false, so record.timestamp() is null (Kafka assigns current broker time)
        ProducerRecord<?, ?> defaultRecord = outboxPoller.createProducerRecord(msg);
        assertThat(defaultRecord.topic()).isEqualTo("orders-topic");
        assertThat(defaultRecord.key()).isEqualTo("ORD-KEY-1");
        assertThat(defaultRecord.value()).isEqualTo("{\"orderId\":\"ORD-1\"}");
        assertThat(defaultRecord.timestamp()).isNull();
        assertThat(new String(defaultRecord.headers().lastHeader("X-Custom-Header").value(), StandardCharsets.UTF_8)).isEqualTo("custom-value");
        assertThat(new String(defaultRecord.headers().lastHeader("X-Trace-Id").value(), StandardCharsets.UTF_8)).isEqualTo("trace-abc-123");
        assertThat(defaultRecord.headers().lastHeader("   ")).isNull();

        // When preserveCreationTimestamp is enabled, timestamp matches createdAt
        properties.getOutbox().setPreserveCreationTimestamp(true);
        ProducerRecord<?, ?> preservedRecord = outboxPoller.createProducerRecord(msg);
        assertThat(preservedRecord.timestamp()).isEqualTo(now.toEpochMilli());
    }

    @Test
    @DisplayName("Senior Refinements v22: isFatalProducerError correctly classifies non-retryable fatal producer errors")
    void shouldClassifyFatalProducerErrors() {
        assertThat(outboxPoller.isFatalProducerError(new org.apache.kafka.common.errors.RecordTooLargeException("Payload too large"))).isTrue();
        assertThat(outboxPoller.isFatalProducerError(new org.apache.kafka.common.errors.SerializationException("Cannot serialize"))).isTrue();
        assertThat(outboxPoller.isFatalProducerError(new org.apache.kafka.common.errors.InvalidTopicException("Invalid topic name"))).isTrue();

        // Wrapped in execution exception
        Exception wrapped = new java.util.concurrent.ExecutionException(new org.apache.kafka.common.errors.RecordTooLargeException("nested"));
        assertThat(outboxPoller.isFatalProducerError(wrapped)).isTrue();

        // Non-fatal errors
        assertThat(outboxPoller.isFatalProducerError(new org.apache.kafka.common.errors.TimeoutException("Timeout"))).isFalse();
        assertThat(outboxPoller.isFatalProducerError(new IllegalArgumentException("Arg error"))).isFalse();
        assertThat(outboxPoller.isFatalProducerError(null)).isFalse();
    }

    @Test
    @DisplayName("Senior Refinements v22: isBrokerOutage correctly classifies broker connectivity and outage errors")
    void shouldClassifyBrokerOutageErrors() {
        assertThat(outboxPoller.isBrokerOutage(new java.util.concurrent.TimeoutException("Future timeout"))).isTrue();
        assertThat(outboxPoller.isBrokerOutage(new org.apache.kafka.common.errors.TimeoutException("Kafka timeout"))).isTrue();
        assertThat(outboxPoller.isBrokerOutage(new org.apache.kafka.common.errors.DisconnectException("Broker disconnected"))).isTrue();
        assertThat(outboxPoller.isBrokerOutage(new org.apache.kafka.common.errors.NetworkException("Network partition"))).isTrue(); // NetworkException extends RetriableException
        assertThat(outboxPoller.isBrokerOutage(new java.net.ConnectException("Connection refused"))).isTrue();

        // Wrapped in execution exception
        Exception wrapped = new java.util.concurrent.ExecutionException(new org.apache.kafka.common.errors.DisconnectException("nested"));
        assertThat(outboxPoller.isBrokerOutage(wrapped)).isTrue();

        // Non-broker outage errors
        assertThat(outboxPoller.isBrokerOutage(new org.apache.kafka.common.errors.RecordTooLargeException("Big"))).isFalse();
        assertThat(outboxPoller.isBrokerOutage(new NullPointerException("NPE"))).isFalse();
        assertThat(outboxPoller.isBrokerOutage(null)).isFalse();
    }

    @Test
    @DisplayName("Senior Refinements v23: isKafkaThread identifies Kafka network threads correctly")
    void shouldDetectKafkaThread() {
        Thread kafkaThread = new Thread(() -> {}, "kafka-producer-network-thread | producer-1");
        Thread appThread = new Thread(() -> {}, "outbox-poller-1");
        assertThat(OutboxPoller.isKafkaThread(kafkaThread)).isTrue();
        assertThat(OutboxPoller.isKafkaThread(appThread)).isFalse();
        assertThat(OutboxPoller.isKafkaThread(null)).isFalse();
    }

    @Test
    @DisplayName("Senior Refinements v23: handleSendFailure during shutdown releases claim immediately to PENDING without failure count")
    void shouldReleaseClaimsImmediatelyOnShutdownDuringSendFailure() {
        OutboxMessage msg = new OutboxMessage(
                UUID.randomUUID(), "orders-topic", "KEY-1", "{}",
                Collections.emptyMap(), OutboxStatus.IN_PROGRESS, 2, Instant.now(), null, null, null
        );
        outboxPoller.shutdown();
        assertThat(outboxPoller.isRunning()).isFalse();

        OutboxPoller.DeliveryStatus status = outboxPoller.handleSendFailure(msg, new RuntimeException("Kafka send failed during shutdown"));

        assertThat(status).isEqualTo(OutboxPoller.DeliveryStatus.RECOVERABLE_BROKER_OUTAGE);
        verify(outboxRepository).releaseClaims(eq(List.of(msg.getId())), eq(Duration.ZERO));
        verify(outboxRepository, never()).markAsFailed(any(), any(), anyInt(), any());
        verify(outboxRepository, never()).rescheduleMessage(any(), any(), any());
    }

    @Test
    @DisplayName("Senior Refinements v26: releaseClaims uses Duration.ZERO when poller is stopped during broker outage")
    void shouldReleaseClaimsWithZeroDurationWhenShuttingDownOnBrokerOutage() {
        UUID id1 = UUID.randomUUID();
        UUID id2 = UUID.randomUUID();
        OutboxMessage m1 = new OutboxMessage(id1, "t", "k1", "{}", Collections.emptyMap(), OutboxStatus.IN_PROGRESS, 0, Instant.now(), null, null, null);
        OutboxMessage m2 = new OutboxMessage(id2, "t", "k2", "{}", Collections.emptyMap(), OutboxStatus.IN_PROGRESS, 0, Instant.now(), null, null, null);

        when(outboxRepository.claimBatchForProcessing(anyInt(), any(), anyInt(), any())).thenReturn(List.of(m1, m2));
        CompletableFuture<SendResult<Object, Object>> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new org.apache.kafka.common.errors.NetworkException("Connection reset"));
        when(kafkaOperations.send(anyProducerRecord())).thenAnswer(inv -> {
            outboxPoller.shutdown();
            return failedFuture;
        });

        outboxPoller.pollAndPublishBatch();

        verify(outboxRepository).releaseClaims(eq(List.of(id2)), eq(Duration.ZERO));
    }

    @Test
    @DisplayName("Senior Refinements Round 59: OutboxPoller implements SmartLifecycle with phase Integer.MAX_VALUE - 500 and initializes running to false")
    void shouldConformToSmartLifecycleContract() {
        OutboxPoller freshPoller = new OutboxPoller(outboxRepository, kafkaOperations, properties, IdemboxMetrics.noOp());
        assertThat(freshPoller.isAutoStartup()).isTrue();
        assertThat(freshPoller.getPhase()).isEqualTo(Integer.MAX_VALUE - 500);
        assertThat(freshPoller.isRunning()).isFalse();

        freshPoller.start();
        assertThat(freshPoller.isRunning()).isTrue();

        java.util.concurrent.atomic.AtomicBoolean callbackInvoked = new java.util.concurrent.atomic.AtomicBoolean(false);
        freshPoller.stop(() -> callbackInvoked.set(true));

        assertThat(freshPoller.isRunning()).isFalse();
        assertThat(callbackInvoked.get()).isTrue();

        freshPoller.start();
        assertThat(freshPoller.isRunning()).isTrue();
    }

    @Test
    @DisplayName("Senior Refinements Round 59: pollAndPublishBatch returns 0 without touching repository when not started")
    void shouldNotPollWhenPollerNotStarted() {
        OutboxPoller unstartedPoller = new OutboxPoller(outboxRepository, kafkaOperations, properties, IdemboxMetrics.noOp());
        assertThat(unstartedPoller.isRunning()).isFalse();

        int processed = unstartedPoller.pollAndPublishBatch();
        assertThat(processed).isEqualTo(0);
        verify(outboxRepository, never()).claimBatchForProcessing(anyInt(), any(), anyInt(), any());
    }

    @Test
    @DisplayName("Senior Refinements v28: Constructor rejects null KafkaOperations with NullPointerException")
    void shouldRejectNullKafkaOperationsInConstructor() {
        org.assertj.core.api.Assertions.assertThatThrownBy(() ->
                new OutboxPoller(outboxRepository, null, properties, IdemboxMetrics.noOp()))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("KafkaOperations must not be null for OutboxPoller");
    }

    @Test
    @DisplayName("Senior Refinements v30: createProducerRecord unwraps JSON scalar quotes for text/plain messages")
    void shouldUnwrapJsonScalarQuotesForPlainTextMessages() {
        UUID msgId = UUID.randomUUID();
        Map<String, String> headers = Map.of("contentType", "text/plain; charset=UTF-8");
        OutboxMessage message = new OutboxMessage(
                msgId, "ack-topic", "key-1", "\"PAYMENT-PROCESSED-OK\"",
                headers, OutboxStatus.PENDING, 0,
                Instant.now(), null, null, null
        );

        ProducerRecord<?, ?> record = outboxPoller.createProducerRecord(message);

        assertThat(record.value()).isEqualTo("PAYMENT-PROCESSED-OK");
    }

    @Test
    @DisplayName("Senior Refinements v30: createProducerRecord preserves quotes for JSON objects and arrays")
    void shouldPreservePayloadForJsonMessages() {
        UUID msgId = UUID.randomUUID();
        Map<String, String> headers = Map.of("contentType", "application/json");
        OutboxMessage message = new OutboxMessage(
                msgId, "orders-topic", "key-2", "{\"orderId\":\"123\"}",
                headers, OutboxStatus.PENDING, 0,
                Instant.now(), null, null, null
        );

        ProducerRecord<?, ?> record = outboxPoller.createProducerRecord(message);

        assertThat(record.value()).isEqualTo("{\"orderId\":\"123\"}");
    }

    @Test
    @DisplayName("Senior Refinements Round 53: isBrokerOutage identifies Kafka AuthorizationException and AuthenticationException")
    void shouldIdentifyAuthAndAclExceptionsAsBrokerOutage() {
        org.apache.kafka.common.errors.AuthorizationException aclError = new org.apache.kafka.common.errors.TopicAuthorizationException("Unauthorized");
        org.apache.kafka.common.errors.AuthenticationException authError = new org.apache.kafka.common.errors.SaslAuthenticationException("Invalid credentials");

        assertThat(outboxPoller.isFatalProducerError(aclError)).isFalse();
        assertThat(outboxPoller.isFatalProducerError(authError)).isFalse();

        assertThat(outboxPoller.isBrokerOutage(aclError)).isTrue();
        assertThat(outboxPoller.isBrokerOutage(authError)).isTrue();

        java.util.concurrent.ExecutionException wrappedAcl = new java.util.concurrent.ExecutionException(aclError);
        assertThat(outboxPoller.isBrokerOutage(wrappedAcl)).isTrue();
        assertThat(outboxPoller.isFatalProducerError(wrappedAcl)).isFalse();

        java.util.concurrent.ExecutionException wrappedAuth = new java.util.concurrent.ExecutionException(authError);
        assertThat(outboxPoller.isBrokerOutage(wrappedAuth)).isTrue();
        assertThat(outboxPoller.isFatalProducerError(wrappedAuth)).isFalse();
    }

    @Test
    @DisplayName("Senior Refinements Round 53: Authorization and authentication exceptions trigger broker cooldown and reschedule without burning retries")
    void shouldTriggerCooldownAndRescheduleOnAuthOrAclException() {
        UUID msgId = UUID.randomUUID();
        OutboxMessage message = new OutboxMessage(
                msgId, "secure-topic", "key-auth", "{\"id\":1}",
                Collections.emptyMap(), OutboxStatus.IN_PROGRESS, 0,
                Instant.now(), null, null, null
        );

        outboxPoller.start();
        OutboxPoller.DeliveryStatus status = outboxPoller.handleSendFailure(
                message,
                new org.apache.kafka.common.errors.TopicAuthorizationException("Not authorized to write to topic")
        );

        assertThat(status).isEqualTo(OutboxPoller.DeliveryStatus.RECOVERABLE_BROKER_OUTAGE);
        verify(outboxRepository, never()).markAsFailed(any(), any(), anyInt(), any());
        verify(outboxRepository).rescheduleMessage(eq(msgId), org.mockito.ArgumentMatchers.contains("TopicAuthorizationException"), any(Duration.class));
    }

    @Test
    @DisplayName("Senior Refinements v36: Trailing messages in batch are not starved by cumulative timeout compression")
    void shouldNotStarveTrailingMessagesByCumulativeTimeout() {
        OutboxPoller poller = new OutboxPoller(outboxRepository, kafkaOperations, properties, IdemboxMetrics.noOp());
        poller.start();
        properties.getOutbox().setSendTimeout(Duration.ofMillis(300));

        UUID id1 = UUID.randomUUID();
        UUID id2 = UUID.randomUUID();
        OutboxMessage m1 = new OutboxMessage(id1, "topic", "k1", "{}", Collections.emptyMap(), OutboxStatus.IN_PROGRESS, 0, Instant.now(), null, null, null);
        OutboxMessage m2 = new OutboxMessage(id2, "topic", "k2", "{}", Collections.emptyMap(), OutboxStatus.IN_PROGRESS, 0, Instant.now(), null, null, null);

        when(outboxRepository.claimBatchForProcessing(anyInt(), any(), anyInt(), any()))
                .thenReturn(List.of(m1, m2))
                .thenReturn(Collections.emptyList());

        CompletableFuture<SendResult<Object, Object>> f1 = new CompletableFuture<>();
        CompletableFuture<SendResult<Object, Object>> f2 = new CompletableFuture<>();

        when(kafkaOperations.send(anyProducerRecord()))
                .thenReturn(f1)
                .thenReturn(f2);

        RecordMetadata meta = new RecordMetadata(new TopicPartition("topic", 0), 0, 0, 0, 0, 0);

        // Simulate f1 completing after 120ms, and f2 completing after 220ms from start (100ms after f1)
        new Thread(() -> {
            try {
                Thread.sleep(120);
                f1.complete(new SendResult<>(new ProducerRecord<>("topic", "k1", "{}"), meta));
                Thread.sleep(100);
                f2.complete(new SendResult<>(new ProducerRecord<>("topic", "k2", "{}"), meta));
            } catch (InterruptedException ignored) {}
        }).start();

        int processed = poller.pollAndPublishBatch();

        // Both messages complete successfully without timeout
        assertThat(processed).isEqualTo(2);
        verify(outboxRepository).markAsSent(eq(id1), any(Instant.class));
        verify(outboxRepository).markAsSent(eq(id2), any(Instant.class));
    }

    @Test
    @DisplayName("Senior Refinements Round 63: in-flight dispatched messages are left in IN_PROGRESS on broker outage to prevent duplicate sends")
    void shouldLeaveInFlightDispatchedMessagesInProgressOnBrokerOutage() {
        UUID id1 = UUID.randomUUID();
        UUID id2 = UUID.randomUUID();
        OutboxMessage m1 = new OutboxMessage(id1, "topic", "k1", "{}", Collections.emptyMap(), OutboxStatus.IN_PROGRESS, 0, Instant.now(), null, null, null);
        OutboxMessage m2 = new OutboxMessage(id2, "topic", "k2", "{}", Collections.emptyMap(), OutboxStatus.IN_PROGRESS, 0, Instant.now(), null, null, null);

        when(outboxRepository.claimBatchForProcessing(anyInt(), any(Duration.class), anyInt(), any(Duration.class)))
                .thenReturn(List.of(m1, m2));

        CompletableFuture<SendResult<Object, Object>> f1 = new CompletableFuture<>();
        CompletableFuture<SendResult<Object, Object>> f2 = new CompletableFuture<>();

        when(kafkaOperations.send(anyProducerRecord()))
                .thenReturn(f1)
                .thenAnswer(inv -> {
                    // Complete f1 with broker timeout exception to trigger outage in Step 3
                    f1.completeExceptionally(new org.apache.kafka.common.errors.TimeoutException("Kafka broker disconnected"));
                    return f2;
                });

        int processed = outboxPoller.pollAndPublishBatch();

        // 1 attempted, 1 halted due to outage
        assertThat(processed).isEqualTo(1);

        // Dispatched in-flight m2 must NOT have claims released (left in IN_PROGRESS so whenComplete handles it safely)
        verify(outboxRepository, never()).releaseClaims(eq(List.of(id2)), any(Duration.class));
    }

    @Test
    @DisplayName("Senior Refinements Round 70: when async markAsSent fails in database, message is left in IN_PROGRESS and never rescheduled to PENDING to avoid duplicate Kafka publication")
    void shouldRescheduleMessageWhenAsyncMarkAsSentFailsInDatabase() {
        UUID id = UUID.randomUUID();
        OutboxMessage msg = new OutboxMessage(id, "topic", "k", "{}", Collections.emptyMap(), OutboxStatus.IN_PROGRESS, 0, Instant.now(), null, null, null);

        when(outboxRepository.claimBatchForProcessing(anyInt(), any(Duration.class), anyInt(), any(Duration.class)))
                .thenReturn(List.of(msg))
                .thenReturn(Collections.emptyList());

        CompletableFuture<SendResult<Object, Object>> future = new CompletableFuture<>();
        when(kafkaOperations.send(anyProducerRecord())).thenReturn(future);

        // Simulate database failure during markAsSent (e.g. pool timeout or deadlock)
        org.mockito.Mockito.doThrow(new org.springframework.dao.CannotAcquireLockException("Lock deadlock"))
                .when(outboxRepository).markAsSent(eq(id), any(Instant.class));

        RecordMetadata meta = new RecordMetadata(new TopicPartition("topic", 0), 0, 0, 0, 0, 0);
        future.complete(new SendResult<>(new ProducerRecord<>("topic", "k", "{}"), meta));

        int processed = outboxPoller.pollAndPublishBatch();

        assertThat(processed).isEqualTo(1);
        // Message was already delivered to Kafka; never revert to PENDING via rescheduleMessage or markAsFailed
        verify(outboxRepository, never()).rescheduleMessage(eq(id), any(), any(Duration.class), anyInt());
        verify(outboxRepository, never()).rescheduleMessage(eq(id), any(), any(Duration.class));
        verify(outboxRepository, never()).markAsFailed(eq(id), any(), anyInt(), any(Duration.class));
    }

    @Test
    @DisplayName("Senior Refinements v38: resolvePayloadForKafka should decode Base64 payload to byte[] when encoding or contentType is binary")
    void shouldDecodeBase64PayloadToByteArray() {
        byte[] original = new byte[]{1, 2, 3, 4, 5};
        String base64Json = "\"" + java.util.Base64.getEncoder().encodeToString(original) + "\"";
        OutboxMessage msg = new OutboxMessage(
                UUID.randomUUID(),
                "binary-topic",
                "key-1",
                base64Json,
                Map.of("__Encoding__", "base64", "contentType", "application/octet-stream"),
                OutboxStatus.PENDING,
                0,
                Instant.now(),
                null,
                null,
                null
        );

        Object resolved = OutboxPoller.resolvePayloadForKafka(msg);
        assertThat(resolved).isInstanceOf(byte[].class);
        assertThat((byte[]) resolved).isEqualTo(original);

        ProducerRecord<?, ?> record = outboxPoller.createProducerRecord(msg);
        assertThat(record.value()).isEqualTo(original);
    }

    @Test
    @DisplayName("Senior Refinements v38: resolvePayloadForKafka preserves standard JSON payloads as String")
    void shouldPreserveJsonPayloadsAsString() {
        String json = "{\"orderId\":\"123\"}";
        OutboxMessage msg = new OutboxMessage(
                UUID.randomUUID(),
                "json-topic",
                "key-1",
                json,
                Map.of("contentType", "application/json"),
                OutboxStatus.PENDING,
                0,
                Instant.now(),
                null,
                null,
                null
        );

        Object resolved = OutboxPoller.resolvePayloadForKafka(msg);
        assertThat(resolved).isEqualTo(json);
    }

    @Test
    @DisplayName("Senior Refinements v46: resolvePayloadForKafka returns byte[] when __PayloadType__ is bytes even if payload is valid JSON")
    void shouldReturnByteArrayWhenPayloadTypeIsBytes() {
        String json = "{\"orderId\":100,\"status\":\"PAID\"}";
        byte[] expectedBytes = json.getBytes(StandardCharsets.UTF_8);

        OutboxMessage msg = new OutboxMessage(
                UUID.randomUUID(),
                "json-bytes-topic",
                "key-1",
                json,
                Map.of("__PayloadType__", "bytes"),
                OutboxStatus.PENDING,
                0,
                Instant.now(),
                null,
                null,
                null
        );

        Object resolved = OutboxPoller.resolvePayloadForKafka(msg);
        assertThat(resolved).isInstanceOf(byte[].class);
        assertThat((byte[]) resolved).isEqualTo(expectedBytes);

        ProducerRecord<?, ?> record = outboxPoller.createProducerRecord(msg);
        assertThat(record.value()).isEqualTo(expectedBytes);
    }

    @Test
    @DisplayName("Senior Refinements v39: createProducerRecord does not duplicate X-Created-At header if present in message headers")
    void shouldNotDuplicateXCreatedAtHeaderWhenPresentInCustomHeaders() {
        Instant dbCreatedAt = Instant.ofEpochMilli(1700000000000L);
        String customCreatedAt = "1699999999999";

        OutboxMessage msg = new OutboxMessage(
                UUID.randomUUID(),
                "test-topic",
                "key-1",
                "{\"status\":\"ok\"}",
                Map.of("X-Created-At", customCreatedAt, "trace" + "parent", "00-abc-def-01"),
                OutboxStatus.PENDING,
                0,
                dbCreatedAt,
                null,
                null,
                null
        );

        ProducerRecord<?, ?> record = outboxPoller.createProducerRecord(msg);

        // Count occurrences of X-Created-At
        int count = 0;
        String headerValue = null;
        for (org.apache.kafka.common.header.Header header : record.headers()) {
            if ("X-Created-At".equalsIgnoreCase(header.key())) {
                count++;
                headerValue = new String(header.value(), StandardCharsets.UTF_8);
            }
        }

        assertThat(count).isEqualTo(1);
        assertThat(headerValue).isEqualTo(customCreatedAt);
    }

    @Test
    @DisplayName("Senior Refinements v41: persistent broker outage exceeding maxOutageDuration transitions to FAILED")
    void persistentBrokerOutageExceedingMaxDurationShouldTransitionToFailed() {
        UUID id = UUID.randomUUID();
        Instant oldCreatedAt = Instant.now().minus(Duration.ofHours(25));
        OutboxMessage msg = new OutboxMessage(
                id, "orders-topic", "key-1", "{}",
                Map.of(), OutboxStatus.IN_PROGRESS, 0,
                oldCreatedAt, null, null, null
        );

        when(outboxRepository.claimBatchForProcessing(anyInt(), any(Duration.class), anyInt(), any(Duration.class)))
                .thenReturn(List.of(msg))
                .thenReturn(Collections.emptyList());

        CompletableFuture<SendResult<Object, Object>> future = new CompletableFuture<>();
        future.completeExceptionally(new org.apache.kafka.common.errors.TimeoutException("Persistent cluster down"));
        when(kafkaOperations.send(anyProducerRecord())).thenReturn(future);

        outboxPoller.pollAndPublishBatch();

        verify(outboxRepository).markAsFailed(eq(id), org.mockito.ArgumentMatchers.contains("Exceeded maximum broker outage duration"), eq(1), eq(Duration.ZERO));
    }

    @Test
    @DisplayName("Senior Refinements v43: createProducerRecord preserves all headers for multi-value header keys")
    void createProducerRecordShouldPreserveMultiValueHeaders() {
        UUID id = UUID.randomUUID();
        Map<String, List<String>> multi = Map.of(
                "X-Trace-Step", List.of("gateway", "auth-filter", "order-service"),
                "Custom-Tag", List.of("v1")
        );
        OutboxMessage msg = new OutboxMessage(
                id, "orders-topic", "key-1", "{}",
                null, multi, OutboxStatus.IN_PROGRESS, 0,
                Instant.now(), null, null, null, null
        );

        ProducerRecord<?, ?> record = outboxPoller.createProducerRecord(msg);

        List<String> traceSteps = new ArrayList<>();
        for (org.apache.kafka.common.header.Header header : record.headers()) {
            if ("X-Trace-Step".equals(header.key())) {
                traceSteps.add(new String(header.value(), StandardCharsets.UTF_8));
            }
        }

        assertThat(traceSteps).containsExactly("gateway", "auth-filter", "order-service");
    }

    @Test
    @DisplayName("Senior Refinements v44: outage expired records terminal failure metric only without calling recordOutboxFailed")
    void outageExpiredShouldRecordTerminalFailureOnlyWithThresholdOne() {
        IdemboxMetrics mockMetrics = org.mockito.Mockito.mock(IdemboxMetrics.class);
        OutboxPoller poller = new OutboxPoller(outboxRepository, kafkaOperations, properties, mockMetrics, Runnable::run);
        poller.start();

        UUID msgId = UUID.randomUUID();
        Instant oldCreatedAt = Instant.now().minus(Duration.ofHours(25));
        OutboxMessage msg = new OutboxMessage(
                msgId, "outage-topic", "k1", "{\"data\":1}",
                Collections.emptyMap(), OutboxStatus.IN_PROGRESS, 0,
                oldCreatedAt, null, null, null
        );

        when(outboxRepository.claimBatchForProcessing(anyInt(), any(Duration.class), anyInt(), any(Duration.class)))
                .thenReturn(List.of(msg))
                .thenReturn(Collections.emptyList());

        CompletableFuture<SendResult<Object, Object>> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new org.apache.kafka.common.errors.TimeoutException("Cluster down"));
        when(kafkaOperations.send(anyProducerRecord())).thenReturn(failedFuture);

        poller.pollAndPublishBatch();

        verify(mockMetrics).recordOutboxTerminalFailure("outage-topic");
        verify(mockMetrics, never()).recordOutboxFailed(anyString());
        verify(outboxRepository).markAsFailed(eq(msgId), org.mockito.ArgumentMatchers.contains("Exceeded maximum broker outage duration"), eq(1), eq(Duration.ZERO));
    }

    @Test
    @DisplayName("Senior Refinements v45: onOutboxCommitted rejects execution on caller thread under CallerRunsPolicy")
    void onOutboxCommittedShouldRejectExecutionOnCallerThread() {
        // Executor configured to run tasks synchronously on caller thread (simulating CallerRunsPolicy)
        org.springframework.core.task.TaskExecutor callerRunsExecutor = Runnable::run;
        OutboxRepository mockRepo = org.mockito.Mockito.mock(OutboxRepository.class);
        OutboxPoller poller = new OutboxPoller(mockRepo, kafkaOperations, properties, IdemboxMetrics.noOp(), callerRunsExecutor);

        java.util.concurrent.atomic.AtomicBoolean isDispatchScheduled =
                (java.util.concurrent.atomic.AtomicBoolean) org.springframework.test.util.ReflectionTestUtils.getField(poller, "isDispatchScheduled");
        java.util.concurrent.atomic.AtomicBoolean pendingRepoll =
                (java.util.concurrent.atomic.AtomicBoolean) org.springframework.test.util.ReflectionTestUtils.getField(poller, "pendingRepoll");

        assertThat(isDispatchScheduled).isNotNull();
        assertThat(pendingRepoll).isNotNull();
        poller.onOutboxCommitted(new io.github.alexlitvinov.idembox.starter.publisher.DefaultOutboxPublisher.OutboxCommittedEvent(UUID.randomUUID()));

        // Verification: The task threw RejectedExecutionException instead of executing on the caller thread
        assertThat(isDispatchScheduled.get()).isFalse();
        assertThat(pendingRepoll.get()).isTrue();
        // Repository should NOT have been invoked to claim batches on caller thread
        verify(mockRepo, never()).claimBatchForProcessing(anyInt(), any(), anyInt(), any());
    }

    @Test
    @DisplayName("Senior Refinements v45: calculateBackoff is strictly capped at maxBackoff")
    void calculateBackoffShouldNeverExceedMaxBackoff() {
        Duration maxBackoff = properties.getOutbox().getMaxBackoff();
        // Run calculateBackoff 1000 times with high retryCount to ensure jitter never exceeds maxBackoff
        for (int i = 0; i < 1000; i++) {
            Duration backoff = outboxPoller.calculateBackoff(20);
            assertThat(backoff.toMillis()).isLessThanOrEqualTo(maxBackoff.toMillis());
            assertThat(backoff.toMillis()).isGreaterThanOrEqualTo(100L);
        }
    }

    @Test
    @DisplayName("Senior Refinements v45: pollAndPublishBatch loops iteratively without recursion when async executor rejects repoll")
    void pollAndPublishBatchShouldLoopIterativelyWhenRepollRejected() {
        org.springframework.core.task.TaskExecutor rejectingExecutor = org.mockito.Mockito.mock(org.springframework.core.task.TaskExecutor.class);
        org.mockito.Mockito.doThrow(new java.util.concurrent.RejectedExecutionException("Pool full"))
                .when(rejectingExecutor).execute(any(Runnable.class));

        OutboxRepository mockRepo = org.mockito.Mockito.mock(OutboxRepository.class);
        OutboxPoller poller = new OutboxPoller(mockRepo, kafkaOperations, properties, IdemboxMetrics.noOp(), rejectingExecutor);
        poller.start();

        java.util.concurrent.atomic.AtomicInteger claimInvocations = new java.util.concurrent.atomic.AtomicInteger();
        when(mockRepo.claimBatchForProcessing(anyInt(), any(), anyInt(), any())).thenAnswer(inv -> {
            int count = claimInvocations.incrementAndGet();
            if (count == 1) {
                // Set pendingRepoll on first iteration
                java.util.concurrent.atomic.AtomicBoolean pendingRepoll =
                        (java.util.concurrent.atomic.AtomicBoolean) org.springframework.test.util.ReflectionTestUtils.getField(poller, "pendingRepoll");
                assertThat(pendingRepoll).isNotNull();
                pendingRepoll.set(true);
            }
            return Collections.emptyList();
        });

        int processed = poller.pollAndPublishBatch();
        assertThat(processed).isEqualTo(0);
        assertThat(claimInvocations.get()).isEqualTo(2);
    }

    @Test
    @DisplayName("Senior Refinements v48: pollAndPublishBatch passes configured maxOutageDuration to claimBatchForProcessing")
    void shouldPassMaxOutageDurationToClaimBatch() {
        properties.getOutbox().setMaxOutageDuration(Duration.ofHours(3));

        when(outboxRepository.claimBatchForProcessing(eq(10), any(Duration.class), eq(3), eq(Duration.ofHours(3))))
                .thenReturn(Collections.emptyList());

        int processed = outboxPoller.pollAndPublishBatch();

        assertThat(processed).isEqualTo(0);
        verify(outboxRepository).claimBatchForProcessing(eq(10), any(Duration.class), eq(3), eq(Duration.ofHours(3)));
    }

    @Test
    @DisplayName("Senior Refinements Round 70: markAsSent failure leaves message in IN_PROGRESS and never reschedules to PENDING")
    void markAsSentFailureShouldNotReschedule() {
        UUID msgId = UUID.randomUUID();
        OutboxMessage msg = new OutboxMessage(
                msgId, "test-topic", "k1", "{}",
                Collections.emptyMap(), OutboxStatus.IN_PROGRESS, 0,
                Instant.now(), null, null, null
        );

        when(outboxRepository.claimBatchForProcessing(anyInt(), any(Duration.class), anyInt(), any(Duration.class)))
                .thenReturn(List.of(msg))
                .thenReturn(Collections.emptyList());

        CompletableFuture<SendResult<Object, Object>> sendFuture = CompletableFuture.completedFuture(org.mockito.Mockito.mock(ObjectSendResult.class));
        when(kafkaOperations.send(anyProducerRecord())).thenReturn(sendFuture);

        org.mockito.Mockito.doThrow(new org.springframework.dao.DataRetrievalFailureException("DB connection dropped"))
                .when(outboxRepository).markAsSent(eq(msgId), any());

        outboxPoller.pollAndPublishBatch();

        verify(outboxRepository, never()).rescheduleMessage(eq(msgId), any(), any(), anyInt());
        verify(outboxRepository, never()).rescheduleMessage(eq(msgId), any(), any());
        verify(outboxRepository, never()).markAsFailed(eq(msgId), any(), anyInt(), any(Duration.class));
    }

    @Test
    @DisplayName("Senior Refinements Round 52: createProducerRecord preserves headers with null values")
    void createProducerRecordShouldPreserveNullValuedHeaders() {
        UUID id = UUID.randomUUID();
        Map<String, String> headers = new java.util.HashMap<>();
        headers.put("X-Presence-Flag", null);
        headers.put("X-Custom-Header", "value1");

        OutboxMessage msg = new OutboxMessage(
                id, "orders-topic", "key-1", "{}",
                headers, OutboxStatus.IN_PROGRESS, 0,
                Instant.now(), null, null, null
        );

        ProducerRecord<?, ?> record = outboxPoller.createProducerRecord(msg);

        boolean foundNullHeader = false;
        boolean foundCustomHeader = false;
        for (org.apache.kafka.common.header.Header header : record.headers()) {
            if ("X-Presence-Flag".equals(header.key())) {
                foundNullHeader = true;
                assertThat(header.value()).isNull();
            } else if ("X-Custom-Header".equals(header.key())) {
                foundCustomHeader = true;
                assertThat(new String(header.value(), StandardCharsets.UTF_8)).isEqualTo("value1");
            }
        }

        assertThat(foundNullHeader).isTrue();
        assertThat(foundCustomHeader).isTrue();
    }

    @Test
    @DisplayName("Senior Refinements Round 53: failureHandled prevents duplicate handleSendFailure execution on concurrent whenComplete and Step 3")
    void shouldExecuteHandleSendFailureAtMostOncePerMessage() {
        UUID msgId = UUID.randomUUID();
        OutboxMessage message = new OutboxMessage(
                msgId, "orders-topic", "K1", "{}",
                Collections.emptyMap(), OutboxStatus.IN_PROGRESS, 0,
                Instant.now(), null, null, null
        );

        when(outboxRepository.claimBatchForProcessing(anyInt(), any(Duration.class), anyInt(), any(Duration.class)))
                .thenReturn(List.of(message))
                .thenReturn(Collections.emptyList());

        CompletableFuture<SendResult<Object, Object>> failedFuture = new CompletableFuture<>();
        when(kafkaOperations.send(anyProducerRecord())).thenReturn(failedFuture);

        // Set brokerCooldownUntilMs in the past so cooldown is not currently active
        outboxPoller.setBrokerCooldownUntilMs(System.currentTimeMillis() - 1000L);

        // Fail the future with a non-broker error
        failedFuture.completeExceptionally(new IllegalArgumentException("Bad message"));

        int processed = outboxPoller.pollAndPublishBatch();
        assertThat(processed).isEqualTo(1);

        // markAsFailed must be called exactly once, not twice
        verify(outboxRepository, times(1)).markAsFailed(eq(msgId), anyString(), anyInt(), any(Duration.class));
    }

    @Test
    @DisplayName("Senior Refinements v55: extractErrorReason avoids cutting high surrogate without low surrogate")
    void shouldAvoidSplittingSurrogatePairInExtractErrorReason() {
        // "RuntimeException: " is 18 chars. Prefix of 1978 puts high surrogate at index 1996
        String prefix = "A".repeat(1978);
        String emoji = "\uD83D\uDE00"; // high surrogate \uD83D, low surrogate \uDE00
        RuntimeException ex = new RuntimeException(prefix + emoji + "extra data that makes it over 2000 chars");

        String reason = outboxPoller.extractErrorReason(ex, "default");

        assertThat(reason).endsWith("...");
        // Ensure no unpaired surrogates exist in the entire truncated reason
        for (int i = 0; i < reason.length(); i++) {
            char c = reason.charAt(i);
            if (Character.isHighSurrogate(c)) {
                assertThat(i + 1).isLessThan(reason.length());
                assertThat(Character.isLowSurrogate(reason.charAt(i + 1))).isTrue();
            } else if (Character.isLowSurrogate(c)) {
                assertThat(i - 1).isGreaterThanOrEqualTo(0);
                assertThat(Character.isHighSurrogate(reason.charAt(i - 1))).isTrue();
            }
        }
    }

    @Test
    @DisplayName("Round 56: Step 3 uses individual remaining timeout per message and independent dbMarkTimeoutMs")
    void step3ShouldUseIndividualRemainingTimeoutPerMessage() {
        UUID msgId1 = UUID.randomUUID();
        UUID msgId2 = UUID.randomUUID();
        OutboxMessage message1 = new OutboxMessage(
                msgId1, "orders-topic", "K1", "{\"p\":1}",
                Collections.emptyMap(), OutboxStatus.IN_PROGRESS, 0,
                Instant.now(), null, null, null
        );
        OutboxMessage message2 = new OutboxMessage(
                msgId2, "orders-topic", "K2", "{\"p\":2}",
                Collections.emptyMap(), OutboxStatus.IN_PROGRESS, 0,
                Instant.now(), null, null, null
        );

        when(outboxRepository.claimBatchForProcessing(anyInt(), any(Duration.class), anyInt(), any(Duration.class)))
                .thenReturn(List.of(message1, message2))
                .thenReturn(Collections.emptyList());

        RecordMetadata metadata = new RecordMetadata(new TopicPartition("orders-topic", 0), 0, 0, 0, 0, 0);
        SendResult<Object, Object> sendResult1 = new SendResult<>(new ProducerRecord<>("orders-topic", "K1", "{\"p\":1}"), metadata);
        SendResult<Object, Object> sendResult2 = new SendResult<>(new ProducerRecord<>("orders-topic", "K2", "{\"p\":2}"), metadata);

        CompletableFuture<SendResult<Object, Object>> f1 = CompletableFuture.completedFuture(sendResult1);
        CompletableFuture<SendResult<Object, Object>> f2 = CompletableFuture.completedFuture(sendResult2);

        when(kafkaOperations.send(anyProducerRecord()))
                .thenReturn(f1)
                .thenReturn(f2);

        int processed = outboxPoller.pollAndPublishBatch();
        assertThat(processed).isEqualTo(2);

        verify(outboxRepository).markAsSent(eq(msgId1), any(Instant.class));
        verify(outboxRepository).markAsSent(eq(msgId2), any(Instant.class));
    }

    @Test
    @DisplayName("Senior Refinements Round 57: createProducerRecord preserves flat headers when multiHeaders is present")
    void shouldPreserveFlatHeadersWhenMultiHeadersPresent() {
        UUID id = UUID.randomUUID();
        Map<String, String> flatHeaders = Map.of(
                "X-Flat-Header", "flatValue",
                "X-Multi-Header", "shouldBeShadowed"
        );
        Map<String, List<String>> multiHeaders = Map.of(
                "X-Multi-Header", List.of("multiValue1", "multiValue2")
        );
        OutboxMessage msg = new OutboxMessage(
                id, "orders-topic", "key-1", "{}",
                flatHeaders, multiHeaders, OutboxStatus.IN_PROGRESS, 0,
                Instant.now(), null, null, null, null
        );

        ProducerRecord<?, ?> record = outboxPoller.createProducerRecord(msg);

        List<String> multiValues = new ArrayList<>();
        String flatValue = null;
        for (org.apache.kafka.common.header.Header header : record.headers()) {
            if ("X-Multi-Header".equals(header.key())) {
                multiValues.add(new String(header.value(), StandardCharsets.UTF_8));
            } else if ("X-Flat-Header".equals(header.key())) {
                flatValue = new String(header.value(), StandardCharsets.UTF_8);
            }
        }

        assertThat(multiValues).containsExactly("multiValue1", "multiValue2");
        assertThat(flatValue).isEqualTo("flatValue");
    }

    @Test
    @DisplayName("Senior Refinements Round 57: resolvePayloadForKafka decodes Base64 payload when encoding and contentType are in multiHeaders")
    void shouldDecodeBase64PayloadWhenHeadersInMultiHeaders() {
        byte[] original = new byte[]{10, 20, 30, 40, 50};
        String base64Json = "\"" + java.util.Base64.getEncoder().encodeToString(original) + "\"";
        OutboxMessage msg = new OutboxMessage(
                UUID.randomUUID(),
                "binary-topic",
                "key-1",
                base64Json,
                null,
                Map.of("__Encoding__", List.of("base64"), "contentType", List.of("application/octet-stream")),
                OutboxStatus.PENDING,
                0,
                Instant.now(),
                null,
                null,
                null,
                null
        );

        Object resolved = OutboxPoller.resolvePayloadForKafka(msg);
        assertThat(resolved).isInstanceOf(byte[].class);
        assertThat((byte[]) resolved).isEqualTo(original);

        ProducerRecord<?, ?> record = outboxPoller.createProducerRecord(msg);
        assertThat(record.value()).isEqualTo(original);
    }

    @Test
    @DisplayName("Senior Refinements Round 57: pollAndPublishBatch schedules repoll on async executor after clearing isPolling")
    void shouldScheduleRepollAfterClearingIsPolling() {
        java.util.concurrent.atomic.AtomicBoolean capturedIsPollingWhenTaskSubmitted = new java.util.concurrent.atomic.AtomicBoolean(true);
        java.util.concurrent.atomic.AtomicInteger submittedCount = new java.util.concurrent.atomic.AtomicInteger(0);

        OutboxPoller[] pollerRef = new OutboxPoller[1];
        org.springframework.core.task.TaskExecutor testExecutor = task -> {
            submittedCount.incrementAndGet();
            java.util.concurrent.atomic.AtomicBoolean isPollingField =
                    (java.util.concurrent.atomic.AtomicBoolean) org.springframework.test.util.ReflectionTestUtils.getField(pollerRef[0], "isPolling");
            assertThat(isPollingField).isNotNull();
            capturedIsPollingWhenTaskSubmitted.set(isPollingField.get());
        };

        OutboxPoller poller = new OutboxPoller(outboxRepository, kafkaOperations, properties, IdemboxMetrics.noOp(), testExecutor);
        poller.start();
        pollerRef[0] = poller;
        poller.setBrokerCooldownUntilMs(System.currentTimeMillis() - 1000L);

        when(outboxRepository.claimBatchForProcessing(anyInt(), any(Duration.class), anyInt(), any(Duration.class)))
                .thenAnswer(inv -> {
                    java.util.concurrent.atomic.AtomicBoolean pendingRepoll =
                            (java.util.concurrent.atomic.AtomicBoolean) org.springframework.test.util.ReflectionTestUtils.getField(poller, "pendingRepoll");
                    assertThat(pendingRepoll).isNotNull();
                    pendingRepoll.set(true);
                    return Collections.emptyList();
                });

        poller.pollAndPublishBatch();

        assertThat(submittedCount.get()).isEqualTo(1);
        assertThat(capturedIsPollingWhenTaskSubmitted.get()).isFalse();
    }

    @Test
    @DisplayName("Senior Refinements Round 58: handleSendFailure calculates progressive backoff based on elapsed time during broker outage")
    void shouldCalculateProgressiveBackoffDuringBrokerOutage() {
        properties.getOutbox().setInitialBackoff(Duration.ofMillis(200));
        properties.getOutbox().setMaxBackoff(Duration.ofSeconds(60));
        properties.getOutbox().setBackoffMultiplier(2.0);
        properties.getOutbox().setBrokerCooldown(Duration.ofMillis(100));

        // Message created 20 seconds ago, retryCount = 0 (preserved across broker outage reschedules)
        Instant createdAt = Instant.now().minusSeconds(20);
        OutboxMessage msg = new OutboxMessage(
                UUID.randomUUID(), "orders-topic", "key-1", "{}",
                Collections.emptyMap(), OutboxStatus.IN_PROGRESS, 0,
                createdAt, null, null, null
        );

        org.apache.kafka.common.errors.DisconnectException outageEx =
                new org.apache.kafka.common.errors.DisconnectException("Broker disconnected");

        ArgumentCaptor<Duration> deferralCaptor = ArgumentCaptor.forClass(Duration.class);

        outboxPoller.handleSendFailure(msg, outageEx);

        verify(outboxRepository).rescheduleMessage(eq(msg.getId()), anyString(), deferralCaptor.capture());
        Duration capturedDeferral = deferralCaptor.getValue();

        // 20 seconds elapsed / 5000 = 4 powers of 2.0 -> 200ms * 2^4 = 3200ms
        assertThat(capturedDeferral.toMillis()).isGreaterThanOrEqualTo(3000L);
    }

    @Test
    @DisplayName("Senior Refinements Round 59: calculateBackoff is safe against negative, zero, and corrupted configurations")
    void calculateBackoffShouldNotThrowOnCorruptedOrZeroBounds() {
        // Zero initial backoff and zero max backoff
        properties.getOutbox().setInitialBackoff(Duration.ZERO);
        properties.getOutbox().setMaxBackoff(Duration.ZERO);
        properties.getOutbox().setBackoffMultiplier(0.0);

        for (int i = -5; i <= 50; i++) {
            Duration backoff = outboxPoller.calculateBackoff(i);
            assertThat(backoff).isNotNull();
            assertThat(backoff.toMillis()).isGreaterThanOrEqualTo(80L);
        }

        // Negative duration configurations
        properties.getOutbox().setInitialBackoff(Duration.ofMillis(-100));
        properties.getOutbox().setMaxBackoff(Duration.ofMillis(-500));
        properties.getOutbox().setBackoffMultiplier(-2.0);

        for (int i = 0; i <= 10; i++) {
            Duration backoff = outboxPoller.calculateBackoff(i);
            assertThat(backoff).isNotNull();
            assertThat(backoff.toMillis()).isGreaterThanOrEqualTo(80L);
        }
    }

    @Test
    @DisplayName("Senior Refinements Round 60: pollAndPublishBatch loops iteratively via repeatDirectly when outboxAsyncExecutor is null and pendingRepoll is set")
    void pollAndPublishBatchShouldLoopIterativelyWhenAsyncExecutorIsNullAndPendingRepollSet() {
        OutboxRepository mockRepo = org.mockito.Mockito.mock(OutboxRepository.class);
        OutboxPoller poller = new OutboxPoller(mockRepo, kafkaOperations, properties, IdemboxMetrics.noOp(), null);
        poller.start();

        java.util.concurrent.atomic.AtomicInteger claimInvocations = new java.util.concurrent.atomic.AtomicInteger();
        when(mockRepo.claimBatchForProcessing(anyInt(), any(), anyInt(), any())).thenAnswer(inv -> {
            int count = claimInvocations.incrementAndGet();
            if (count == 1) {
                // Set pendingRepoll on first iteration
                java.util.concurrent.atomic.AtomicBoolean pendingRepoll =
                        (java.util.concurrent.atomic.AtomicBoolean) org.springframework.test.util.ReflectionTestUtils.getField(poller, "pendingRepoll");
                assertThat(pendingRepoll).isNotNull();
                pendingRepoll.set(true);
            }
            return Collections.emptyList();
        });

        int processed = poller.pollAndPublishBatch();
        assertThat(processed).isEqualTo(0);
        assertThat(claimInvocations.get()).isEqualTo(2);
    }

    @Test
    @DisplayName("Senior Refinements Round 62: Step 3 timeout on dbMarkFuture does not call rescheduleMessage when Kafka send succeeded")
    void step3DbMarkTimeoutShouldNotRescheduleWhenKafkaSendSucceeded() {
        UUID msgId = UUID.randomUUID();
        OutboxMessage message = new OutboxMessage(
                msgId, "orders-topic", "ORD-1", "{\"amount\":100}",
                Collections.emptyMap(), OutboxStatus.IN_PROGRESS, 0,
                Instant.now(), null, null, null
        );

        when(outboxRepository.claimBatchForProcessing(anyInt(), any(Duration.class), anyInt(), any(Duration.class)))
                .thenReturn(List.of(message))
                .thenReturn(Collections.emptyList());

        RecordMetadata metadata = new RecordMetadata(new TopicPartition("orders-topic", 0), 0, 0, 0, 0, 0);
        SendResult<Object, Object> sendResult = new SendResult<>(new ProducerRecord<>("orders-topic", "ORD-1", "{\"amount\":100}"), metadata);

        java.util.concurrent.CountDownLatch asyncStarted = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.CountDownLatch pauseLatch = new java.util.concurrent.CountDownLatch(1);

        CompletableFuture<SendResult<Object, Object>> sendFuture = new CompletableFuture<>() {
            @Override
            public SendResult<Object, Object> get(long timeout, java.util.concurrent.TimeUnit unit)
                    throws InterruptedException {
                boolean started = asyncStarted.await(2, java.util.concurrent.TimeUnit.SECONDS);
                assertThat(started).isTrue();
                return sendResult;
            }
        };
        sendFuture.complete(sendResult);
        when(kafkaOperations.send(anyProducerRecord())).thenReturn(sendFuture);

        org.mockito.Mockito.doAnswer(inv -> {
            asyncStarted.countDown();
            boolean paused = pauseLatch.await(5, java.util.concurrent.TimeUnit.SECONDS);
            assertThat(paused).isTrue();
            return null;
        }).doThrow(new org.springframework.dao.DataRetrievalFailureException("DB down fallback"))
          .when(outboxRepository).markAsSent(eq(msgId), any(Instant.class));

        org.springframework.core.task.TaskExecutor backgroundExecutor = task -> new Thread(task, "step3-timeout-test-thread").start();

        properties.getOutbox().setSendTimeout(Duration.ofMillis(1000));
        OutboxPoller testPoller = new OutboxPoller(outboxRepository, kafkaOperations, properties, IdemboxMetrics.noOp(), backgroundExecutor);
        testPoller.start();

        try {
            int processed = testPoller.pollAndPublishBatch();
            assertThat(processed).isEqualTo(1);
        } finally {
            pauseLatch.countDown();
        }

        // Must never call rescheduleMessage because message was confirmed delivered to Kafka
        verify(outboxRepository, never()).rescheduleMessage(eq(msgId), anyString(), any(), anyInt());
        verify(outboxRepository, never()).rescheduleMessage(eq(msgId), anyString(), any());
    }

    @Test
    @DisplayName("Senior Refinements Round 65: OutboxPoller leaves message IN_PROGRESS and does not reschedule to PENDING on local future.get TimeoutException")
    void shouldNotRescheduleToPendingOnLocalFutureGetTimeoutException() {
        UUID msgId = UUID.randomUUID();
        OutboxMessage msg = new OutboxMessage(msgId, "timeout-topic", "k", "{\"v\":1}", Collections.emptyMap(), OutboxStatus.IN_PROGRESS, 0, Instant.now(), null, null, null);

        when(outboxRepository.claimBatchForProcessing(anyInt(), any(Duration.class), anyInt(), any(Duration.class)))
                .thenReturn(List.of(msg))
                .thenReturn(Collections.emptyList());

        CompletableFuture<SendResult<Object, Object>> sendFuture = new CompletableFuture<>();
        when(kafkaOperations.send(anyProducerRecord())).thenReturn(sendFuture);

        properties.getOutbox().setSendTimeout(Duration.ofMillis(50));
        OutboxPoller testPoller = new OutboxPoller(outboxRepository, kafkaOperations, properties, IdemboxMetrics.noOp(), null);
        testPoller.start();

        int processed = testPoller.pollAndPublishBatch();
        assertThat(processed).isEqualTo(1);

        // Crucial: Must NEVER reschedule message to PENDING on local wait timeout!
        verify(outboxRepository, never()).rescheduleMessage(eq(msgId), anyString(), any());
        verify(outboxRepository, never()).rescheduleMessage(eq(msgId), anyString(), any(), anyInt());

        // Now background Kafka delivery completes with success
        RecordMetadata metadata = new RecordMetadata(new TopicPartition("timeout-topic", 0), 0, 0, 0, 0, 0);
        SendResult<Object, Object> sendResult = new SendResult<>(new ProducerRecord<>("timeout-topic", "k", "{}"), metadata);
        sendFuture.complete(sendResult);

        // whenComplete callback marks message as SENT
        verify(outboxRepository).markAsSent(eq(msgId), any(Instant.class));
    }

    @Test
    @DisplayName("Senior Refinements Round 67: resolvePayloadForKafka unwraps JSON quotes for XML, CSV, YAML, and other text payloads")
    void shouldUnwrapJsonQuotesForNonJsonTextPayloads() {
        // XML test
        String rawXml = "<order><id>123</id></order>";
        String quotedXml = "\"<order><id>123</id></order>\"";
        OutboxMessage xmlMsg = new OutboxMessage(
                UUID.randomUUID(), "xml-topic", "k1", quotedXml,
                Map.of("contentType", "application/xml; charset=UTF-8"),
                OutboxStatus.PENDING, 0, Instant.now(), null, null, null
        );
        assertThat(OutboxPoller.resolvePayloadForKafka(xmlMsg)).isEqualTo(rawXml);

        // CSV test
        String rawCsv = "id,sku,amount\n1,SKU-A,50.0";
        String quotedCsv = "\"" + rawCsv.replace("\n", "\\n") + "\"";
        OutboxMessage csvMsg = new OutboxMessage(
                UUID.randomUUID(), "csv-topic", "k2", quotedCsv,
                Map.of("contentType", "text/csv"),
                OutboxStatus.PENDING, 0, Instant.now(), null, null, null
        );
        assertThat(OutboxPoller.resolvePayloadForKafka(csvMsg)).isEqualTo(rawCsv);

        // YAML test
        String rawYaml = "orderId: 456\nstatus: confirmed";
        String quotedYaml = "\"orderId: 456\\nstatus: confirmed\"";
        OutboxMessage yamlMsg = new OutboxMessage(
                UUID.randomUUID(), "yaml-topic", "k3", quotedYaml,
                Map.of("contentType", "application/yaml"),
                OutboxStatus.PENDING, 0, Instant.now(), null, null, null
        );
        assertThat(OutboxPoller.resolvePayloadForKafka(yamlMsg)).isEqualTo(rawYaml);
    }

    @Test
    @DisplayName("Senior Refinements Round 68: whenComplete always executes failTask when future completes exceptionally outside broker cooldown")
    void whenCompleteShouldAlwaysHandleFailureEvenWhenOutsideBrokerCooldown() {
        UUID msgId = UUID.randomUUID();
        OutboxMessage message = new OutboxMessage(
                msgId, "orders-topic", "ORD-1", "{\"amount\":100}",
                Collections.emptyMap(), OutboxStatus.IN_PROGRESS, 0,
                Instant.now(), null, null, null
        );

        when(outboxRepository.claimBatchForProcessing(anyInt(), any(Duration.class), anyInt(), any(Duration.class)))
                .thenReturn(List.of(message))
                .thenReturn(Collections.emptyList());

        CompletableFuture<SendResult<Object, Object>> sendFuture = new CompletableFuture<>();
        when(kafkaOperations.send(anyProducerRecord())).thenReturn(sendFuture);

        // Step 2 & 3: Poller dispatches and starts awaiting.
        // Complete the future exceptionally from another thread (simulating Kafka client callback)
        sendFuture.completeExceptionally(new org.apache.kafka.common.errors.NetworkException("Connection reset"));

        int processed = outboxPoller.pollAndPublishBatch();

        assertThat(processed).isEqualTo(1);
        // Verify failure was handled and message was rescheduled (broker outage)
        verify(outboxRepository).rescheduleMessage(eq(msgId), org.mockito.ArgumentMatchers.contains("NetworkException"), any(Duration.class));
    }

    @Test
    @DisplayName("Senior Refinements Round 68: poller propagates nextRetryAt lease deadline to markAsSent when present")
    void shouldPropagateLeaseDeadlineWhenPresentOnMessage() {
        UUID msgId = UUID.randomUUID();
        Instant deadline = Instant.now().plusSeconds(120);
        OutboxMessage message = new OutboxMessage(
                msgId, "orders-topic", "ORD-1", "{\"amount\":100}",
                Collections.emptyMap(), OutboxStatus.IN_PROGRESS, 0,
                Instant.now(), null, deadline, null
        );

        when(outboxRepository.claimBatchForProcessing(anyInt(), any(Duration.class), anyInt(), any(Duration.class)))
                .thenReturn(List.of(message))
                .thenReturn(Collections.emptyList());

        RecordMetadata metadata = new RecordMetadata(new TopicPartition("orders-topic", 0), 0, 0, 0, 0, 0);
        SendResult<Object, Object> sendResult = new SendResult<>(new ProducerRecord<>("orders-topic", "ORD-1", "{\"amount\":100}"), metadata);
        when(kafkaOperations.send(anyProducerRecord())).thenReturn(CompletableFuture.completedFuture(sendResult));

        int processed = outboxPoller.pollAndPublishBatch();

        assertThat(processed).isEqualTo(1);
        verify(outboxRepository).markAsSent(eq(msgId), any(Instant.class), eq(deadline));
    }

    @Test
    @DisplayName("Senior Refinements Round 68: poller propagates nextRetryAt lease deadline to markAsFailed when present")
    void shouldPropagateLeaseDeadlineOnFailureWhenPresent() {
        UUID msgId = UUID.randomUUID();
        Instant deadline = Instant.now().plusSeconds(120);
        OutboxMessage message = new OutboxMessage(
                msgId, "orders-topic", "ORD-1", "{\"amount\":100}",
                Collections.emptyMap(), OutboxStatus.IN_PROGRESS, 0,
                Instant.now(), null, deadline, null
        );

        when(outboxRepository.claimBatchForProcessing(anyInt(), any(Duration.class), anyInt(), any(Duration.class)))
                .thenReturn(List.of(message))
                .thenReturn(Collections.emptyList());

        when(kafkaOperations.send(anyProducerRecord()))
                .thenReturn(CompletableFuture.failedFuture(new org.apache.kafka.common.errors.RecordTooLargeException("Too large")));

        int processed = outboxPoller.pollAndPublishBatch();

        assertThat(processed).isEqualTo(1);
        verify(outboxRepository).markAsFailed(eq(msgId), org.mockito.ArgumentMatchers.contains("RecordTooLargeException"), eq(1), eq(Duration.ZERO), eq(deadline));
    }

    @Test
    @DisplayName("Senior Refinements Round 69: poller groups unattempted claims by nextRetryAt when releasing claims with heterogeneous deadlines")
    void shouldGroupUnattemptedClaimsByNextRetryAtOnRelease() {
        UUID msg1Id = UUID.randomUUID();
        UUID msg2Id = UUID.randomUUID();
        UUID msg3Id = UUID.randomUUID();

        Instant deadline1 = Instant.now().plusSeconds(60);
        Instant deadline2 = Instant.now().plusSeconds(120);

        OutboxMessage msg1 = new OutboxMessage(
                msg1Id, "topic", "k1", "payload",
                Collections.emptyMap(), OutboxStatus.IN_PROGRESS, 0,
                Instant.now(), null, deadline1, null
        );
        OutboxMessage msg2 = new OutboxMessage(
                msg2Id, "topic", "k2", "payload",
                Collections.emptyMap(), OutboxStatus.IN_PROGRESS, 0,
                Instant.now(), null, deadline1, null
        );
        OutboxMessage msg3 = new OutboxMessage(
                msg3Id, "topic", "k3", "payload",
                Collections.emptyMap(), OutboxStatus.IN_PROGRESS, 0,
                Instant.now(), null, deadline2, null
        );

        when(outboxRepository.claimBatchForProcessing(anyInt(), any(Duration.class), anyInt(), any(Duration.class)))
                .thenReturn(List.of(msg1, msg2, msg3))
                .thenReturn(Collections.emptyList());

        when(kafkaOperations.send(anyProducerRecord()))
                .thenReturn(CompletableFuture.failedFuture(new org.apache.kafka.common.errors.DisconnectException("Broker disconnected")));

        outboxPoller.pollAndPublishBatch();

        verify(outboxRepository).releaseClaims(eq(List.of(msg2Id)), any(Duration.class), eq(deadline1));
        verify(outboxRepository).releaseClaims(eq(List.of(msg3Id)), any(Duration.class), eq(deadline2));
    }

    @Test
    @DisplayName("Senior Refinements Round 70: processSingleMessage returns SUCCESS and never reschedules to PENDING when markAsSent fails in database")
    void shouldReturnSuccessAndNotRescheduleWhenProcessSingleMessageMarkAsSentFails() {
        UUID msgId = UUID.randomUUID();
        OutboxMessage msg = new OutboxMessage(
                msgId, "topic", "k", "{}",
                Collections.emptyMap(), OutboxStatus.IN_PROGRESS, 0,
                Instant.now(), null, null, null
        );

        RecordMetadata meta = new RecordMetadata(new TopicPartition("topic", 0), 0, 0, 0, 0, 0);
        CompletableFuture<SendResult<Object, Object>> sendFuture = CompletableFuture.completedFuture(
                new SendResult<>(new ProducerRecord<>("topic", "k", "{}"), meta)
        );
        when(kafkaOperations.send(anyProducerRecord())).thenReturn(sendFuture);

        org.mockito.Mockito.doThrow(new org.springframework.dao.CannotAcquireLockException("DB lock deadlock"))
                .when(outboxRepository).markAsSent(eq(msgId), any(Instant.class));

        OutboxPoller.DeliveryStatus status = outboxPoller.processSingleMessage(msg);

        assertThat(status).isEqualTo(OutboxPoller.DeliveryStatus.SUCCESS);
        verify(outboxRepository, never()).rescheduleMessage(eq(msgId), any(), any(), anyInt());
        verify(outboxRepository, never()).rescheduleMessage(eq(msgId), any(), any());
        verify(outboxRepository, never()).markAsFailed(eq(msgId), any(), anyInt(), any(Duration.class));
    }

    @Test
    @DisplayName("Senior Refinements Round 73: pollAndPublishBatch awaits async failTask completion to detect RECOVERABLE_BROKER_OUTAGE and release unattempted claims")
    void shouldAwaitAsyncFailTaskToDetectBrokerOutageWithoutRaceCondition() {
        UUID msg1Id = UUID.randomUUID();
        UUID msg2Id = UUID.randomUUID();
        Instant deadline = Instant.now().plusSeconds(60);

        OutboxMessage msg1 = new OutboxMessage(
                msg1Id, "topic", "k1", "{}",
                Collections.emptyMap(), OutboxStatus.IN_PROGRESS, 0,
                Instant.now(), null, deadline, null
        );
        OutboxMessage msg2 = new OutboxMessage(
                msg2Id, "topic", "k2", "{}",
                Collections.emptyMap(), OutboxStatus.IN_PROGRESS, 0,
                Instant.now(), null, deadline, null
        );

        org.springframework.core.task.TaskExecutor asyncExecutor = task -> new Thread(task, "async-fail-task-thread").start();
        OutboxPoller asyncPoller = new OutboxPoller(outboxRepository, kafkaOperations, properties, IdemboxMetrics.noOp(), asyncExecutor);
        asyncPoller.start();

        when(outboxRepository.claimBatchForProcessing(anyInt(), any(Duration.class), anyInt(), any(Duration.class)))
                .thenReturn(List.of(msg1, msg2))
                .thenReturn(Collections.emptyList());

        // Slow down rescheduleMessage on the background thread so poller thread hits resolveFailureStatus while DB update is still in-flight
        when(outboxRepository.rescheduleMessage(eq(msg1Id), anyString(), any(Duration.class), eq(deadline)))
                .thenAnswer(inv -> {
                    Thread.sleep(60);
                    return true;
                });

        when(kafkaOperations.send(anyProducerRecord()))
                .thenReturn(CompletableFuture.failedFuture(new org.apache.kafka.common.errors.DisconnectException("Broker down")));

        int processed = asyncPoller.pollAndPublishBatch();

        assertThat(processed).isEqualTo(1);
        // Only msg1 was sent; msg2 was NOT sent because RECOVERABLE_BROKER_OUTAGE was awaited and detected
        verify(kafkaOperations, times(1)).send(anyProducerRecord());
        verify(outboxRepository).releaseClaims(eq(List.of(msg2Id)), any(Duration.class), eq(deadline));
    }

    @Test
    @DisplayName("Senior Refinements Round 73: resolvePayloadForKafka preserves byte[] for JSON with octet-stream and does not corrupt plain strings without base64 encoding")
    void shouldPreserveBytesAndPlainTextWhenContentTypeIsOctetStreamWithoutBase64Encoding() {
        String json = "{\"orderId\":\"1\"}";
        OutboxMessage jsonBytesWithOctetStream = new OutboxMessage(
                UUID.randomUUID(),
                "orders-topic",
                "k1",
                json,
                Map.of("__PayloadType__", "bytes", "contentType", "application/octet-stream"),
                OutboxStatus.PENDING,
                0,
                Instant.now(),
                null,
                null,
                null
        );

        Object resolvedBytes = OutboxPoller.resolvePayloadForKafka(jsonBytesWithOctetStream);
        assertThat(resolvedBytes).isInstanceOf(byte[].class);
        assertThat((byte[]) resolvedBytes).isEqualTo(json.getBytes(StandardCharsets.UTF_8));

        OutboxMessage plainTextWithOctetStream = new OutboxMessage(
                UUID.randomUUID(),
                "orders-topic",
                "k2",
                "\"ABCD\"",
                Map.of("contentType", "application/octet-stream", "__ContentType__", "text/plain"),
                OutboxStatus.PENDING,
                0,
                Instant.now(),
                null,
                null,
                null
        );

        Object resolvedText = OutboxPoller.resolvePayloadForKafka(plainTextWithOctetStream);
        assertThat(resolvedText).isEqualTo("ABCD");
    }

    @Test
    @DisplayName("Senior Refinements Round 74: createProducerRecord strips internal storage headers (__PayloadType__, __ContentType__, __Encoding__) from Kafka ProducerRecord")
    void shouldStripInternalOutboxHeadersFromProducerRecord() {
        byte[] rawBinary = new byte[]{10, 20, 30, 40};
        String base64Payload = "\"" + java.util.Base64.getEncoder().encodeToString(rawBinary) + "\"";

        Map<String, String> flatHeaders = Map.of(
                "__PayloadType__", "bytes",
                "__ContentType__", "application/octet-stream",
                "__Encoding__", "base64",
                "contentType", "application/octet-stream",
                "X-Custom-Header", "val-1"
        );
        Map<String, List<String>> multiHeaders = Map.of(
                "__PayloadType__", List.of("bytes"),
                "__ContentType__", List.of("application/octet-stream"),
                "__Encoding__", List.of("base64"),
                "contentType", List.of("application/octet-stream"),
                "X-Custom-Header", List.of("val-1")
        );

        OutboxMessage msg = new OutboxMessage(
                UUID.randomUUID(),
                "binary-topic",
                "k1",
                base64Payload,
                flatHeaders,
                multiHeaders,
                OutboxStatus.IN_PROGRESS,
                0,
                Instant.now(),
                null,
                null,
                null,
                null
        );

        ProducerRecord<?, ?> record = outboxPoller.createProducerRecord(msg);

        assertThat(record.value()).isEqualTo(rawBinary);
        assertThat(record.headers().lastHeader("__PayloadType__")).isNull();
        assertThat(record.headers().lastHeader("__ContentType__")).isNull();
        assertThat(record.headers().lastHeader("__Encoding__")).isNull();
        assertThat(record.headers().lastHeader("contentType")).isNotNull();
        assertThat(record.headers().lastHeader("X-Custom-Header")).isNotNull();
    }

    @Test
    @DisplayName("Senior Refinements Round 75: inProgressLockDuration defaults to 5m and OutboxPoller enforces lockDuration >= max(kafkaDeliveryTimeout, sendTimeout) + 30s")
    void shouldEnforceMinimumSafeInProgressLockDurationAboveKafkaDeliveryTimeout() {
        IdemboxProperties defaultProps = new IdemboxProperties();
        assertThat(defaultProps.getOutbox().getInProgressLockDuration()).isEqualTo(Duration.ofMinutes(5));

        // Configure inProgressLockDuration (60s) smaller than kafkaDeliveryTimeout (120s)
        properties.getOutbox().setInProgressLockDuration(Duration.ofSeconds(60));
        properties.getOutbox().setKafkaDeliveryTimeout(Duration.ofSeconds(120));
        properties.getOutbox().setSendTimeout(Duration.ofSeconds(10));

        when(outboxRepository.claimBatchForProcessing(anyInt(), any(Duration.class), anyInt(), any(Duration.class)))
                .thenReturn(Collections.emptyList());

        outboxPoller.pollAndPublishBatch();

        ArgumentCaptor<Duration> lockCaptor = ArgumentCaptor.captor();
        verify(outboxRepository).claimBatchForProcessing(anyInt(), lockCaptor.capture(), anyInt(), any(Duration.class));
        // Must be clamped to at least 120s + 30s = 150s
        assertThat(lockCaptor.getValue()).isEqualTo(Duration.ofSeconds(150));
    }

    @Test
    @DisplayName("Senior Refinements Round 75: Local future.get TimeoutException does not prematurely record recordOutboxFailed metric while send is still in-flight")
    void shouldNotRecordPrematureOutboxFailedMetricOnLocalFutureTimeout() {
        IdemboxMetrics mockMetrics = org.mockito.Mockito.mock(IdemboxMetrics.class);
        properties.getOutbox().setSendTimeout(Duration.ofMillis(40));
        OutboxPoller testPoller = new OutboxPoller(outboxRepository, kafkaOperations, properties, mockMetrics, null);
        testPoller.start();

        UUID msgId = UUID.randomUUID();
        OutboxMessage msg = new OutboxMessage(
                msgId, "metrics-topic", "k", "{\"v\":1}",
                Collections.emptyMap(), OutboxStatus.IN_PROGRESS, 0,
                Instant.now(), null, null, null
        );

        when(outboxRepository.claimBatchForProcessing(anyInt(), any(Duration.class), anyInt(), any(Duration.class)))
                .thenReturn(List.of(msg))
                .thenReturn(Collections.emptyList());

        CompletableFuture<SendResult<Object, Object>> sendFuture = new CompletableFuture<>();
        when(kafkaOperations.send(anyProducerRecord())).thenReturn(sendFuture);

        int processed = testPoller.pollAndPublishBatch();
        assertThat(processed).isEqualTo(1);

        // While sendFuture is still in-flight after local wait timeout, recordOutboxFailed must NOT have been called!
        verify(mockMetrics, never()).recordOutboxFailed(anyString());

        // When background Kafka send eventually succeeds, only recordOutboxSent is recorded
        RecordMetadata metadata = new RecordMetadata(new TopicPartition("metrics-topic", 0), 0, 0, 0, 0, 0);
        SendResult<Object, Object> sendResult = new SendResult<>(new ProducerRecord<>("metrics-topic", "k", "{}"), metadata);
        sendFuture.complete(sendResult);

        verify(mockMetrics, times(1)).recordOutboxSent(eq("metrics-topic"), any(Duration.class));
        verify(mockMetrics, never()).recordOutboxFailed(anyString());
    }

    @Test
    @DisplayName("Senior Refinements Round 77: Kafka send completing on kafka-producer-network-thread after Step 3 timeout still calls markAsSent when outboxAsyncExecutor is null or rejects task")
    void shouldMarkAsSentWhenKafkaSendCompletesOnKafkaThreadAfterStep3TimeoutWithNullOrRejectedExecutor() throws InterruptedException {
        UUID msgId = UUID.randomUUID();
        OutboxMessage msg = new OutboxMessage(
                msgId, "late-topic", "k", "{\"v\":1}",
                Collections.emptyMap(), OutboxStatus.IN_PROGRESS, 0,
                Instant.now(), null, null, null
        );

        when(outboxRepository.claimBatchForProcessing(anyInt(), any(Duration.class), anyInt(), any(Duration.class)))
                .thenReturn(List.of(msg))
                .thenReturn(Collections.emptyList());

        CompletableFuture<SendResult<Object, Object>> sendFuture = new CompletableFuture<>();
        when(kafkaOperations.send(anyProducerRecord())).thenReturn(sendFuture);

        properties.getOutbox().setSendTimeout(Duration.ofMillis(30));
        OutboxPoller testPoller = new OutboxPoller(outboxRepository, kafkaOperations, properties, IdemboxMetrics.noOp(), null);
        testPoller.start();

        int processed = testPoller.pollAndPublishBatch();
        assertThat(processed).isEqualTo(1);

        // Now complete sendFuture from a simulated Kafka producer network thread AFTER Step 3 has exited
        RecordMetadata metadata = new RecordMetadata(new TopicPartition("late-topic", 0), 0, 0, 0, 0, 0);
        SendResult<Object, Object> sendResult = new SendResult<>(new ProducerRecord<>("late-topic", "k", "{}"), metadata);

        java.util.concurrent.CountDownLatch markedLatch = new java.util.concurrent.CountDownLatch(1);
        org.mockito.Mockito.doAnswer(inv -> {
            markedLatch.countDown();
            return null;
        }).when(outboxRepository).markAsSent(eq(msgId), any(Instant.class));

        Thread kafkaThread = new Thread(() -> sendFuture.complete(sendResult), "kafka-producer-network-thread | producer-1");
        kafkaThread.start();
        kafkaThread.join();

        assertThat(markedLatch.await(2, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
        verify(outboxRepository).markAsSent(eq(msgId), any(Instant.class));
    }

    @Test
    @DisplayName("Senior Refinements Round 77: recordOutboxSent metric is not recorded when markAsSent returns false due to expired lease")
    void shouldNotRecordOutboxSentMetricWhenMarkAsSentReturnsFalse() {
        IdemboxMetrics mockMetrics = org.mockito.Mockito.mock(IdemboxMetrics.class);
        OutboxPoller testPoller = new OutboxPoller(outboxRepository, kafkaOperations, properties, mockMetrics, Runnable::run);
        testPoller.start();

        UUID msgId = UUID.randomUUID();
        Instant deadline = Instant.now().plusSeconds(60);
        OutboxMessage msg = new OutboxMessage(
                msgId, "fenced-topic", "k", "{\"v\":1}",
                Collections.emptyMap(), OutboxStatus.IN_PROGRESS, 0,
                Instant.now(), null, deadline, null
        );

        when(outboxRepository.claimBatchForProcessing(anyInt(), any(Duration.class), anyInt(), any(Duration.class)))
                .thenReturn(List.of(msg))
                .thenReturn(Collections.emptyList());

        RecordMetadata metadata = new RecordMetadata(new TopicPartition("fenced-topic", 0), 0, 0, 0, 0, 0);
        SendResult<Object, Object> sendResult = new SendResult<>(new ProducerRecord<>("fenced-topic", "k", "{}"), metadata);
        when(kafkaOperations.send(anyProducerRecord())).thenReturn(CompletableFuture.completedFuture(sendResult));

        // Simulate expired lease: markAsSent returns false
        when(outboxRepository.markAsSent(eq(msgId), any(Instant.class), eq(deadline))).thenReturn(false);

        int processed = testPoller.pollAndPublishBatch();
        assertThat(processed).isEqualTo(1);

        verify(outboxRepository).markAsSent(eq(msgId), any(Instant.class), eq(deadline));
        verify(mockMetrics, never()).recordOutboxSent(eq("fenced-topic"), any(Duration.class));
    }

    @Test
    @DisplayName("Senior Refinements Round 78: Step 3 finally block retries markAsSent when async markTask failed on a subsequent message before early loop break, and processSingleMessage leaves timed-out message in IN_PROGRESS")
    void shouldRetryFailedAsyncMarkTaskInStep3FinallyAndKeepInProgressOnSingleMessageTimeout() {
        OutboxPoller testPoller = new OutboxPoller(outboxRepository, kafkaOperations, properties, IdemboxMetrics.noOp(), Runnable::run);
        testPoller.start();

        // 1. Step 3 finally fallback when msg2's async markTask failed and msg1 timed out (breaking loop at i=0)
        UUID msg1Id = UUID.randomUUID();
        UUID msg2Id = UUID.randomUUID();
        Instant deadline = Instant.now().plusSeconds(60);
        OutboxMessage msg1 = new OutboxMessage(
                msg1Id, "topic-a", "k1", "{\"v\":1}",
                Collections.emptyMap(), OutboxStatus.IN_PROGRESS, 0,
                Instant.now(), null, deadline, null
        );
        OutboxMessage msg2 = new OutboxMessage(
                msg2Id, "topic-b", "k2", "{\"v\":2}",
                Collections.emptyMap(), OutboxStatus.IN_PROGRESS, 0,
                Instant.now(), null, deadline, null
        );

        properties.getOutbox().setSendTimeout(Duration.ofMillis(30));
        when(outboxRepository.claimBatchForProcessing(anyInt(), any(Duration.class), anyInt(), any(Duration.class)))
                .thenReturn(List.of(msg1, msg2))
                .thenReturn(Collections.emptyList());

        // msg1 never completes before sendTimeout -> Step 3 breaks at i=0
        CompletableFuture<SendResult<Object, Object>> slowFuture1 = new CompletableFuture<>();
        RecordMetadata metadata2 = new RecordMetadata(new TopicPartition("topic-b", 0), 0, 0, 0, 0, 0);
        SendResult<Object, Object> sendResult2 = new SendResult<>(new ProducerRecord<>("topic-b", "k2", "{}"), metadata2);
        CompletableFuture<SendResult<Object, Object>> fastFuture2 = CompletableFuture.completedFuture(sendResult2);

        when(kafkaOperations.send(anyProducerRecord()))
                .thenReturn(slowFuture1)
                .thenReturn(fastFuture2);

        // First call to markAsSent for msg2 (from async markTask) throws transient DB error;
        // second call (from Step 3 finally block) succeeds!
        when(outboxRepository.markAsSent(eq(msg2Id), any(Instant.class), eq(deadline)))
                .thenThrow(new org.springframework.dao.DataAccessResourceFailureException("Transient DB glitch"))
                .thenReturn(true);

        testPoller.pollAndPublishBatch();

        // msg1 was left in IN_PROGRESS (never rescheduled to PENDING)
        verify(outboxRepository, never()).rescheduleMessage(eq(msg1Id), anyString(), any(Duration.class), any());
        // msg2 was retried in Step 3 finally block and marked SENT!
        verify(outboxRepository, org.mockito.Mockito.times(2)).markAsSent(eq(msg2Id), any(Instant.class), eq(deadline));

        // 2. processSingleMessage timeout leaves message in IN_PROGRESS and marks SENT when sendFuture later completes
        UUID singleId = UUID.randomUUID();
        OutboxMessage singleMsg = new OutboxMessage(
                singleId, "single-topic", "k3", "{\"v\":3}",
                Collections.emptyMap(), OutboxStatus.IN_PROGRESS, 0,
                Instant.now(), null, deadline, null
        );
        CompletableFuture<SendResult<Object, Object>> delayedSingleFuture = new CompletableFuture<>();
        when(kafkaOperations.send(anyProducerRecord())).thenReturn(delayedSingleFuture);
        when(outboxRepository.markAsSent(eq(singleId), any(Instant.class), eq(deadline))).thenReturn(true);

        OutboxPoller.DeliveryStatus singleStatus = testPoller.processSingleMessage(singleMsg);
        assertThat(singleStatus).isEqualTo(OutboxPoller.DeliveryStatus.RECOVERABLE_BROKER_OUTAGE);
        verify(outboxRepository, never()).rescheduleMessage(eq(singleId), anyString(), any(Duration.class), any());

        // Now complete the in-flight Kafka future -> callback marks singleMsg as SENT
        delayedSingleFuture.complete(sendResult2);
        verify(outboxRepository).markAsSent(eq(singleId), any(Instant.class), eq(deadline));
    }

    @Test
    @DisplayName("Issue 1: pollAndPublishBatch increments batchProcessed when transactional KafkaTemplate encounters MESSAGE_ERROR")
    void shouldCountBatchProcessedOnMessageErrorInTransactionalKafkaTemplate() {
        org.springframework.kafka.core.KafkaTemplate<Object, Object> txKafkaTemplate =
                mock(ObjectKafkaTemplate.class);
        when(txKafkaTemplate.isTransactional()).thenReturn(true);

        OutboxPoller txPoller = new OutboxPoller(outboxRepository, txKafkaTemplate, properties, IdemboxMetrics.noOp(), Runnable::run);
        txPoller.start();

        UUID msgId = UUID.randomUUID();
        OutboxMessage msg = new OutboxMessage(
                msgId, "orders-topic", "k1", "{\"order\":\"data\"}",
                Collections.emptyMap(), OutboxStatus.IN_PROGRESS, 0,
                Instant.now(), null, Instant.now().plusSeconds(60), null
        );

        when(outboxRepository.claimBatchForProcessing(anyInt(), any(Duration.class), anyInt(), any(Duration.class)))
                .thenReturn(List.of(msg))
                .thenReturn(Collections.emptyList());

        when(txKafkaTemplate.executeInTransaction(any())).thenThrow(
                new org.apache.kafka.common.errors.RecordTooLargeException("Fatal oversized record")
        );

        int processed = txPoller.pollAndPublishBatch();
        assertThat(processed).isEqualTo(1);
        verify(outboxRepository).markAsFailed(eq(msgId), anyString(), anyInt(), any(Duration.class), any());
    }

    @Test
    @DisplayName("Issue 3: OutboxPoller records lease stolen metric and invokes forceMarkAsSent when lease expires")
    void shouldRecordLeaseStolenMetricAndForceMarkAsSentWhenLeaseExpires() {
        IdemboxMetrics mockMetrics = org.mockito.Mockito.mock(IdemboxMetrics.class);
        OutboxPoller testPoller = new OutboxPoller(outboxRepository, kafkaOperations, properties, mockMetrics, Runnable::run);
        testPoller.start();

        UUID msgId = UUID.randomUUID();
        Instant deadline = Instant.now().plusSeconds(60);
        OutboxMessage msg = new OutboxMessage(
                msgId, "events-topic", "k1", "{\"status\":\"ok\"}",
                Collections.emptyMap(), OutboxStatus.IN_PROGRESS, 0,
                Instant.now(), null, deadline, null
        );

        when(outboxRepository.claimBatchForProcessing(anyInt(), any(Duration.class), anyInt(), any(Duration.class)))
                .thenReturn(List.of(msg))
                .thenReturn(Collections.emptyList());

        RecordMetadata metadata = new RecordMetadata(new TopicPartition("events-topic", 0), 0, 0, 0, 0, 0);
        SendResult<Object, Object> sendResult = new SendResult<>(new ProducerRecord<>("events-topic", "k1", "{}"), metadata);
        when(kafkaOperations.send(anyProducerRecord())).thenReturn(CompletableFuture.completedFuture(sendResult));

        // Lease expired / stolen -> markAsSent with deadline returns false
        when(outboxRepository.markAsSent(eq(msgId), any(Instant.class), eq(deadline))).thenReturn(false);

        int processed = testPoller.pollAndPublishBatch();
        assertThat(processed).isEqualTo(1);

        verify(mockMetrics).recordOutboxLeaseStolen("events-topic");
        verify(outboxRepository).forceMarkAsSent(eq(msgId), any(Instant.class));
    }
}
