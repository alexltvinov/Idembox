package io.github.alexlitvinov.sample.config;

import io.github.alexlitvinov.idembox.core.repository.IdempotentRepository;
import io.github.alexlitvinov.sample.event.OrderCreatedEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class KafkaConsumerConfigTest {

    private KafkaConsumerConfig config;
    private KafkaOperations<Object, Object> kafkaOperations;
    private IdempotentRepository repository;
    private ObjectProvider<IdempotentRepository> repositoryProvider;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        config = new KafkaConsumerConfig();
        kafkaOperations = mock(KafkaOperations.class);
        repository = mock(IdempotentRepository.class);
        repositoryProvider = mock(ObjectProvider.class);
        when(repositoryProvider.getIfAvailable()).thenReturn(repository);
        when(kafkaOperations.send(any(org.apache.kafka.clients.producer.ProducerRecord.class)))
                .thenReturn(java.util.concurrent.CompletableFuture.completedFuture(mock(org.springframework.kafka.support.SendResult.class)));
        org.springframework.kafka.support.KafkaUtils.setConsumerGroupId("payment-service-group");
    }

    @org.junit.jupiter.api.AfterEach
    void tearDown() {
        org.springframework.kafka.support.KafkaUtils.clearConsumerGroupId();
    }

    @Test
    @DisplayName("Senior Refinements v21: DeadLetterPublishingRecoverer extracts key from header X-Idempotency-Key")
    void shouldExtractKeyFromHeader() {
        DeadLetterPublishingRecoverer recoverer = config.createRecoverer(kafkaOperations, repositoryProvider);
        assertThat(recoverer).isNotNull();

        RecordHeaders headers = new RecordHeaders();
        headers.add("X-Idempotency-Key", "ORDER-HDR-001".getBytes(StandardCharsets.UTF_8));
        ConsumerRecord<Object, Object> record = new ConsumerRecord<>("order-events-topic", 0, 100L, "PART-KEY-999", "{}");
        headers.forEach(h -> record.headers().add(h));

        recoverer.accept(record, new RuntimeException("Kafka processing failed"));

        verify(repository).markTerminalFailed(eq("ORDER-HDR-001"), eq("payment-service-group"), anyString(), eq(Duration.ofDays(7)));
    }

    @Test
    @DisplayName("Senior Refinements v21: DeadLetterPublishingRecoverer extracts orderId from JSON string payload")
    void shouldExtractOrderIdFromJsonPayload() {
        DeadLetterPublishingRecoverer recoverer = config.createRecoverer(kafkaOperations, repositoryProvider);
        assertThat(recoverer).isNotNull();

        String jsonPayload = "{\"orderId\":\"ORDER-JSON-002\",\"amount\":150.00,\"currency\":\"USD\"}";
        ConsumerRecord<Object, Object> record = new ConsumerRecord<>("order-events-topic", 0, 101L, "PART-KEY-999", jsonPayload);

        recoverer.accept(record, new RuntimeException("Validation error"));

        verify(repository).markTerminalFailed(eq("ORDER-JSON-002"), eq("payment-service-group"), anyString(), eq(Duration.ofDays(7)));
    }

    @Test
    @DisplayName("Senior Refinements v21: DeadLetterPublishingRecoverer extracts orderId from POJO payload")
    void shouldExtractOrderIdFromPojoPayload() {
        DeadLetterPublishingRecoverer recoverer = config.createRecoverer(kafkaOperations, repositoryProvider);
        assertThat(recoverer).isNotNull();

        OrderCreatedEvent event = new OrderCreatedEvent("ORDER-POJO-003", "CUST-1", BigDecimal.valueOf(99.99), "EUR");
        ConsumerRecord<Object, Object> record = new ConsumerRecord<>("order-events-topic", 0, 102L, "PART-KEY-999", event);

        recoverer.accept(record, new RuntimeException("Service error"));

        verify(repository).markTerminalFailed(eq("ORDER-POJO-003"), eq("payment-service-group"), anyString(), eq(Duration.ofDays(7)));
    }

    @Test
    @DisplayName("Senior Refinements v21 & Round 75: DeadLetterPublishingRecoverer falls back to record.key() only for true tombstone records (value == null)")
    void shouldFallbackToRecordKey() {
        DeadLetterPublishingRecoverer recoverer = config.createRecoverer(kafkaOperations, repositoryProvider);
        assertThat(recoverer).isNotNull();

        ConsumerRecord<Object, Object> tombstoneRecord = new ConsumerRecord<>("order-events-topic", 0, 103L, "FALLBACK-KEY-004", null);

        recoverer.accept(tombstoneRecord, new RuntimeException("Unexpected error"));

        verify(repository).markTerminalFailed(eq("FALLBACK-KEY-004"), eq("payment-service-group"), anyString(), eq(Duration.ofDays(7)));
    }

    @Test
    @DisplayName("commonErrorHandler should be instantiated as DefaultErrorHandler with non-retryable exceptions")
    void shouldCreateCommonErrorHandlerWithNonRetryableExceptions() throws Exception {
        org.springframework.kafka.listener.CommonErrorHandler handler = config.commonErrorHandler(kafkaOperations, repositoryProvider);

        assertThat(handler).isNotNull();
        assertThat(handler).isInstanceOf(org.springframework.kafka.listener.DefaultErrorHandler.class);

        java.lang.reflect.Method getClassifierMethod = org.springframework.kafka.listener.ExceptionClassifier.class.getDeclaredMethod("getClassifier");
        getClassifierMethod.setAccessible(true);
        org.springframework.classify.BinaryExceptionClassifier classifier =
                (org.springframework.classify.BinaryExceptionClassifier) getClassifierMethod.invoke(handler);

        assertThat(classifier.classify(new jakarta.validation.ValidationException("Invalid input"))).isFalse();
        assertThat(classifier.classify(new IllegalArgumentException("Illegal state"))).isFalse();
        assertThat(classifier.classify(new org.springframework.expression.EvaluationException("SpEL error"))).isFalse();
        assertThat(classifier.classify(new IllegalStateException("Illegal state"))).isFalse();
        assertThat(classifier.classify(new io.github.alexlitvinov.idembox.core.model.DuplicateMessageException("KEY-1", "GRP-1"))).isFalse();
        assertThat(classifier.classify(new RuntimeException("Transient failure"))).isTrue();
    }

    @Test
    @DisplayName("Senior Refinements Round 73: extractConsumerGroup ignores untrusted network headers and uses topic conventions")
    void shouldExtractConsumerGroupFromHeadersAndTopic() {
        org.springframework.kafka.support.KafkaUtils.clearConsumerGroupId();
        try {
            // Untrusted network header on ConsumerRecord must be ignored
            RecordHeaders headers = new RecordHeaders();
            headers.add("kafka_groupId", "spoofed-header-group".getBytes(StandardCharsets.UTF_8));
            headers.add("X-Consumer-Group", "spoofed-header-group-2".getBytes(StandardCharsets.UTF_8));
            ConsumerRecord<Object, Object> recordWithHeader = new ConsumerRecord<>("billing-topic", 0, 107L, "KEY", "{}");
            headers.forEach(h -> recordWithHeader.headers().add(h));
            assertThat(config.extractConsumerGroup(recordWithHeader)).isEqualTo("billing-topic-group");

            // Topic starting with order-
            ConsumerRecord<Object, Object> orderRecord = new ConsumerRecord<>("order-events-topic", 0, 108L, "KEY", "{}");
            assertThat(config.extractConsumerGroup(orderRecord)).isEqualTo("order-events-topic-group");

            // Other topic
            ConsumerRecord<Object, Object> otherRecord = new ConsumerRecord<>("inventory-events", 0, 109L, "KEY", "{}");
            assertThat(config.extractConsumerGroup(otherRecord)).isEqualTo("inventory-events-group");
        } finally {
            org.springframework.kafka.support.KafkaUtils.setConsumerGroupId("payment-service-group");
        }
    }

    @Test
    @DisplayName("Senior Refinements v29: extractConsumerGroup prioritizes KafkaUtils.getConsumerGroupId() over topic heuristics")
    void shouldPrioritizeKafkaUtilsConsumerGroupId() {
        try {
            org.springframework.kafka.support.KafkaUtils.setConsumerGroupId("custom-active-container-group");
            ConsumerRecord<Object, Object> record = new ConsumerRecord<>("order-events-topic", 0, 110L, "KEY", "{}");

            assertThat(config.extractConsumerGroup(record)).isEqualTo("custom-active-container-group");
        } finally {
            org.springframework.kafka.support.KafkaUtils.clearConsumerGroupId();
        }
    }

    @Test
    @DisplayName("extractEventKey should extract id from byte[] JSON payload")
    void shouldExtractIdFromByteArrayJsonPayload() {
        byte[] jsonBytes = "{\"id\":\"BYTE-ID-999\",\"status\":\"NEW\"}".getBytes(StandardCharsets.UTF_8);
        ConsumerRecord<Object, Object> record = new ConsumerRecord<>("order-events-topic", 0, 104L, "KEY-999", jsonBytes);

        String extracted = config.extractEventKey(record);

        assertThat(extracted).isEqualTo("BYTE-ID-999");
    }

    @Test
    @DisplayName("extractEventKey should prioritize X-Idempotency-Key header over idempotencyKey and orderId")
    void shouldRespectHeaderPrecedence() {
        RecordHeaders headers = new RecordHeaders();
        headers.add("orderId", "LOWEST-PRIORITY".getBytes(StandardCharsets.UTF_8));
        headers.add("idempotencyKey", "MEDIUM-PRIORITY".getBytes(StandardCharsets.UTF_8));
        headers.add("X-Idempotency-Key", "HIGHEST-PRIORITY".getBytes(StandardCharsets.UTF_8));

        ConsumerRecord<Object, Object> record = new ConsumerRecord<>("order-events-topic", 0, 105L, "KEY-1", "{}");
        headers.forEach(h -> record.headers().add(h));

        String extracted = config.extractEventKey(record);

        assertThat(extracted).isEqualTo("HIGHEST-PRIORITY");
    }

    @Test
    @DisplayName("extractEventKey should return null when payload and key are null")
    void shouldReturnNullWhenPayloadAndKeyAreNull() {
        ConsumerRecord<Object, Object> record = new ConsumerRecord<>("order-events-topic", 0, 106L, null, null);

        String extracted = config.extractEventKey(record);

        assertThat(extracted).isNull();
    }

    @Test
    @DisplayName("Senior Refinements v40: prepareReplayRecord resets failed state and sets X-Idempotent-Replay header")
    void prepareReplayRecordShouldResetFailedAndSetHeader() {
        IdempotentRepository mockRepo = mock(IdempotentRepository.class);
        ConsumerRecord<Object, Object> dltRecord = new ConsumerRecord<>("order-events.DLT", 0, 200L, "ORDER-99", "{\"orderId\":\"ORDER-99\"}");

        org.apache.kafka.clients.producer.ProducerRecord<Object, Object> replayRecord = config.prepareReplayRecord(dltRecord, "order-events", mockRepo);

        assertThat(replayRecord.topic()).isEqualTo("order-events");
        assertThat(replayRecord.key()).isEqualTo("ORDER-99");
        assertThat(replayRecord.value()).isEqualTo("{\"orderId\":\"ORDER-99\"}");

        org.apache.kafka.common.header.Header replayHeader = replayRecord.headers().lastHeader("X-Idempotent-Replay");
        assertThat(replayHeader).isNotNull();
        assertThat(new String(replayHeader.value(), StandardCharsets.UTF_8)).isEqualTo("true");

        verify(mockRepo).resetFailed(eq("ORDER-99"), anyString());
    }

    @Test
    @DisplayName("Senior Refinements v42: DeadLetterPublishingRecoverer skips DLT publication and terminal failure for DuplicateMessageException")
    void shouldSkipDltPublicationAndTerminalFailureForDuplicateMessageException() {
        DeadLetterPublishingRecoverer recoverer = config.createRecoverer(kafkaOperations, repositoryProvider);
        assertThat(recoverer).isNotNull();

        ConsumerRecord<Object, Object> record = new ConsumerRecord<>("order-events-topic", 0, 107L, "DUP-KEY-001", "{\"orderId\":\"DUP-KEY-001\"}");
        io.github.alexlitvinov.idembox.core.model.DuplicateMessageException dupEx =
                new io.github.alexlitvinov.idembox.core.model.DuplicateMessageException("DUP-KEY-001", "payment-service-group");

        recoverer.accept(record, dupEx);

        verify(repository, never()).markTerminalFailed(any(), any(), any(), any());
        verify(kafkaOperations, never()).send(any(org.apache.kafka.clients.producer.ProducerRecord.class));
    }

    @Test
    @DisplayName("Senior Refinements v48 & Round 75: extractEventKey formats byte[] key as hex string on tombstone record avoiding identity hash code")
    void shouldExtractHexKeyFromByteArrayKey() {
        byte[] keyBytes = new byte[]{0x0a, 0x1b, (byte) 0xff};
        ConsumerRecord<Object, Object> tombstoneRecord = new ConsumerRecord<>("order-events-topic", 0, 111L, keyBytes, null);

        String extracted = config.extractEventKey(tombstoneRecord);

        assertThat(extracted).isEqualTo("0a1bff");

        // Non-tombstone record lacking orderId falls back to record.key() (Issue 4 resolved)
        OrderCreatedEvent eventWithoutOrderId = new OrderCreatedEvent(null, "CUST-42", BigDecimal.TEN, "RUB");
        ConsumerRecord<Object, Object> nonTombstoneRecord =
                new ConsumerRecord<>("order-events-topic", 0, 112L, "CUST-42", eventWithoutOrderId);
        assertThat(config.extractEventKey(nonTombstoneRecord)).isEqualTo("CUST-42");
    }

    @Test
    @DisplayName("Senior Refinements v49: extractConsumerGroup strips .DLT and .dlt suffix before constructing group name")
    void shouldExtractConsumerGroupStrippingDltSuffixFromNonOrderTopic() {
        org.springframework.kafka.support.KafkaUtils.clearConsumerGroupId();
        try {
            // Uppercase .DLT
            ConsumerRecord<Object, Object> recordDlt = new ConsumerRecord<>("inventory-events.DLT", 0, 112L, "K1", "{}");
            assertThat(config.extractConsumerGroup(recordDlt)).isEqualTo("inventory-events-group");

            // Lowercase .dlt
            ConsumerRecord<Object, Object> recordLowerDlt = new ConsumerRecord<>("warehouse-sync.dlt", 0, 113L, "K2", "{}");
            assertThat(config.extractConsumerGroup(recordLowerDlt)).isEqualTo("warehouse-sync-group");

            // Order topic with .DLT
            ConsumerRecord<Object, Object> recordOrderDlt = new ConsumerRecord<>("order-events.DLT", 0, 114L, "K3", "{}");
            assertThat(config.extractConsumerGroup(recordOrderDlt)).isEqualTo("order-events-group");
        } finally {
            org.springframework.kafka.support.KafkaUtils.setConsumerGroupId("payment-service-group");
        }
    }

    @Test
    @DisplayName("Senior Refinements v49: prepareReplayRecord resets failed state for actual consumer group and stripped topic")
    void shouldPrepareReplayRecordWithCorrectGroupAndTargetTopicForNonOrderDltTopic() {
        IdempotentRepository mockRepo = mock(IdempotentRepository.class);
        ConsumerRecord<Object, Object> dltRecord = new ConsumerRecord<>(
                "inventory-events.DLT", 0, 300L, "INV-100", "{\"id\":\"INV-100\"}");
        dltRecord.headers().add("kafka_dlt-original-consumer-group", "inventory-events-group".getBytes(StandardCharsets.UTF_8));

        org.apache.kafka.clients.producer.ProducerRecord<Object, Object> replayRecord =
                config.prepareReplayRecord(dltRecord, null, mockRepo);

        assertThat(replayRecord.topic()).isEqualTo("inventory-events");
        assertThat(replayRecord.key()).isEqualTo("INV-100");
        verify(mockRepo).resetFailed(eq("INV-100"), eq("inventory-events-group"));
    }

    @Test
    @DisplayName("Senior Refinements Round 53 & Round 71: DeadLetterPublishingRecoverer skips terminal failure marking but forwards to .DLT for ConcurrentProcessingException")
    @SuppressWarnings("unchecked")
    void shouldSkipTerminalFailureAndForwardToDltForConcurrentProcessingException() {
        when(kafkaOperations.send(any(org.apache.kafka.clients.producer.ProducerRecord.class)))
                .thenReturn(java.util.concurrent.CompletableFuture.completedFuture(null));

        DeadLetterPublishingRecoverer recoverer = config.createRecoverer(kafkaOperations, repositoryProvider);
        assertThat(recoverer).isNotNull();

        ConsumerRecord<Object, Object> record = new ConsumerRecord<>("order-events-topic", 0, 108L, "CONC-KEY-001", "{\"orderId\":\"CONC-KEY-001\"}");
        io.github.alexlitvinov.idembox.core.model.ConcurrentProcessingException concEx =
                new io.github.alexlitvinov.idembox.core.model.ConcurrentProcessingException("CONC-KEY-001", "payment-service-group");

        recoverer.accept(record, concEx);

        verify(repository, never()).markTerminalFailed(any(), any(), any(), any());
        org.mockito.ArgumentCaptor<org.apache.kafka.clients.producer.ProducerRecord<Object, Object>> captor =
                org.mockito.ArgumentCaptor.forClass(org.apache.kafka.clients.producer.ProducerRecord.class);
        verify(kafkaOperations, times(1)).send(captor.capture());
        assertThat(captor.getValue().topic()).isEqualTo("order-events-topic.DLT");

        // Also test wrapped exception (e.g. inside ListenerExecutionFailedException)
        RuntimeException wrappedEx = new RuntimeException("Listener error", concEx);
        recoverer.accept(record, wrappedEx);

        verify(repository, never()).markTerminalFailed(any(), any(), any(), any());
        verify(kafkaOperations, times(2)).send(captor.capture());
        assertThat(captor.getValue().topic()).isEqualTo("order-events-topic.DLT");
    }

    @Test
    @DisplayName("Senior Refinements Round 58: prepareReplayRecord filters out kafka_dlt-* and duplicate X-Idempotent-Replay headers")
    void shouldFilterDltAndPreviousReplayHeadersOnReplay() {
        ConsumerRecord<Object, Object> dltRecord = new ConsumerRecord<>(
                "orders.DLT", 0, 500L, "ORD-REPLAY-1", "{\"orderId\":\"ORD-REPLAY-1\"}");
        dltRecord.headers().add("kafka_dlt-exception-stacktrace", "trace...".getBytes(StandardCharsets.UTF_8));
        dltRecord.headers().add("kafka_dlt-original-topic", "orders".getBytes(StandardCharsets.UTF_8));
        dltRecord.headers().add("X-Idempotent-Replay", "true".getBytes(StandardCharsets.UTF_8));
        dltRecord.headers().add("X-Correlation-ID", "corr-123".getBytes(StandardCharsets.UTF_8));

        org.apache.kafka.clients.producer.ProducerRecord<Object, Object> replayRecord =
                config.prepareReplayRecord(dltRecord, "orders", repository);

        java.util.List<String> headerKeys = new java.util.ArrayList<>();
        replayRecord.headers().forEach(h -> headerKeys.add(h.key()));

        assertThat(headerKeys).contains("X-Correlation-ID", "X-Idempotent-Replay");
        assertThat(headerKeys).doesNotContain("kafka_dlt-exception-stacktrace", "kafka_dlt-original-topic");

        long replayHeaderCount = headerKeys.stream().filter(k -> k.equalsIgnoreCase("X-Idempotent-Replay")).count();
        assertThat(replayHeaderCount).isEqualTo(1);
    }

    @Test
    @DisplayName("Senior Refinements Round 60: stripDltSuffix removes single and multiple chained DLT suffixes")
    void stripDltSuffixShouldStripSingleAndRepeatedDltSuffixes() {
        assertThat(KafkaConsumerConfig.stripDltSuffix(null)).isNull();
        assertThat(KafkaConsumerConfig.stripDltSuffix("orders")).isEqualTo("orders");
        assertThat(KafkaConsumerConfig.stripDltSuffix("orders.DLT")).isEqualTo("orders");
        assertThat(KafkaConsumerConfig.stripDltSuffix("orders.dlt")).isEqualTo("orders");
        assertThat(KafkaConsumerConfig.stripDltSuffix("orders.DLT.DLT")).isEqualTo("orders");
        assertThat(KafkaConsumerConfig.stripDltSuffix("orders.DLT.dlt.DLT")).isEqualTo("orders");
    }

    @Test
    @DisplayName("Senior Refinements Round 60: DeadLetterPublishingRecoverer prevents recursive topic multiplication when consuming from DLT")
    void shouldNotCreateChainedDltTopicsWhenFailingOnDltTopic() {
        DeadLetterPublishingRecoverer recoverer = config.createRecoverer(kafkaOperations, repositoryProvider);
        assertThat(recoverer).isNotNull();

        ConsumerRecord<Object, Object> dltRecord =
                new ConsumerRecord<>("order-events-topic.DLT", 0, 109L, "KEY-DLT", "{\"orderId\":\"ORD-DLT\"}");

        recoverer.accept(dltRecord, new RuntimeException("Failed in DLT consumer"));

        org.mockito.ArgumentCaptor<org.apache.kafka.clients.producer.ProducerRecord<Object, Object>> recordCaptor =
                org.mockito.ArgumentCaptor.forClass(org.apache.kafka.clients.producer.ProducerRecord.class);
        verify(kafkaOperations).send(recordCaptor.capture());

        assertThat(recordCaptor.getValue().topic()).isEqualTo("order-events-topic.DLT");
    }

    @Test
    @DisplayName("Senior Refinements Round 73 & Round 74: extractEventKey prevents Header Shadowing and handles ErrorHandlingDeserializer safely")
    void shouldPreventHeaderShadowingAndExtractKeyFromDeserializationException() {
        // 1. Header Shadowing prevention: payload orderId wins over untrusted X-Idempotency-Key header
        ConsumerRecord<Object, Object> shadowedRecord =
                new ConsumerRecord<>("order-events-topic", 0, 200L, "customer-42", "{\"orderId\":\"ORD-REAL-100\",\"amount\":10.0}");
        shadowedRecord.headers().add("X-Idempotency-Key", "ORD-VICTIM-999".getBytes(StandardCharsets.UTF_8));
        assertThat(config.extractEventKey(shadowedRecord)).isEqualTo("ORD-REAL-100");

        // 2. ErrorHandlingDeserializer with parseable JSON in DeserializationException.getData()
        byte[] rawJsonBytes = "{\"orderId\":\"ORD-DESER-200\",\"amount\":\"NOT_A_NUMBER\"}".getBytes(StandardCharsets.UTF_8);
        org.springframework.kafka.support.serializer.DeserializationException deserEx =
                new org.springframework.kafka.support.serializer.DeserializationException("Failed to deserialize", rawJsonBytes, false, new RuntimeException("Bad number"));
        ConsumerRecord<Object, Object> deserRecord =
                new ConsumerRecord<>("order-events-topic", 0, 201L, "customer-42", null);
        assertThat(config.extractEventKey(deserRecord, deserEx)).isEqualTo("ORD-DESER-200");

        // 3. ErrorHandlingDeserializer with truncated/corrupted JSON bytes falls back to explicit idempotency header if present,
        //    and returns null (never partition key customer-42) when no idempotency header is present
        byte[] corruptedBytes = "{\"orderId\":\"ORD-123\", \"amount\":".getBytes(StandardCharsets.UTF_8);
        org.springframework.kafka.support.serializer.DeserializationException corruptedDeserEx =
                new org.springframework.kafka.support.serializer.DeserializationException("Malformed JSON", corruptedBytes, false, new RuntimeException("EOF"));
        ConsumerRecord<Object, Object> corruptedRecordWithHeader =
                new ConsumerRecord<>("order-events-topic", 0, 202L, "customer-42", null);
        corruptedRecordWithHeader.headers().add("X-Idempotency-Key", "ORD-HDR-300".getBytes(StandardCharsets.UTF_8));
        assertThat(config.extractEventKey(corruptedRecordWithHeader, corruptedDeserEx)).isEqualTo("ORD-HDR-300");

        ConsumerRecord<Object, Object> corruptedRecordWithoutHeader =
                new ConsumerRecord<>("order-events-topic", 0, 203L, "customer-42", null);
        assertThat(config.extractEventKey(corruptedRecordWithoutHeader, corruptedDeserEx)).isNull();
    }

    @Test
    @DisplayName("Senior Refinements Round 74: extractFailedDeserializationBytes and prepareReplayRecord safely deserialize Java ObjectOutputStream header without brace-scanning corruption")
    void shouldDeserializeExceptionHeaderStreamWithoutCorruptingPayloadWhenMessageContainsBraces() throws Exception {
        byte[] originalPayload = "{\"orderId\":\"ORD-STREAM-777\",\"amount\":\"invalid\"}".getBytes(StandardCharsets.UTF_8);
        // Exception message intentionally contains '{' and '}' before and after payload bytes in ObjectOutputStream
        org.springframework.kafka.support.serializer.DeserializationException deserEx =
                new org.springframework.kafka.support.serializer.DeserializationException(
                        "Cannot deserialize value from Object value (token '{' to '}')",
                        originalPayload,
                        false,
                        new IllegalArgumentException("Trailing brace } in cause"));

        java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
        try (java.io.ObjectOutputStream oos = new java.io.ObjectOutputStream(baos)) {
            oos.writeObject(deserEx);
        }
        byte[] serializedHeaderBytes = baos.toByteArray();

        ConsumerRecord<Object, Object> dltRecord =
                new ConsumerRecord<>("order-events-topic.DLT", 0, 300L, "customer-42", null);
        dltRecord.headers().add("springDeserializerExceptionValue", serializedHeaderBytes);

        // 1. Exact byte[] recovery without corruption from '{'/'}' inside exception message or JVM stream metadata
        byte[] extractedBytes = config.extractFailedDeserializationBytes(dltRecord, null);
        assertThat(extractedBytes).isEqualTo(originalPayload);

        // 2. Key extraction from serialized header
        assertThat(config.extractEventKey(dltRecord)).isEqualTo("ORD-STREAM-777");

        // 3. prepareReplayRecord restores exact payload bytes and strips springDeserializerExceptionValue header
        org.apache.kafka.clients.producer.ProducerRecord<Object, Object> replayRecord =
                config.prepareReplayRecord(dltRecord, "order-events-topic", repository);
        assertThat(replayRecord.value()).isEqualTo(originalPayload);
        assertThat(replayRecord.headers().lastHeader("springDeserializerExceptionValue")).isNull();
        verify(repository).resetFailed("ORD-STREAM-777", "payment-service-group");
    }

    @Test
    @DisplayName("Senior Refinements Round 76: prepareReplayRecord strips corrupt __TypeId__ header when DLT record failed deserialization")
    void shouldStripCorruptTypeIdHeaderWhenReplayingDeserializationFailedDltRecord() {
        byte[] validJsonPayload = "{\"orderId\":\"ORD-TYPEID-888\",\"amount\":50.00}".getBytes(StandardCharsets.UTF_8);
        ConsumerRecord<Object, Object> dltRecord =
                new ConsumerRecord<>("order-events-topic.DLT", 0, 400L, "customer-42", validJsonPayload);
        dltRecord.headers().add("kafka_dlt-exception-fqcn", org.springframework.kafka.support.serializer.DeserializationException.class.getName().getBytes(StandardCharsets.UTF_8));
        dltRecord.headers().add("__TypeId__", "com.unknown.ExternalOrderEvent".getBytes(StandardCharsets.UTF_8));
        dltRecord.headers().add("X-Correlation-Id", "corr-123".getBytes(StandardCharsets.UTF_8));

        org.apache.kafka.clients.producer.ProducerRecord<Object, Object> replayRecord =
                config.prepareReplayRecord(dltRecord, "order-events-topic", repository);

        // Corrupt __TypeId__ must be stripped on deserialization failure so consumer falls back to default type
        assertThat(replayRecord.headers().lastHeader("__TypeId__")).isNull();
        // Business headers are preserved
        assertThat(replayRecord.headers().lastHeader("X-Correlation-Id")).isNotNull();
    }

    @Test
    @DisplayName("Senior Refinements Round 77: prepareReplayRecord uses kafka_dlt-original-consumer-group header and recovers key from springDeserializerExceptionKey while preserving __TypeId__")
    void shouldUseOriginalConsumerGroupHeaderAndRecoverKeyOnKeyDeserializationFailure() throws Exception {
        byte[] rawKeyBytes = "corrupt-key-bytes".getBytes(StandardCharsets.UTF_8);
        org.springframework.kafka.support.serializer.DeserializationException keyDeserEx =
                new org.springframework.kafka.support.serializer.DeserializationException(
                        "Failed to deserialize key", rawKeyBytes, true, new RuntimeException("Bad key"));
        java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
        try (java.io.ObjectOutputStream oos = new java.io.ObjectOutputStream(baos)) {
            oos.writeObject(keyDeserEx);
        }

        ConsumerRecord<Object, Object> dltRecord =
                new ConsumerRecord<>("order-events-topic.DLT", 0, 500L, null, "{\"orderId\":\"ORD-KEY-FAIL-900\",\"amount\":15.0}");
        dltRecord.headers().add("kafka_dlt-original-consumer-group", "custom-orders-consumer-group".getBytes(StandardCharsets.UTF_8));
        dltRecord.headers().add("springDeserializerExceptionKey", baos.toByteArray());
        dltRecord.headers().add("__TypeId__", "io.github.alexlitvinov.sample.event.OrderCreatedEvent".getBytes(StandardCharsets.UTF_8));
        dltRecord.headers().add("__KeyTypeId__", "com.unknown.BadKeyType".getBytes(StandardCharsets.UTF_8));

        org.apache.kafka.clients.producer.ProducerRecord<Object, Object> replayRecord =
                config.prepareReplayRecord(dltRecord, "order-events-topic", repository);

        // 1. Resets FAILED status for the original consumer group from kafka_dlt-original-consumer-group header
        verify(repository).resetFailed("ORD-KEY-FAIL-900", "custom-orders-consumer-group");
        // 2. Recovers raw key bytes from springDeserializerExceptionKey
        assertThat(replayRecord.key()).isEqualTo(rawKeyBytes);
        // 3. Preserves valid value __TypeId__ while stripping corrupt __KeyTypeId__
        assertThat(replayRecord.headers().lastHeader("__TypeId__")).isNotNull();
        assertThat(replayRecord.headers().lastHeader("__KeyTypeId__")).isNull();
    }

    @Test
    @DisplayName("Senior Refinements Round 78: prepareReplayRecord classifies kafka_dlt-key-exception-fqcn as key deserialization failure and preserves value __TypeId__")
    void shouldClassifyDltKeyExceptionFqcnAsKeyFailureAndPreserveValueTypeId() {
        ConsumerRecord<Object, Object> dltRecord =
                new ConsumerRecord<>("order-events-topic.DLT", 0, 600L, "raw-key".getBytes(StandardCharsets.UTF_8), "{\"orderId\":\"ORD-KEY-FQCN-901\",\"amount\":25.0}");
        // Simulate DeadLetterPublishingRecoverer headers when springDeserializerExceptionKey was already stripped before DLT
        dltRecord.headers().add("kafka_dlt-exception-fqcn", "org.springframework.kafka.listener.ListenerExecutionFailedException".getBytes(StandardCharsets.UTF_8));
        dltRecord.headers().add("kafka_dlt-exception-cause-fqcn", org.springframework.kafka.support.serializer.DeserializationException.class.getName().getBytes(StandardCharsets.UTF_8));
        dltRecord.headers().add("kafka_dlt-key-exception-fqcn", org.springframework.kafka.support.serializer.DeserializationException.class.getName().getBytes(StandardCharsets.UTF_8));
        dltRecord.headers().add("__TypeId__", "io.github.alexlitvinov.sample.event.OrderCreatedEvent".getBytes(StandardCharsets.UTF_8));
        dltRecord.headers().add("__KeyTypeId__", "com.unknown.BadKeyType".getBytes(StandardCharsets.UTF_8));

        org.apache.kafka.clients.producer.ProducerRecord<Object, Object> replayRecord =
                config.prepareReplayRecord(dltRecord, "order-events-topic", repository);

        verify(repository).resetFailed("ORD-KEY-FQCN-901", "payment-service-group");
        // Valid value __TypeId__ is preserved while corrupt __KeyTypeId__ is stripped
        assertThat(replayRecord.headers().lastHeader("__TypeId__")).isNotNull();
        assertThat(replayRecord.headers().lastHeader("__KeyTypeId__")).isNull();
    }
}
