package io.github.alexlitvinov.idembox.starter.consumer;

import io.github.alexlitvinov.idembox.core.model.ConcurrentProcessingException;
import io.github.alexlitvinov.idembox.core.model.DuplicateMessageException;
import io.github.alexlitvinov.idembox.core.repository.IdempotentRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.DefaultErrorHandler;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;

import io.github.alexlitvinov.idembox.starter.aop.IdempotentExceptionClassifier;
import org.springframework.expression.EvaluationException;
import jakarta.validation.ValidationException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.springframework.messaging.converter.MessageConversionException;
import org.springframework.messaging.handler.invocation.MethodArgumentResolutionException;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.classify.BinaryExceptionClassifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class IdemboxDeadLetterPublishingRecovererTest {

    private interface ObjectKafkaOperations extends KafkaOperations<Object, Object> {}

    private static abstract class ObjectSendResult extends org.springframework.kafka.support.SendResult<Object, Object> {
        protected ObjectSendResult() {
            super(null, null);
        }
    }

    private static ProducerRecord<Object, Object> anyProducerRecord() {
        return any();
    }

    private KafkaOperations<Object, Object> kafkaOperations;
    private IdempotentRepository repository;
    private IdemboxDeadLetterPublishingRecoverer recoverer;

    @BeforeEach
    void setUp() {
        kafkaOperations = mock(ObjectKafkaOperations.class);
        repository = mock(IdempotentRepository.class);
        when(kafkaOperations.send(anyProducerRecord()))
                .thenReturn(java.util.concurrent.CompletableFuture.completedFuture(mock(ObjectSendResult.class)));
        recoverer = new IdemboxDeadLetterPublishingRecoverer(kafkaOperations, repository);
    }

    @Test
    @DisplayName("Skips DLT publication and terminal failure marking for DuplicateMessageException")
    void shouldSkipDltForDuplicateMessageException() {
        ConsumerRecord<Object, Object> record = new ConsumerRecord<>("orders-topic", 0, 100L, "key-1", "{}");
        DuplicateMessageException duplicateEx = new DuplicateMessageException("order-101", "orders-group");

        recoverer.accept(record, duplicateEx);

        verify(kafkaOperations, never()).send(anyProducerRecord());
        verify(repository, never()).markTerminalFailed(anyString(), anyString(), anyString(), any(Duration.class));
    }

    @Test
    @DisplayName("Forwards to .DLT without marking terminal failed for ConcurrentProcessingException")
    void shouldForwardToDltWithoutTerminalFailureForConcurrentProcessing() {
        ConsumerRecord<Object, Object> record = new ConsumerRecord<>("orders-topic", 0, 101L, "key-1", "{}");
        ConcurrentProcessingException concurrentEx = new ConcurrentProcessingException("order-101", "orders-group");

        recoverer.accept(record, concurrentEx);

        verify(kafkaOperations).send(anyProducerRecord());
        verify(repository, never()).markTerminalFailed(anyString(), anyString(), anyString(), any(Duration.class));
    }

    @Test
    @DisplayName("Marks record as terminally failed in idempotency repository and forwards to .DLT for unrecoverable errors")
    void shouldMarkTerminalFailedAndForwardToDltForGeneralError() {
        RecordHeaders headers = new RecordHeaders();
        headers.add("X-Idempotency-Key", "ORD-IDEM-777".getBytes(StandardCharsets.UTF_8));
        ConsumerRecord<Object, Object> record = new ConsumerRecord<>("orders-topic", 0, 102L, "key-1", "{}");
        headers.forEach(h -> record.headers().add(h));

        recoverer.accept(record, new RuntimeException("Business processing crashed"));

        verify(kafkaOperations).send(anyProducerRecord());
        verify(repository).markTerminalFailed(eq("ORD-IDEM-777"), anyString(), eq("Business processing crashed"), any(Duration.class));
    }

    @Test
    @DisplayName("extractEventKey retrieves orderId from JSON string payload")
    void shouldExtractOrderIdFromJsonPayload() {
        String json = "{\"orderId\":\"ORDER-PAYLOAD-999\",\"amount\":100}";
        ConsumerRecord<Object, Object> record = new ConsumerRecord<>("orders-topic", 0, 103L, "customer-1", json);

        String key = recoverer.extractEventKey(record);
        assertThat(key).isEqualTo("ORDER-PAYLOAD-999");
    }

    @Test
    @DisplayName("strip DLT suffix removes repeated .dlt and .DLT suffixes")
    void shouldStripRepeatedDltSuffixes() {
        assertThat(IdemboxDeadLetterPublishingRecoverer.stripDltSuffix("orders.dlt")).isEqualTo("orders");
        assertThat(IdemboxDeadLetterPublishingRecoverer.stripDltSuffix("orders.DLT.dlt")).isEqualTo("orders");
        assertThat(IdemboxDeadLetterPublishingRecoverer.stripDltSuffix("orders")).isEqualTo("orders");
        assertThat(IdemboxDeadLetterPublishingRecoverer.stripDltSuffix(null)).isNull();
    }

    @Test
    @DisplayName("prepareReplayRecord resets failed status and prepares replay producer record")
    void shouldPrepareReplayRecordAndResetFailedStatus() {
        ConsumerRecord<Object, Object> dltRecord = new ConsumerRecord<>("orders-topic.DLT", 0, 50L, "customer-1", "{\"orderId\":\"ORD-REPLAY-1\"}");
        dltRecord.headers().add("kafka_dlt-original-topic", "orders-topic".getBytes(StandardCharsets.UTF_8));
        dltRecord.headers().add("kafka_dlt-exception-fqcn", "java.lang.RuntimeException".getBytes(StandardCharsets.UTF_8));
        dltRecord.headers().add("X-Custom-Header", "custom-value".getBytes(StandardCharsets.UTF_8));

        ProducerRecord<Object, Object> replayRecord = recoverer.prepareReplayRecord(dltRecord, "orders-topic");

        assertThat(replayRecord.topic()).isEqualTo("orders-topic");
        assertThat(replayRecord.headers().lastHeader("kafka_dlt-exception-fqcn")).isNull();
        assertThat(replayRecord.headers().lastHeader("X-Custom-Header")).isNotNull();
        assertThat(new String(replayRecord.headers().lastHeader("X-Idempotent-Replay").value(), StandardCharsets.UTF_8)).isEqualTo("true");
        verify(repository).resetFailed(eq("ORD-REPLAY-1"), anyString());
    }

    @Test
    @DisplayName("createDefaultErrorHandler configures non-retryable poison pills including DuplicateMessageException")
    void shouldConfigureDefaultErrorHandlerProperly() {
        DefaultErrorHandler errorHandler = IdemboxDeadLetterPublishingRecoverer.createDefaultErrorHandler(recoverer);
        assertThat(errorHandler).isNotNull();
    }

    record EventWithTime(String orderId, java.time.Instant createdAt, java.time.LocalDateTime localTime) {
    }

    @Test
    @DisplayName("extractEventKey handles POJOs with java.time types without JSR-310 serialization failure")
    void shouldExtractEventKeyFromPojoWithJavaTime() {
        EventWithTime pojo = new EventWithTime("ORD-TIME-123", java.time.Instant.now(), java.time.LocalDateTime.now());
        ConsumerRecord<Object, Object> record = new ConsumerRecord<>("orders-topic", 0, 104L, "customer-1", pojo);

        String key = recoverer.extractEventKey(record);
        assertThat(key).isEqualTo("ORD-TIME-123");
    }

    @Test
    @DisplayName("Issue 3: extractEventKey extracts custom keyFields configured by user")
    void shouldExtractCustomKeyFields() {
        List<String> customFields = List.of("paymentId", "invoiceNumber");
        IdemboxDeadLetterPublishingRecoverer customRecoverer = new IdemboxDeadLetterPublishingRecoverer(
                kafkaOperations,
                null,
                repository,
                new ObjectMapper(),
                Duration.ofDays(7),
                customFields,
                null
        );

        // JSON payload with paymentId
        ConsumerRecord<Object, Object> record = new ConsumerRecord<>("payments", 0, 1L, "p-key", "{\"paymentId\":\"PAY-888\"}");
        assertThat(customRecoverer.extractEventKey(record)).isEqualTo("PAY-888");

        // Header with paymentId
        ConsumerRecord<Object, Object> headerRecord = new ConsumerRecord<>("payments", 0, 2L, "p-key", "{}");
        headerRecord.headers().add("paymentId", "PAY-HEADER-999".getBytes(StandardCharsets.UTF_8));
        assertThat(customRecoverer.extractEventKey(headerRecord)).isEqualTo("PAY-HEADER-999");
    }

    @Test
    @DisplayName("Issue 4: extractEventKey falls back to record.key() when value is non-null but has no key fields")
    void shouldFallbackToRecordKeyWhenValueHasNoKeyFields() {
        // Value is non-null JSON without id/orderId, but record.key() is present
        ConsumerRecord<Object, Object> record = new ConsumerRecord<>("events", 0, 10L, "NATURAL-KAFKA-KEY", "{\"description\":\"some payload without id\"}");
        String key = recoverer.extractEventKey(record);
        assertThat(key).isEqualTo("NATURAL-KAFKA-KEY");
    }

    @Test
    @DisplayName("Issue 5: resolveDestination respects configured DLT topic suffix and topic pattern")
    void shouldRespectConfiguredDltTopicSuffixAndPattern() {
        io.github.alexlitvinov.idembox.starter.config.IdemboxProperties.DltProperties dltProps =
                new io.github.alexlitvinov.idembox.starter.config.IdemboxProperties.DltProperties();
        dltProps.setTopicSuffix("-dlq");

        ConsumerRecord<Object, Object> record = new ConsumerRecord<>("orders", 0, 1L, "k1", "{}");
        org.apache.kafka.common.TopicPartition destination = IdemboxDeadLetterPublishingRecoverer.resolveDestination(
                record, new RuntimeException("fail"), null, repository, new ObjectMapper(), Duration.ofDays(7), null, dltProps);
        assertThat(destination).isNotNull();
        assertThat(destination.topic()).isEqualTo("orders-dlq");

        // Topic pattern
        dltProps.setTopicPattern("dead-letter.{topic}");
        destination = IdemboxDeadLetterPublishingRecoverer.resolveDestination(
                record, new RuntimeException("fail"), null, repository, new ObjectMapper(), Duration.ofDays(7), null, dltProps);
        assertThat(destination).isNotNull();
        assertThat(destination.topic()).isEqualTo("dead-letter.orders");

        // Custom suffix stripping
        assertThat(IdemboxDeadLetterPublishingRecoverer.stripDltSuffix("orders-dlq", "-dlq")).isEqualTo("orders");
    }

    @Test
    @DisplayName("Issue 2: prepareReplayRecord uses instance keyFields to extract key, reset failed state and strip custom DLT suffix")
    void prepareReplayRecordShouldUseConfiguredKeyFieldsAndCustomDltSuffix() {
        IdemboxDeadLetterPublishingRecoverer customRecoverer = createCustomDltRecoverer();

        ConsumerRecord<Object, Object> dltRecord = new ConsumerRecord<>(
                "payments-custom-dlt", 0, 10L, "k1", "{\"paymentId\":\"PAY-12345\"}");
        dltRecord.headers().add("kafka_dlt-original-consumer-group", "payment-service-group".getBytes(StandardCharsets.UTF_8));

        ProducerRecord<Object, Object> replayRecord = customRecoverer.prepareReplayRecord(dltRecord, null);

        assertThat(replayRecord.topic()).isEqualTo("payments");
        verify(repository).resetFailed("PAY-12345", "payment-service-group");
    }

    private IdemboxDeadLetterPublishingRecoverer createCustomDltRecoverer() {
        io.github.alexlitvinov.idembox.starter.config.IdemboxProperties.DltProperties dltProps =
                new io.github.alexlitvinov.idembox.starter.config.IdemboxProperties.DltProperties();
        dltProps.setTopicSuffix("-custom-dlt");

        List<String> keyFields = List.of("paymentId");
        return new IdemboxDeadLetterPublishingRecoverer(
                kafkaOperations, null, repository, new ObjectMapper(), Duration.ofDays(7), keyFields, dltProps);
    }

    @Test
    @DisplayName("Issue 1: Extracts SpEL key and consumer group from Exception metadata attached by aspect")
    void shouldExtractKeyAndConsumerGroupFromExceptionMetadata() {
        ConsumerRecord<Object, Object> record = new ConsumerRecord<>(
                "orders", 0, 100L, null, "{\"transactionId\":\"tx-custom-999\"}");
        IllegalArgumentException businessEx = new IllegalArgumentException("Invalid state transition");
        IdempotentExceptionClassifier.attachMetadata(businessEx, "tx-custom-999", "billing-worker-custom");

        recoverer.accept(record, businessEx);

        verify(repository).markTerminalFailed(eq("tx-custom-999"), eq("billing-worker-custom"), eq("Invalid state transition"), eq(Duration.ofDays(7)));
    }

    @Test
    @DisplayName("Issue 1: Extracts key and consumer group from X-Idempotent headers")
    void shouldExtractKeyAndConsumerGroupFromXIdempotentHeaders() {
        ConsumerRecord<Object, Object> record = new ConsumerRecord<>(
                "orders", 0, 101L, null, "{\"customPayload\":\"val\"}");
        record.headers().add("X-Idempotent-Key", "tx-header-key".getBytes(StandardCharsets.UTF_8));
        record.headers().add("X-Idempotent-Group", "orders-header-group".getBytes(StandardCharsets.UTF_8));

        assertThat(recoverer.extractEventKey(record)).isEqualTo("tx-header-key");
        assertThat(IdemboxDeadLetterPublishingRecoverer.extractConsumerGroup(record)).isEqualTo("orders-header-group");
    }

    @Test
    @DisplayName("Issue 2: IdempotentExceptionClassifier classifies IllegalArgumentException, IllegalStateException, EvaluationException and ValidationException as fatal")
    void shouldClassifyFatalExceptionsConsistentlyWithDefaultErrorHandler() {
        IdempotentExceptionClassifier classifier = new IdempotentExceptionClassifier();

        assertThat(classifier.isKnownFatalException(new IllegalArgumentException("illegal arg"))).isTrue();
        assertThat(classifier.isKnownFatalException(new IllegalStateException("illegal state"))).isTrue();
        assertThat(classifier.isKnownFatalException(new EvaluationException("expression evaluation error"))).isTrue();
        assertThat(classifier.isKnownFatalException(new ValidationException("bean validation failed"))).isTrue();
        assertThat(classifier.isKnownFatalException(new RuntimeException("transient network timeout"))).isFalse();
    }

    @Test
    @DisplayName("Deep Audit Issue 2: createDefaultErrorHandler registers RecordTooLargeException, message and validation exceptions as non-retryable")
    void createDefaultErrorHandlerShouldIncludeAllFatalExceptions() {
        DefaultErrorHandler errorHandler = IdemboxDeadLetterPublishingRecoverer.createDefaultErrorHandler(recoverer);
        BinaryExceptionClassifier classifier = (BinaryExceptionClassifier) ReflectionTestUtils.getField(errorHandler, "classifier");
        assertThat(classifier).isNotNull();

        // In Spring Kafka, retryable exceptions classify to true, not-retryable classify to false
        assertThat(classifier.classify(new RecordTooLargeException("too large"))).isFalse();
        assertThat(classifier.classify(new MessageConversionException("conversion failed"))).isFalse();
        assertThat(classifier.classify(mock(MethodArgumentResolutionException.class))).isFalse();
        assertThat(classifier.classify(new NoSuchMethodException("missing method"))).isFalse();
        assertThat(classifier.classify(new ClassCastException("bad cast"))).isFalse();
        assertThat(classifier.classify(new IllegalArgumentException("illegal arg"))).isFalse();
        assertThat(classifier.classify(new IllegalStateException("illegal state"))).isFalse();
        assertThat(classifier.classify(new DuplicateMessageException("dup-key", "dup-group"))).isFalse();
        assertThat(classifier.classify(new EvaluationException("eval"))).isFalse();
        assertThat(classifier.classify(new ValidationException("val"))).isFalse();

        // Regular transient exceptions remain retryable (classify = true)
        assertThat(classifier.classify(new RuntimeException("transient network issue"))).isTrue();
    }

    @Test
    @DisplayName("Deep Audit Issue 3: prepareReplayRecord attaches X-Idempotent-Group when absent in original headers")
    void prepareReplayRecordShouldAddIdempotentGroupHeaderWhenMissing() {
        ConsumerRecord<Object, Object> dltRecord = new ConsumerRecord<>(
                "orders.dlt", 0, 50L, "k1", "{\"orderId\":\"ORD-777\"}");
        // Record has kafka_dlt-original-consumer-group, but NO X-Idempotent-Group header
        dltRecord.headers().add("kafka_dlt-original-consumer-group", "order-worker-group".getBytes(StandardCharsets.UTF_8));

        ProducerRecord<Object, Object> replayRecord = recoverer.prepareReplayRecord(dltRecord, "orders");

        assertThat(replayRecord.topic()).isEqualTo("orders");
        // Verify X-Idempotent-Group header was populated
        org.apache.kafka.common.header.Header groupHeader = replayRecord.headers().lastHeader("X-Idempotent-Group");
        assertThat(groupHeader).isNotNull();
        assertThat(new String(groupHeader.value(), StandardCharsets.UTF_8)).isEqualTo("order-worker-group");

        // Verify X-Idempotent-Key header was populated
        org.apache.kafka.common.header.Header keyHeader = replayRecord.headers().lastHeader("X-Idempotent-Key");
        assertThat(keyHeader).isNotNull();
        assertThat(new String(keyHeader.value(), StandardCharsets.UTF_8)).isEqualTo("ORD-777");

        // Verify kafka_dlt-* headers were stripped
        assertThat(replayRecord.headers().lastHeader("kafka_dlt-original-consumer-group")).isNull();
        // Verify X-Idempotent-Replay is present
        assertThat(replayRecord.headers().lastHeader("X-Idempotent-Replay")).isNotNull();
    }

    @Test
    @DisplayName("Deep Audit Issue 4: strip DLT suffix correctly strips suffix case-insensitively without redundant .DLT")
    void stripDltSuffixShouldHandleCaseInsensitiveSuffixes() {
        assertThat(IdemboxDeadLetterPublishingRecoverer.stripDltSuffix("orders.dlt", null)).isEqualTo("orders");
        assertThat(IdemboxDeadLetterPublishingRecoverer.stripDltSuffix("orders.DLT", null)).isEqualTo("orders");
        assertThat(IdemboxDeadLetterPublishingRecoverer.stripDltSuffix("orders-dlt", null)).isEqualTo("orders");
        assertThat(IdemboxDeadLetterPublishingRecoverer.stripDltSuffix("orders.dlq", null)).isEqualTo("orders");
        assertThat(IdemboxDeadLetterPublishingRecoverer.stripDltSuffix("orders-DLQ", null)).isEqualTo("orders");
    }

    @Test
    @DisplayName("Deep Audit Issue 3: prepareReplayRecord validates message size and handles large replay payloads safely")
    void prepareReplayRecordShouldWarnWhenSizeExceedsLimit() {
        byte[] largeBytes = new byte[1_100_000];
        ConsumerRecord<Object, Object> dltRecord = new ConsumerRecord<>(
                "orders.dlt", 0, 50L, "k1", largeBytes);

        ProducerRecord<Object, Object> replayRecord = recoverer.prepareReplayRecord(dltRecord, "orders");
        assertThat(replayRecord).isNotNull();
        assertThat(replayRecord.value()).isEqualTo(largeBytes);
    }
}
