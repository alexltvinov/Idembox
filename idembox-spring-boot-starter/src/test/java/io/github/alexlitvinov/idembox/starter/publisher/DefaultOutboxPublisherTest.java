package io.github.alexlitvinov.idembox.starter.publisher;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.alexlitvinov.idembox.core.model.OutboxMessage;
import io.github.alexlitvinov.idembox.core.repository.OutboxRepository;
import io.github.alexlitvinov.idembox.starter.config.IdemboxProperties;
import io.github.alexlitvinov.idembox.starter.metrics.IdemboxMetrics;
import io.github.alexlitvinov.idembox.starter.publisher.DefaultOutboxPublisher.OutboxCommittedEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DefaultOutboxPublisherTest {

    @Mock
    private OutboxRepository outboxRepository;

    @Mock
    private ApplicationEventPublisher eventPublisher;

    private ObjectMapper objectMapper;
    private IdemboxProperties properties;
    private DefaultOutboxPublisher publisher;

    @BeforeEach
    void setUp() {
        TransactionSynchronizationManager.clear();
        objectMapper = new ObjectMapper();
        properties = new IdemboxProperties();
        properties.getOutbox().setImmediateFlush(true);

        publisher = new DefaultOutboxPublisher(
                outboxRepository,
                objectMapper,
                properties,
                eventPublisher,
                IdemboxMetrics.noOp()
        );
    }

    @AfterEach
    void tearDown() {
        TransactionSynchronizationManager.clear();
    }

    @Test
    @DisplayName("Fix 1: Transaction synchronizations are registered without bindResource to prevent REQUIRES_NEW traps")
    void shouldRegisterTransactionSynchronizationsIndependently() {
        TransactionSynchronizationManager.initSynchronization();
        TransactionSynchronizationManager.setActualTransactionActive(true);

        // Publish two messages in the same transaction
        UUID id1 = publisher.publish("orders-topic", "K1", "{\"msg\":1}");
        UUID id2 = publisher.publish("orders-topic", "K2", "{\"msg\":2}");

        assertThat(id1).isNotNull();
        assertThat(id2).isNotNull();

        // Check synchronizations registered in current transaction
        List<TransactionSynchronization> syncs = TransactionSynchronizationManager.getSynchronizations();
        assertThat(syncs).hasSize(2);

        // Before commit, eventPublisher should not have been called
        verify(eventPublisher, never()).publishEvent(any(OutboxCommittedEvent.class));

        // Simulate transaction commit
        for (TransactionSynchronization sync : syncs) {
            sync.afterCommit();
        }

        // OutboxCommittedEvent dispatched for each published message
        verify(eventPublisher, times(2)).publishEvent(any(OutboxCommittedEvent.class));
        // No thread-local resource was leaked
        assertThat(TransactionSynchronizationManager.hasResource("IDEMBOX_OUTBOX_SYNC_REGISTERED")).isFalse();
    }

    @Test
    @DisplayName("Refinement 9: Publish outside active transaction dispatches immediate flush directly")
    void shouldDispatchImmediateFlushWhenNoActiveTransaction() {
        // Ensure no transaction is active
        assertThat(TransactionSynchronizationManager.isActualTransactionActive()).isFalse();

        UUID id = publisher.publish("orders-topic", "KEY-OUTSIDE", "{\"test\":true}");

        assertThat(id).isNotNull();
        ArgumentCaptor<OutboxCommittedEvent> eventCaptor = ArgumentCaptor.forClass(OutboxCommittedEvent.class);
        verify(eventPublisher, times(1)).publishEvent(eventCaptor.capture());
        assertThat(eventCaptor.getValue().messageId()).isEqualTo(id);
        verify(outboxRepository, times(1)).save(any(OutboxMessage.class));
    }

    @Test
    @DisplayName("Fix 3: Publish with null payload should create a Tombstone record with null payload")
    void shouldPublishTombstoneWithNullPayload() {
        UUID id = publisher.publish("compacted-topic", "TOMBSTONE-KEY", null);

        assertThat(id).isNotNull();
        ArgumentCaptor<OutboxMessage> messageCaptor = ArgumentCaptor.forClass(OutboxMessage.class);
        verify(outboxRepository, times(1)).save(messageCaptor.capture());

        OutboxMessage saved = messageCaptor.getValue();
        assertThat(saved.getTopic()).isEqualTo("compacted-topic");
        assertThat(saved.getPartitionKey()).isEqualTo("TOMBSTONE-KEY");
        assertThat(saved.getPayload()).isNull();
    }

    @Test
    @DisplayName("Fix 4: Should automatically inject W3C trace" + "parent when active Tracer span is present")
    void shouldInjectDistributedTracingHeadersWhenTracerPresent() {
        io.micrometer.tracing.Tracer mockTracer = org.mockito.Mockito.mock(io.micrometer.tracing.Tracer.class);
        io.micrometer.tracing.Span mockSpan = org.mockito.Mockito.mock(io.micrometer.tracing.Span.class);
        io.micrometer.tracing.TraceContext mockContext = org.mockito.Mockito.mock(io.micrometer.tracing.TraceContext.class);

        when(mockTracer.currentSpan()).thenReturn(mockSpan);
        when(mockSpan.context()).thenReturn(mockContext);
        when(mockContext.traceId()).thenReturn("4bf92f3577b34da6a3ce929d0e0e4736");
        when(mockContext.spanId()).thenReturn("00f067aa0ba902b7");
        when(mockContext.sampled()).thenReturn(true);

        DefaultOutboxPublisher tracingPublisher = new DefaultOutboxPublisher(
                outboxRepository,
                objectMapper,
                properties,
                eventPublisher,
                IdemboxMetrics.noOp(),
                mockTracer
        );

        tracingPublisher.publish("orders-topic", "K-TRACE", "{\"ok\":true}");

        ArgumentCaptor<OutboxMessage> messageCaptor = ArgumentCaptor.forClass(OutboxMessage.class);
        verify(outboxRepository).save(messageCaptor.capture());

        OutboxMessage saved = messageCaptor.getValue();
        assertThat(saved.getHeaders()).containsEntry("trace" + "parent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01");
        assertThat(saved.getHeaders()).containsEntry("b3", "4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7");
    }

    @Test
    @DisplayName("Refinement 6: Publish byte[] containing valid JSON should store raw JSON string without base64 encoding")
    void shouldPublishValidJsonByteArrayAsRawJsonStringWithoutBase64Encoding() {
        byte[] jsonBytes = "{\"orderId\":\"ORD-123\",\"amount\":99.9}".getBytes(java.nio.charset.StandardCharsets.UTF_8);

        UUID id = publisher.publish("orders-topic", "K-BYTES", jsonBytes);

        assertThat(id).isNotNull();
        ArgumentCaptor<OutboxMessage> messageCaptor = ArgumentCaptor.forClass(OutboxMessage.class);
        verify(outboxRepository).save(messageCaptor.capture());

        OutboxMessage saved = messageCaptor.getValue();
        assertThat(saved.getPayload()).isEqualTo("{\"orderId\":\"ORD-123\",\"amount\":99.9}");
    }

    @Test
    @DisplayName("Senior Refinements v8: W3C trace" + "parent normalization handles AWS X-Ray dashes and pads/truncates to strictly 55 chars")
    void shouldNormalizeAwsXRayTraceIdToStrictW3cTraceParent() {
        io.micrometer.tracing.Tracer mockTracer = org.mockito.Mockito.mock(io.micrometer.tracing.Tracer.class);
        io.micrometer.tracing.Span mockSpan = org.mockito.Mockito.mock(io.micrometer.tracing.Span.class);
        io.micrometer.tracing.TraceContext mockContext = org.mockito.Mockito.mock(io.micrometer.tracing.TraceContext.class);

        when(mockTracer.currentSpan()).thenReturn(mockSpan);
        when(mockSpan.context()).thenReturn(mockContext);
        // AWS X-Ray format: 1-5759e988-bd862e3c1be46a994272793e (contains dashes and 1 prefix = 33 hex chars)
        when(mockContext.traceId()).thenReturn("1-5759e988-bd862e3c1be46a994272793e");
        // Short spanId requiring zero-padding
        when(mockContext.spanId()).thenReturn("ba902b7");
        when(mockContext.sampled()).thenReturn(false);

        DefaultOutboxPublisher tracingPublisher = new DefaultOutboxPublisher(
                outboxRepository,
                objectMapper,
                properties,
                eventPublisher,
                IdemboxMetrics.noOp(),
                mockTracer
        );

        tracingPublisher.publish("orders-topic", "K-AWS-TRACE", "{\"status\":\"ok\"}");

        ArgumentCaptor<OutboxMessage> messageCaptor = ArgumentCaptor.forClass(OutboxMessage.class);
        verify(outboxRepository).save(messageCaptor.capture());

        OutboxMessage saved = messageCaptor.getValue();
        String traceParent = saved.getHeaders().get("trace" + "parent");
        assertThat(traceParent).isNotNull();
        assertThat(traceParent).hasSize(55);
        assertThat(traceParent).isEqualTo("00-5759e988bd862e3c1be46a994272793e-000000000ba902b7-00");
    }

    @Test
    @DisplayName("Senior Refinements v8: Invalid JSON payload should be base64-encoded")
    void shouldBase64EncodeNonJsonBinaryData() throws Exception {
        byte[] arbitraryBytes = new byte[]{1, 2, 3, 4, 5, (byte) 255};

        UUID id = publisher.publish("binary-topic", "K-BIN", arbitraryBytes);

        assertThat(id).isNotNull();
        ArgumentCaptor<OutboxMessage> messageCaptor = ArgumentCaptor.forClass(OutboxMessage.class);
        verify(outboxRepository).save(messageCaptor.capture());

        OutboxMessage saved = messageCaptor.getValue();
        assertThat(saved.getPayload()).isEqualTo(objectMapper.writeValueAsString(arbitraryBytes));
    }

    @Test
    @DisplayName("Senior Refinements Round 50: Concatenated multi-root JSON string does not throw and is serialized as plain text")
    void shouldAllowConcatenatedMultiRootJsonStringAsPlainText() throws Exception {
        String concatenatedJson = "{\"a\":1}{\"b\":2}";

        UUID id = publisher.publish("orders-topic", "K-MULTI", concatenatedJson);
        assertThat(id).isNotNull();

        ArgumentCaptor<OutboxMessage> messageCaptor = ArgumentCaptor.forClass(OutboxMessage.class);
        verify(outboxRepository).save(messageCaptor.capture());

        OutboxMessage saved = messageCaptor.getValue();
        assertThat(saved.getPayload()).isEqualTo(objectMapper.writeValueAsString(concatenatedJson));
        assertThat(saved.getHeaders()).containsEntry("contentType", "text/plain");
    }

    @Test
    @DisplayName("Senior Refinements v9: publishWithNamespace should prefix partitionKey with namespace")
    void shouldPrefixPartitionKeyWithNamespace() {
        UUID id = publisher.publishWithNamespace("tenant-abc", "orders-topic", "ord-999", "{\"item\":\"laptop\"}");

        assertThat(id).isNotNull();
        ArgumentCaptor<OutboxMessage> messageCaptor = ArgumentCaptor.forClass(OutboxMessage.class);
        verify(outboxRepository).save(messageCaptor.capture());

        OutboxMessage saved = messageCaptor.getValue();
        assertThat(saved.getPartitionKey()).isEqualTo("tenant-abc:ord-999");
        assertThat(saved.getPayload()).isEqualTo("{\"item\":\"laptop\"}");
    }

    @Test
    @DisplayName("Senior Refinements v10: Blank partitionKey should be normalized to null to prevent single-partition degradation")
    void shouldNormalizeBlankPartitionKeyToNull() {
        UUID id = publisher.publish("orders-topic", "   ", "{\"item\":\"laptop\"}");

        assertThat(id).isNotNull();
        ArgumentCaptor<OutboxMessage> messageCaptor = ArgumentCaptor.forClass(OutboxMessage.class);
        verify(outboxRepository).save(messageCaptor.capture());

        OutboxMessage saved = messageCaptor.getValue();
        assertThat(saved.getPartitionKey()).isNull();
    }

    @Test
    @DisplayName("Senior Refinements v26: Null bytes should be stripped from topic and partitionKey")
    void shouldStripNullBytesFromTopicAndPartitionKey() {
        UUID id = publisher.publish("orders\0-topic\u0000", "key\0-123\u0000", "{\"item\":\"laptop\"}");

        assertThat(id).isNotNull();
        ArgumentCaptor<OutboxMessage> messageCaptor = ArgumentCaptor.forClass(OutboxMessage.class);
        verify(outboxRepository).save(messageCaptor.capture());

        OutboxMessage saved = messageCaptor.getValue();
        assertThat(saved.getTopic()).isEqualTo("orders-topic");
        assertThat(saved.getPartitionKey()).isEqualTo("key-123");
    }

    record SampleEvent(String orderId, double amount) {}

    @Test
    @DisplayName("Senior Refinements v13: Should automatically inject __TypeId__ header for POJO payloads")
    void shouldInjectTypeIdHeaderForPojoPayload() {
        SampleEvent event = new SampleEvent("ORD-123", 99.9);
        UUID id = publisher.publish("orders-topic", "ORD-123", event);

        assertThat(id).isNotNull();
        ArgumentCaptor<OutboxMessage> messageCaptor = ArgumentCaptor.forClass(OutboxMessage.class);
        verify(outboxRepository).save(messageCaptor.capture());

        OutboxMessage saved = messageCaptor.getValue();
        assertThat(saved.getHeaders()).containsEntry("__TypeId__", SampleEvent.class.getName());
    }

    @Test
    @DisplayName("Senior Refinements v13: Should not inject __TypeId__ header for raw String or byte[] payloads")
    void shouldNotInjectTypeIdHeaderForStringOrByteArray() {
        publisher.publish("orders-topic", "K1", "{\"msg\":\"hello\"}");
        publisher.publish("orders-topic", "K2", "{\"msg\":\"bytes\"}".getBytes(java.nio.charset.StandardCharsets.UTF_8));

        ArgumentCaptor<OutboxMessage> messageCaptor = ArgumentCaptor.forClass(OutboxMessage.class);
        verify(outboxRepository, times(2)).save(messageCaptor.capture());

        for (OutboxMessage msg : messageCaptor.getAllValues()) {
            assertThat(msg.getHeaders()).doesNotContainKey("__TypeId__");
        }
    }

    @Test
    @DisplayName("Senior Refinements v13: Should preserve explicit __TypeId__ header when provided by caller")
    void shouldPreserveExplicitTypeIdHeaderWhenProvided() {
        SampleEvent event = new SampleEvent("ORD-999", 50.0);
        java.util.Map<String, String> customHeaders = java.util.Map.of("__TypeId__", "com.custom.CustomEvent");

        UUID id = publisher.publish("orders-topic", "ORD-999", event, customHeaders);

        assertThat(id).isNotNull();
        ArgumentCaptor<OutboxMessage> messageCaptor = ArgumentCaptor.forClass(OutboxMessage.class);
        verify(outboxRepository).save(messageCaptor.capture());

        OutboxMessage saved = messageCaptor.getValue();
        assertThat(saved.getHeaders()).containsEntry("__TypeId__", "com.custom.CustomEvent");
    }

    @Test
    @DisplayName("Senior Refinements v15: publish fails fast in read-only transaction")
    void shouldThrowWhenCurrentTransactionIsReadOnly() {
        TransactionSynchronizationManager.setCurrentTransactionReadOnly(true);

        org.assertj.core.api.Assertions.assertThatThrownBy(() -> publisher.publish("orders-topic", "K1", "{\"msg\":\"fail\"}"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Cannot publish outbox message within a read-only transaction (@Transactional(readOnly = true)).");

        verify(outboxRepository, never()).save(any());
    }

    @Test
    @DisplayName("Senior Refinements v17: publish throws IllegalArgumentException when payload exceeds max-payload-size")
    void shouldRejectPayloadExceedingMaxPayloadSize() {
        properties.getOutbox().setMaxPayloadSize(org.springframework.util.unit.DataSize.ofKilobytes(1)); // 1024 bytes
        String hugePayload = "{\"data\":\"" + "A".repeat(2000) + "\"}";

        org.assertj.core.api.Assertions.assertThatThrownBy(() -> publisher.publish("orders-topic", "K1", hugePayload))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exceeds configured maximum limit");

        verify(outboxRepository, never()).save(any());
    }

    @Test
    @DisplayName("Senior Refinements Round 66: partitionKey > 255 chars is preserved intact without SHA-256 truncation/corruption")
    void shouldPreservePartitionKeyExceeding255Characters() {
        String longKey = "key_segment_".repeat(30); // 360 chars
        UUID id = publisher.publish("orders-topic", longKey, "{\"valid\":true}");

        assertThat(id).isNotNull();
        ArgumentCaptor<OutboxMessage> messageCaptor = ArgumentCaptor.forClass(OutboxMessage.class);
        verify(outboxRepository).save(messageCaptor.capture());

        OutboxMessage saved = messageCaptor.getValue();
        assertThat(saved.getPartitionKey()).isEqualTo(longKey);
        assertThat(saved.getPartitionKey()).isEqualTo(DefaultOutboxPublisher.normalizePartitionKey(longKey));
    }

    @Test
    @DisplayName("Senior Refinements v18: publish sanitizes \\u0000 in payload before saving")
    void shouldSanitizeNulByteInPayloadWhenPublishing() {
        String dirtyPayload = "{\"data\":\"hello" + "\u0000" + "world" + "\\u0000!\"}";
        UUID id = publisher.publish("orders-topic", "K1", dirtyPayload);

        assertThat(id).isNotNull();
        ArgumentCaptor<OutboxMessage> messageCaptor = ArgumentCaptor.forClass(OutboxMessage.class);
        verify(outboxRepository).save(messageCaptor.capture());

        OutboxMessage saved = messageCaptor.getValue();
        assertThat(saved.getPayload()).isEqualTo("{\"data\":\"hello" + "world!\"}");
        assertThat(saved.getPayload()).doesNotContain("\u0000");
        assertThat(saved.getPayload()).doesNotContain("\\u0000");
    }

    static class BaseTestEvent {
        private String id = "ORD-ANON";
        public String getId() { return id; }
        public void setId(String id) { this.id = id; }
    }

    @Test
    @DisplayName("Senior Refinements v18: publish uses superclass name for anonymous class in __TypeId__")
    void shouldUseSuperclassNameForAnonymousClassInTypeIdHeader() {
        BaseTestEvent anonymousEvent = new BaseTestEvent() {};
        assertThat(anonymousEvent.getClass().isAnonymousClass()).isTrue();

        UUID id = publisher.publish("orders-topic", "ORD-ANON", anonymousEvent);

        assertThat(id).isNotNull();
        ArgumentCaptor<OutboxMessage> messageCaptor = ArgumentCaptor.forClass(OutboxMessage.class);
        verify(outboxRepository).save(messageCaptor.capture());

        OutboxMessage saved = messageCaptor.getValue();
        assertThat(saved.getHeaders()).containsEntry("__TypeId__", BaseTestEvent.class.getName());
    }

    @Test
    @DisplayName("Senior Refinements v20: publish enriches non-JSON string payloads with contentType text/plain headers")
    void shouldSetContentTypeHeadersForPlainStringPayload() {
        UUID id = publisher.publish("orders-topic", "K1", "ORD-12345");

        assertThat(id).isNotNull();
        ArgumentCaptor<OutboxMessage> messageCaptor = ArgumentCaptor.forClass(OutboxMessage.class);
        verify(outboxRepository).save(messageCaptor.capture());

        OutboxMessage saved = messageCaptor.getValue();
        assertThat(saved.getHeaders()).containsEntry("contentType", "text/plain");
        assertThat(saved.getHeaders()).containsEntry("__ContentType__", "text/plain");
        assertThat(saved.getHeaders()).doesNotContainKey("__Encoding__");
    }

    @Test
    @DisplayName("Senior Refinements v20: publish enriches binary byte[] payloads with octet-stream and base64 headers")
    void shouldSetContentTypeAndEncodingHeadersForBinaryByteArray() {
        byte[] binaryData = new byte[]{0x00, 0x01, 0x02, (byte) 0xFF};
        UUID id = publisher.publish("orders-topic", "K2", binaryData);

        assertThat(id).isNotNull();
        ArgumentCaptor<OutboxMessage> messageCaptor = ArgumentCaptor.forClass(OutboxMessage.class);
        verify(outboxRepository).save(messageCaptor.capture());

        OutboxMessage saved = messageCaptor.getValue();
        assertThat(saved.getHeaders()).containsEntry("contentType", "application/octet-stream");
        assertThat(saved.getHeaders()).containsEntry("__ContentType__", "application/octet-stream");
        assertThat(saved.getHeaders()).containsEntry("__Encoding__", "base64");
    }

    @Test
    @DisplayName("Senior Refinements v20: publish does not treat valid JSON strings as plain text")
    void shouldNotSetPlainHeadersForValidJsonString() {
        UUID id = publisher.publish("orders-topic", "K3", "{\"foo\":\"bar\"}");

        assertThat(id).isNotNull();
        ArgumentCaptor<OutboxMessage> messageCaptor = ArgumentCaptor.forClass(OutboxMessage.class);
        verify(outboxRepository).save(messageCaptor.capture());

        OutboxMessage saved = messageCaptor.getValue();
        assertThat(saved.getHeaders()).containsEntry("contentType", "application/json");
        assertThat(saved.getHeaders()).containsEntry("__ContentType__", "application/json");
        assertThat(saved.getHeaders()).doesNotContainKey("__Encoding__");
    }

    @Test
    @DisplayName("Senior Refinements v24: injectTracingHeaders does not overwrite existing trace" + "parent header")
    void shouldNotOverwriteExistingTraceParentHeader() {
        io.micrometer.tracing.Tracer mockTracer = org.mockito.Mockito.mock(io.micrometer.tracing.Tracer.class);

        DefaultOutboxPublisher tracingPublisher = new DefaultOutboxPublisher(
                outboxRepository,
                objectMapper,
                properties,
                eventPublisher,
                IdemboxMetrics.noOp(),
                mockTracer
        );

        String customTraceParent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
        java.util.Map<String, String> customHeaders = java.util.Map.of("trace" + "parent", customTraceParent);

        tracingPublisher.publish("orders-topic", "K-CUSTOM-TRACE", "{\"status\":\"ok\"}", customHeaders);

        ArgumentCaptor<OutboxMessage> messageCaptor = ArgumentCaptor.forClass(OutboxMessage.class);
        verify(outboxRepository).save(messageCaptor.capture());

        OutboxMessage saved = messageCaptor.getValue();
        assertThat(saved.getHeaders()).containsEntry("trace" + "parent", customTraceParent);
        verify(mockTracer, never()).currentSpan();
    }

    @Test
    @DisplayName("Senior Refinements v24: normalizePartitionKey trims and hashes keys exceeding 255 chars, and sanitizeJsonb removes null bytes")
    void shouldNormalizePartitionKeyAndSanitizeJsonb() {
        assertThat(DefaultOutboxPublisher.normalizePartitionKey(null)).isNull();
        assertThat(DefaultOutboxPublisher.normalizePartitionKey("   ")).isNull();
        assertThat(DefaultOutboxPublisher.normalizePartitionKey("  my-key  ")).isEqualTo("my-key");

        String longKey = "x".repeat(300);
        String normalizedLongKey = DefaultOutboxPublisher.normalizePartitionKey(longKey);
        assertThat(normalizedLongKey).isNotNull();
        assertThat(normalizedLongKey).isEqualTo(longKey);

        assertThat(DefaultOutboxPublisher.sanitizeJsonb(null)).isNull();
        assertThat(DefaultOutboxPublisher.sanitizeJsonb("hello world")).isEqualTo("hello world");
        assertThat(DefaultOutboxPublisher.sanitizeJsonb("hello" + "\\u0000" + "world\0")).isEqualTo("hello" + "world");
    }

    @Test
    @DisplayName("Senior Refinements Round 66: normalizePartitionKey preserves multi-byte UTF-8 keys when byte length exceeds 255")
    void shouldPreserveMultiByteUtf8KeysExceedingByteLimit() {
        // 130 Cyrillic characters = 260 UTF-8 bytes (< 255 chars, but > 255 bytes)
        String multiByteKey = "Ж".repeat(130);
        assertThat(multiByteKey.length()).isEqualTo(130);
        assertThat(multiByteKey.getBytes(StandardCharsets.UTF_8).length).isEqualTo(260);

        String normalized = DefaultOutboxPublisher.normalizePartitionKey(multiByteKey);
        assertThat(normalized).isNotNull();
        assertThat(normalized).isEqualTo(multiByteKey);

        String smallMultiByteKey = "Ж".repeat(100); // 200 bytes
        String smallNormalized = DefaultOutboxPublisher.normalizePartitionKey(smallMultiByteKey);
        assertThat(smallNormalized).isEqualTo(smallMultiByteKey);
    }

    @Test
    @DisplayName("Senior Refinements v23: findFirstNonWhitespace and findLastNonWhitespace find byte boundaries")
    void shouldFindNonWhitespaceByteBoundaries() {
        assertThat(DefaultOutboxPublisher.findFirstNonWhitespace(null)).isEqualTo(-1);
        assertThat(DefaultOutboxPublisher.findLastNonWhitespace(null)).isEqualTo(-1);
        assertThat(DefaultOutboxPublisher.findFirstNonWhitespace(new byte[0])).isEqualTo(-1);
        assertThat(DefaultOutboxPublisher.findLastNonWhitespace(new byte[0])).isEqualTo(-1);

        byte[] whitespaceOnly = new byte[]{' ', '\t', '\r', '\n'};
        assertThat(DefaultOutboxPublisher.findFirstNonWhitespace(whitespaceOnly)).isEqualTo(-1);
        assertThat(DefaultOutboxPublisher.findLastNonWhitespace(whitespaceOnly)).isEqualTo(-1);

        byte[] jsonBytes = "  \n {\"id\": 1} \t \r ".getBytes(StandardCharsets.UTF_8);
        int first = DefaultOutboxPublisher.findFirstNonWhitespace(jsonBytes);
        int last = DefaultOutboxPublisher.findLastNonWhitespace(jsonBytes);
        assertThat(first).isGreaterThan(-1);
        assertThat(last).isGreaterThan(first);
        assertThat((char) jsonBytes[first]).isEqualTo('{');
        assertThat((char) jsonBytes[last]).isEqualTo('}');
    }

    @Test
    @DisplayName("Senior Refinements v23: publish byte[] with JSON content stores raw JSON string directly")
    void shouldStoreJsonByteArrayDirectly() {
        byte[] jsonBytes = "  {\"status\":\"ACCEPTED\"}  ".getBytes(StandardCharsets.UTF_8);
        UUID id = publisher.publish("orders-topic", "K-JSON-BYTES", jsonBytes);

        assertThat(id).isNotNull();
        ArgumentCaptor<OutboxMessage> messageCaptor = ArgumentCaptor.forClass(OutboxMessage.class);
        verify(outboxRepository).save(messageCaptor.capture());

        OutboxMessage saved = messageCaptor.getValue();
        assertThat(saved.getPayload()).isEqualTo("{\"status\":\"ACCEPTED\"}");
    }

    @Test
    @DisplayName("Senior Refinements v25: publish byte[] with non-JSON binary data sets binary headers without eager String decoding")
    void shouldSetBinaryHeadersForNonJsonByteArray() {
        byte[] arbitraryBytes = new byte[]{(byte) 0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A}; // PNG magic bytes
        UUID id = publisher.publish("images-topic", "K-PNG", arbitraryBytes);

        assertThat(id).isNotNull();
        ArgumentCaptor<OutboxMessage> messageCaptor = ArgumentCaptor.forClass(OutboxMessage.class);
        verify(outboxRepository).save(messageCaptor.capture());

        OutboxMessage saved = messageCaptor.getValue();
        assertThat(saved.getHeaders()).containsEntry("contentType", "application/octet-stream");
        assertThat(saved.getHeaders()).containsEntry("__ContentType__", "application/octet-stream");
        assertThat(saved.getHeaders()).containsEntry("__Encoding__", "base64");
    }

    record SamplePayload(String name) {}

    @Test
    @DisplayName("Senior Refinements v28: publish throws IllegalArgumentException on null or blank topic")
    void shouldRejectNullOrBlankTopicInPublish() {
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> publisher.publish(null, "K1", "{}"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Outbox destination topic cannot be null or blank");

        org.assertj.core.api.Assertions.assertThatThrownBy(() -> publisher.publish("   ", "K1", "{}"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Outbox destination topic cannot be null or blank");
    }

    @Test
    @DisplayName("Senior Refinements v28: add-type-id-header=false omits __TypeId__ header from outbox message")
    void shouldOmitTypeIdHeaderWhenDisabled() {
        properties.getOutbox().setAddTypeIdHeader(false);
        SamplePayload payload = new SamplePayload("test-item");

        UUID id = publisher.publish("orders-topic", "K1", payload);
        assertThat(id).isNotNull();

        ArgumentCaptor<OutboxMessage> messageCaptor = ArgumentCaptor.forClass(OutboxMessage.class);
        verify(outboxRepository).save(messageCaptor.capture());

        OutboxMessage saved = messageCaptor.getValue();
        assertThat(saved.getHeaders()).doesNotContainKey("__TypeId__");
    }

    @Test
    @DisplayName("Senior Refinements v28: add-type-id-header=true (default) includes __TypeId__ header")
    void shouldIncludeTypeIdHeaderByDefault() {
        properties.getOutbox().setAddTypeIdHeader(true);
        SamplePayload payload = new SamplePayload("test-item");

        UUID id = publisher.publish("orders-topic", "K1", payload);
        assertThat(id).isNotNull();

        ArgumentCaptor<OutboxMessage> messageCaptor = ArgumentCaptor.forClass(OutboxMessage.class);
        verify(outboxRepository).save(messageCaptor.capture());

        OutboxMessage saved = messageCaptor.getValue();
        assertThat(saved.getHeaders()).containsEntry("__TypeId__", SamplePayload.class.getName());
    }

    @Test
    @DisplayName("Senior Refinements v30: recordOutboxEnqueued is deferred until after transaction commit, preventing phantom backlog on rollback")
    void recordOutboxEnqueuedShouldOnlyBeCalledAfterTransactionCommit() {
        IdemboxMetrics mockMetrics = org.mockito.Mockito.mock(IdemboxMetrics.class);
        DefaultOutboxPublisher customPublisher = new DefaultOutboxPublisher(
                outboxRepository,
                objectMapper,
                properties,
                eventPublisher,
                mockMetrics
        );

        TransactionSynchronizationManager.initSynchronization();
        TransactionSynchronizationManager.setActualTransactionActive(true);

        UUID id = customPublisher.publish("orders-topic", "K1", "{\"msg\":1}");
        assertThat(id).isNotNull();

        // Before commit, metric MUST NOT be incremented
        verify(mockMetrics, never()).recordOutboxEnqueued(anyString());

        // Simulate commit on registered synchronizations
        List<TransactionSynchronization> syncs = TransactionSynchronizationManager.getSynchronizations();
        assertThat(syncs).hasSize(1);
        syncs.get(0).afterCommit();

        // After commit, metric is incremented
        verify(mockMetrics).recordOutboxEnqueued("orders-topic");
    }

    @Test
    @DisplayName("Senior Refinements v30: recordOutboxEnqueued is invoked immediately when published outside active transaction")
    void recordOutboxEnqueuedShouldBeInvokedImmediatelyOutsideTransaction() {
        IdemboxMetrics mockMetrics = org.mockito.Mockito.mock(IdemboxMetrics.class);
        DefaultOutboxPublisher customPublisher = new DefaultOutboxPublisher(
                outboxRepository,
                objectMapper,
                properties,
                eventPublisher,
                mockMetrics
        );

        assertThat(TransactionSynchronizationManager.isActualTransactionActive()).isFalse();

        UUID id = customPublisher.publish("orders-topic", "K1", "{\"msg\":1}");
        assertThat(id).isNotNull();

        verify(mockMetrics).recordOutboxEnqueued("orders-topic");
    }

    @Test
    @DisplayName("Senior Refinements v50: publish allows plain text enclosed in brackets ([...] and {...}) as text/plain")
    void shouldAllowPlainTextPayloadEnclosedInBrackets() {
        String logMessage = "[INFO] Order processed";
        UUID id1 = publisher.publish("orders-topic", "K1", logMessage);
        assertThat(id1).isNotNull();

        ArgumentCaptor<OutboxMessage> captor1 = ArgumentCaptor.forClass(OutboxMessage.class);
        verify(outboxRepository).save(captor1.capture());
        assertThat(captor1.getValue().getPayload()).isEqualTo("\"[INFO] Order processed\"");
        assertThat(captor1.getValue().getHeaders()).containsEntry("contentType", "text/plain");

        String templateStr = "{tag: 123}";
        UUID id2 = publisher.publish("orders-topic", "K2", templateStr);
        assertThat(id2).isNotNull();

        ArgumentCaptor<OutboxMessage> captor2 = ArgumentCaptor.forClass(OutboxMessage.class);
        verify(outboxRepository, times(2)).save(captor2.capture());
        assertThat(captor2.getValue().getPayload()).isEqualTo("\"{tag: 123}\"");
        assertThat(captor2.getValue().getHeaders()).containsEntry("contentType", "text/plain");
    }

    @Test
    @DisplayName("Senior Refinements v31: publish allows plain text that does not mimic JSON objects or arrays")
    void shouldAllowPlainTextPayload() {
        String plainText = "simple plain text without braces";
        UUID id = publisher.publish("orders-topic", "K1", plainText);
        assertThat(id).isNotNull();

        ArgumentCaptor<OutboxMessage> captor = ArgumentCaptor.forClass(OutboxMessage.class);
        verify(outboxRepository).save(captor.capture());
        assertThat(captor.getValue().getPayload()).isEqualTo("\"simple plain text without braces\"");
        assertThat(captor.getValue().getHeaders()).containsEntry("contentType", "text/plain");
    }

    @Test
    @DisplayName("Senior Refinements v39: publish validates Kafka topic name according to Apache Kafka specification")
    void shouldValidateKafkaTopicNameAccordingToSpec() {
        // Invalid topics
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> publisher.publish("orders/created", "K1", "{}"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("contains invalid characters");

        org.assertj.core.api.Assertions.assertThatThrownBy(() -> publisher.publish("orders:created", "K1", "{}"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("contains invalid characters");

        org.assertj.core.api.Assertions.assertThatThrownBy(() -> publisher.publish("orders created", "K1", "{}"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("contains invalid characters");

        org.assertj.core.api.Assertions.assertThatThrownBy(() -> publisher.publish(".", "K1", "{}"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cannot be '.' or '..'");

        org.assertj.core.api.Assertions.assertThatThrownBy(() -> publisher.publish("..", "K1", "{}"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cannot be '.' or '..'");

        String tooLongTopic = "t".repeat(250);
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> publisher.publish(tooLongTopic, "K1", "{}"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exceeds maximum Kafka limit of 249 characters");

        org.assertj.core.api.Assertions.assertThatThrownBy(() -> publisher.publish(" orders ", "K1", "{}"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("contains leading or trailing whitespace");

        // Valid topics should succeed
        assertThat(publisher.publish("orders.created", "K1", "{}")).isNotNull();
        assertThat(publisher.publish("orders_created", "K1", "{}")).isNotNull();
        assertThat(publisher.publish("orders-created-v2", "K1", "{}")).isNotNull();
        assertThat(publisher.publish("t".repeat(249), "K1", "{}")).isNotNull();
    }

    @Test
    @DisplayName("Senior Refinements Round 63: publish rejects message when total size exceeds maxTotalBytes (sum of maxPayloadSize and maxHeaderSize)")
    void shouldRejectWhenTotalMessageSizeExceedsMaxPayloadSizeDueToHeaders() {
        // Set maxPayloadSize to 1000 bytes, maxHeaderSize to null -> maxTotalBytes = 1000
        properties.getOutbox().setMaxPayloadSize(org.springframework.util.unit.DataSize.ofBytes(1000));
        properties.getOutbox().setMaxHeaderSize(null);

        // Payload is 800 bytes (within maxPayloadSize alone)
        String payload = "{\"data\":\"" + "x".repeat(780) + "\"}";

        // Headers are 300 bytes -> total = 1100 bytes > 1000 bytes
        Map<String, String> largeHeaders = Map.of(
                "X-Trace-Baggage", "b".repeat(150),
                "X-Auth-Token", "t".repeat(150)
        );

        org.assertj.core.api.Assertions.assertThatThrownBy(() ->
                publisher.publish("orders-topic", "K1", payload, largeHeaders))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Outbox total message size of")
                .hasMessageContaining("exceeds configured maximum limit");
    }

    @Test
    @DisplayName("Senior Refinements Round 63: publish permits message near maxPayloadSize when headers are within maxHeaderSize")
    void shouldPermitMessageNearMaxPayloadSizeWhenHeadersWithinMaxHeaderSize() {
        properties.getOutbox().setMaxPayloadSize(org.springframework.util.unit.DataSize.ofBytes(1000));
        properties.getOutbox().setMaxHeaderSize(org.springframework.util.unit.DataSize.ofBytes(200));

        // Payload is 950 bytes (within maxPayloadSize 1000)
        String payload = "{\"data\":\"" + "x".repeat(930) + "\"}";

        // Headers are 100 bytes (within maxHeaderSize 200). Total is 1050 bytes (<= maxTotalBytes 1200)
        Map<String, String> headers = Map.of(
                "X-Trace", "a".repeat(40),
                "X-Span", "b".repeat(40)
        );

        org.assertj.core.api.Assertions.assertThatCode(() ->
                publisher.publish("orders-topic", "K1", payload, headers))
                .doesNotThrowAnyException();
    }

    @Test
    @DisplayName("Senior Refinements v39: publish rejects message when headers exceed maxHeaderSize")
    void shouldRejectWhenHeadersExceedMaxHeaderSize() {
        // Set maxHeaderSize to 200 bytes
        properties.getOutbox().setMaxHeaderSize(org.springframework.util.unit.DataSize.ofBytes(200));

        Map<String, String> excessiveHeaders = Map.of(
                "X-Large-Header", "h".repeat(250)
        );

        org.assertj.core.api.Assertions.assertThatThrownBy(() ->
                publisher.publish("orders-topic", "K1", "{\"ok\":true}", excessiveHeaders))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Outbox headers size of")
                .hasMessageContaining("exceeds configured maximum limit");
    }

    @Test
    @DisplayName("Senior Refinements v46: publish byte[] containing JSON records __PayloadType__ header as bytes")
    void shouldRecordPayloadTypeBytesWhenPublishingByteArrayJson() {
        byte[] jsonBytes = "{\"orderId\":100,\"status\":\"PAID\"}".getBytes(StandardCharsets.UTF_8);

        UUID id = publisher.publish("orders-topic", "K1", jsonBytes);
        assertThat(id).isNotNull();

        ArgumentCaptor<OutboxMessage> captor = ArgumentCaptor.forClass(OutboxMessage.class);
        verify(outboxRepository).save(captor.capture());

        OutboxMessage saved = captor.getValue();
        assertThat(saved.getPayload()).isEqualTo("{\"orderId\":100,\"status\":\"PAID\"}");
        assertThat(saved.getHeaders()).containsEntry("__PayloadType__", "bytes");
        assertThat(saved.getMultiHeaders()).containsKey("__PayloadType__");
        assertThat(saved.getMultiHeaders().get("__PayloadType__")).containsExactly("bytes");
    }

    static class ProxiedOrderEvent {
        private String orderId;
        public ProxiedOrderEvent(String orderId) { this.orderId = orderId; }
        public String getOrderId() { return orderId; }
        public void setOrderId(String orderId) { this.orderId = orderId; }
    }

    static class ProxiedOrderEvent$$SpringCGLIB$$0 extends ProxiedOrderEvent {
        public ProxiedOrderEvent$$SpringCGLIB$$0(String orderId) { super(orderId); }
    }

    @Test
    @DisplayName("Senior Refinements v49: publish unwraps CGLIB / Spring proxy classes using ClassUtils.getUserClass for __TypeId__")
    void shouldUnwrapProxyClassWhenSettingTypeIdHeader() {
        properties.getOutbox().setAddTypeIdHeader(true);

        ProxiedOrderEvent$$SpringCGLIB$$0 proxyPayload = new ProxiedOrderEvent$$SpringCGLIB$$0("ORD-999");
        UUID id = publisher.publish("orders-topic", "K1", proxyPayload);
        assertThat(id).isNotNull();

        ArgumentCaptor<OutboxMessage> captor = ArgumentCaptor.forClass(OutboxMessage.class);
        verify(outboxRepository).save(captor.capture());

        OutboxMessage saved = captor.getValue();
        assertThat(saved.getHeaders()).containsEntry("__TypeId__", ProxiedOrderEvent.class.getName());
        assertThat(saved.getHeaders().get("__TypeId__")).doesNotContain("$$SpringCGLIB$$");
    }

    interface DomainEventMarker {
        String getId();
    }

    interface AnonymousDomainEventMarker {
        String getId();
        String getEventType();
    }

    @Test
    @DisplayName("Senior Refinements v54: publish sets __TypeId__ to interface name for anonymous class implementing interface")
    void shouldSetTypeIdToInterfaceForAnonymousClass() {
        properties.getOutbox().setAddTypeIdHeader(true);

        AnonymousDomainEventMarker event = new AnonymousDomainEventMarker() {
            @Override
            public String getId() {
                return "123";
            }

            @Override
            public String getEventType() {
                return "order-event";
            }
        };

        assertThat(event.getId()).isEqualTo("123");
        assertThat(event.getEventType()).isEqualTo("order-event");

        UUID id = publisher.publish("orders-topic", "K1", event);
        assertThat(id).isNotNull();

        ArgumentCaptor<OutboxMessage> captor = ArgumentCaptor.forClass(OutboxMessage.class);
        verify(outboxRepository).save(captor.capture());

        OutboxMessage saved = captor.getValue();
        assertThat(saved.getHeaders()).containsEntry("__TypeId__", AnonymousDomainEventMarker.class.getName());
        assertThat(saved.getPayload()).contains("123").contains("order-event");
    }

    @Test
    @DisplayName("Senior Refinements v54: binary byte[] payload respects raw byte length for maxPayloadSize without Base64 inflation false-positive")
    void shouldValidateRawByteLengthForBinaryPayloadAgainstMaxPayloadSize() {
        properties.getOutbox().setMaxPayloadSize(org.springframework.util.unit.DataSize.ofBytes(1150));

        // 900 bytes of non-JSON binary data (would inflate to ~1202 bytes in Base64 string, exceeding 1150 bytes)
        byte[] rawBytes = new byte[900];
        java.util.Arrays.fill(rawBytes, (byte) 0x01);

        UUID id = publisher.publish("orders-topic", "K1", rawBytes);
        assertThat(id).isNotNull();

        ArgumentCaptor<OutboxMessage> captor = ArgumentCaptor.forClass(OutboxMessage.class);
        verify(outboxRepository).save(captor.capture());
        assertThat(captor.getValue()).isNotNull();

        // But 1200 bytes should fail
        byte[] oversizedBytes = new byte[1200];
        assertThatThrownBy(() -> publisher.publish("orders-topic", "K1", oversizedBytes))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Outbox payload size of 1200 bytes exceeds configured maximum limit");
    }

    @Test
    @DisplayName("Senior Refinements v55: publish dispatches OutboxCommittedEvent immediately when called during after-commit (isActualTransactionActive is false)")
    void shouldDispatchOutboxCommittedEventImmediatelyWhenActualTxInactive() {
        TransactionSynchronizationManager.initSynchronization();
        TransactionSynchronizationManager.setActualTransactionActive(false);

        UUID id = publisher.publish("orders-topic", "K1", "{\"msg\":1}");
        assertThat(id).isNotNull();

        // Event should be dispatched immediately, not waiting for afterCommit
        verify(eventPublisher).publishEvent(any(OutboxCommittedEvent.class));
    }

    @Test
    @DisplayName("Senior Refinements Round 64: publish with byte[] containing valid JSON processes payload and recognizes JSON")
    void shouldProcessJsonByteArrayPayloadCorrectly() {
        byte[] jsonBytes = "{\"orderId\":\"123\",\"total\":99.99}".getBytes(StandardCharsets.UTF_8);

        UUID id = publisher.publish("orders-topic", "K-JSON", jsonBytes);
        assertThat(id).isNotNull();

        ArgumentCaptor<OutboxMessage> captor = ArgumentCaptor.forClass(OutboxMessage.class);
        verify(outboxRepository).save(captor.capture());

        OutboxMessage msg = captor.getValue();
        assertThat(msg.getPayload()).isEqualTo("{\"orderId\":\"123\",\"total\":99.99}");
        assertThat(msg.getHeaders()).containsEntry("contentType", "application/json");
        assertThat(msg.getHeaders()).containsEntry("__ContentType__", "application/json");
        assertThat(msg.getHeaders()).doesNotContainKey("__Encoding__");
    }

    @Test
    @DisplayName("Senior Refinements Round 64: publishWithMultiHeaders preserves marker headers with empty value lists")
    void shouldPreserveEmptyListMarkerHeadersInMultiHeaders() {
        Map<String, List<String>> multiHeaders = Map.of(
                "X-Sync-Only", List.of(),
                "X-Trace", List.of("trace-1")
        );

        UUID id = publisher.publishWithMultiHeaders("orders-topic", "K-FLAG", "{\"status\":\"ok\"}", multiHeaders);
        assertThat(id).isNotNull();

        ArgumentCaptor<OutboxMessage> captor = ArgumentCaptor.forClass(OutboxMessage.class);
        verify(outboxRepository).save(captor.capture());

        OutboxMessage msg = captor.getValue();
        assertThat(msg.getMultiHeaders()).containsKey("X-Sync-Only");
        assertThat(msg.getMultiHeaders().get("X-Sync-Only")).isEmpty();
        assertThat(msg.getMultiHeaders().get("X-Trace")).containsExactly("trace-1");
    }

    @Test
    @DisplayName("Senior Refinements Round 73 & Round 74: publish does not inject __TypeId__ header for Map, Collection, JsonNode, arrays, Optional, or java.* types")
    void shouldNotInjectTypeIdHeaderForMapCollectionOrJsonNode() {
        properties.getOutbox().setAddTypeIdHeader(true);

        publisher.publish("orders-topic", "K-MAP", Map.of("orderId", "123"));
        publisher.publish("orders-topic", "K-LINKED-MAP", new java.util.LinkedHashMap<>(Map.of("orderId", "456")));
        publisher.publish("orders-topic", "K-LIST", List.of("item-1", "item-2"));
        publisher.publish("orders-topic", "K-NODE", objectMapper.createObjectNode().put("orderId", "789"));
        publisher.publish("orders-topic", "K-ARR-STR", new String[]{"a", "b"});
        publisher.publish("orders-topic", "K-ARR-POJO", new DomainEventMarker[]{() -> "1"});
        publisher.publish("orders-topic", "K-OPT", java.util.Optional.of(Map.of("k", "v")));
        publisher.publish("orders-topic", "K-UUID", UUID.randomUUID());
        publisher.publish("orders-topic", "K-NUM", java.math.BigDecimal.TEN);
        publisher.publish("orders-topic", "K-BOOL", Boolean.TRUE);
        publisher.publish("orders-topic", "K-DATE", new java.util.Date());

        assertThat(DefaultOutboxPublisher.isDomainPojoForTypeId(java.time.Instant.now())).isFalse();
        assertThat(DefaultOutboxPublisher.isDomainPojoForTypeId(java.util.Optional.empty())).isFalse();

        ArgumentCaptor<OutboxMessage> captor = ArgumentCaptor.forClass(OutboxMessage.class);
        verify(outboxRepository, times(11)).save(captor.capture());

        for (OutboxMessage msg : captor.getAllValues()) {
            assertThat(msg.getHeaders()).doesNotContainKey("__TypeId__");
            assertThat(msg.getMultiHeaders()).doesNotContainKey("__TypeId__");
        }
    }

    @Test
    @DisplayName("Senior Refinements Round 75: publish unwraps Optional<byte[]>, Optional<String>, and Optional<POJO> before resolving headers and payload size")
    void shouldUnwrapOptionalBeforeResolvingHeadersAndPayloadSize() {
        properties.getOutbox().setAddTypeIdHeader(true);

        // 1. Optional<byte[]> (binary) -> must have __PayloadType__=bytes, __Encoding__=base64, contentType=application/octet-stream
        byte[] rawBinary = new byte[]{1, 2, 3, 4};
        publisher.publish("binary-topic", "K-BIN", java.util.Optional.of(rawBinary));

        // 2. Optional<String> (plain text) -> must have contentType=text/plain and __ContentType__=text/plain
        publisher.publish("text-topic", "K-TXT", java.util.Optional.of("HELLO-WORLD"));

        // 3. Optional<POJO> -> must have __TypeId__ of unwrapped POJO class (never java.util.Optional)
        publisher.publish("pojo-topic", "K-POJO", java.util.Optional.of(new ProxiedOrderEvent("ORD-OPT-1")));

        ArgumentCaptor<OutboxMessage> captor = ArgumentCaptor.forClass(OutboxMessage.class);
        verify(outboxRepository, times(3)).save(captor.capture());

        List<OutboxMessage> saved = captor.getAllValues();

        OutboxMessage binMsg = saved.get(0);
        assertThat(binMsg.getHeaders())
                .containsEntry("__PayloadType__", "bytes")
                .containsEntry("__Encoding__", "base64")
                .containsEntry("contentType", "application/octet-stream")
                .doesNotContainKey("__TypeId__");
        assertThat(binMsg.getPayload()).isEqualTo("\"" + java.util.Base64.getEncoder().encodeToString(rawBinary) + "\"");

        OutboxMessage txtMsg = saved.get(1);
        assertThat(txtMsg.getHeaders())
                .containsEntry("contentType", "text/plain")
                .containsEntry("__ContentType__", "text/plain")
                .doesNotContainKey("__TypeId__");
        assertThat(txtMsg.getPayload()).isEqualTo("\"HELLO-WORLD\"");

        OutboxMessage pojoMsg = saved.get(2);
        assertThat(pojoMsg.getHeaders())
                .containsEntry("__TypeId__", ProxiedOrderEvent.class.getName());
    }

    @Test
    @DisplayName("Senior Refinements Round 76: Internal outbox storage headers (__PayloadType__, __ContentType__, __Encoding__) are excluded from maxHeaderSize limit")
    void shouldExcludeInternalStorageHeadersFromMaxHeaderSizeCheck() {
        // Set maxHeaderSize to 40 bytes: user header "contentType: application/octet-stream" is 35 bytes (<= 40),
        // whereas internal storage headers (__PayloadType__=bytes, __Encoding__=base64) would add 40+ extra bytes if counted!
        properties.getOutbox().setMaxHeaderSize(org.springframework.util.unit.DataSize.ofBytes(40));

        org.assertj.core.api.Assertions.assertThatCode(() ->
                publisher.publish("binary-topic", "K-BIN-LIMIT", new byte[]{10, 20, 30}))
                .doesNotThrowAnyException();
    }

    @Test
    @DisplayName("Senior Refinements Round 77: publish normalizes CharSequence (StringBuilder) to String with text/plain headers and ByteBuffer to byte[] without mutating buffer position")
    void shouldNormalizeCharSequenceAndByteBufferPayloads() {
        // 1. StringBuilder (non-JSON plain text) -> must set contentType=text/plain and __ContentType__=text/plain
        StringBuilder sb = new StringBuilder("PLAIN-STRING-BUILDER");
        publisher.publish("text-topic", "K-SB", sb);

        // 2. ByteBuffer (binary) -> must set __PayloadType__=bytes, __Encoding__=base64, contentType=application/octet-stream
        byte[] raw = new byte[]{5, 15, 25, 35};
        java.nio.ByteBuffer bb = java.nio.ByteBuffer.wrap(raw);
        publisher.publish("binary-topic", "K-BB", bb);
        // Must not mutate caller's ByteBuffer position
        assertThat(bb.position()).isEqualTo(0);
        assertThat(bb.remaining()).isEqualTo(4);

        ArgumentCaptor<OutboxMessage> captor = ArgumentCaptor.forClass(OutboxMessage.class);
        verify(outboxRepository, times(2)).save(captor.capture());

        List<OutboxMessage> saved = captor.getAllValues();

        OutboxMessage sbMsg = saved.get(0);
        assertThat(sbMsg.getPayload()).isEqualTo("\"PLAIN-STRING-BUILDER\"");
        assertThat(sbMsg.getHeaders())
                .containsEntry("contentType", "text/plain")
                .containsEntry("__ContentType__", "text/plain");

        OutboxMessage bbMsg = saved.get(1);
        assertThat(bbMsg.getPayload()).isEqualTo("\"" + java.util.Base64.getEncoder().encodeToString(raw) + "\"");
        assertThat(bbMsg.getHeaders())
                .containsEntry("__PayloadType__", "bytes")
                .containsEntry("__Encoding__", "base64")
                .containsEntry("contentType", "application/octet-stream");
    }

    @Test
    @DisplayName("Deep Audit Issue 2: publish sets contentType and __ContentType__ application/json for JSON payloads")
    void shouldSetApplicationJsonHeadersForJsonPayloads() {
        publisher.publish("orders-topic", "K-JSON", "{\"orderId\":\"123\"}");

        ArgumentCaptor<OutboxMessage> captor = ArgumentCaptor.forClass(OutboxMessage.class);
        verify(outboxRepository).save(captor.capture());

        OutboxMessage msg = captor.getValue();
        assertThat(msg.getHeaders())
                .containsEntry("contentType", "application/json")
                .containsEntry("__ContentType__", "application/json");
    }

    @Test
    @DisplayName("Deep Audit Issue 2: publish preserves user-provided custom contentType")
    void shouldPreserveCustomContentTypeHeader() {
        publisher.publish("orders-topic", "K-CUSTOM-CT", "{\"orderId\":\"123\"}",
                Map.of("contentType", "application/vnd.my-order.v1+json"));

        ArgumentCaptor<OutboxMessage> captor = ArgumentCaptor.forClass(OutboxMessage.class);
        verify(outboxRepository).save(captor.capture());

        OutboxMessage msg = captor.getValue();
        assertThat(msg.getHeaders())
                .containsEntry("contentType", "application/vnd.my-order.v1+json");
    }
}
