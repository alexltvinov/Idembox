package io.github.alexlitvinov.idembox.starter.publisher;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.alexlitvinov.idembox.core.model.OutboxMessage;
import io.github.alexlitvinov.idembox.core.publisher.OutboxPublisher;
import io.github.alexlitvinov.idembox.core.repository.OutboxRepository;
import io.github.alexlitvinov.idembox.starter.config.IdemboxProperties;
import io.github.alexlitvinov.idembox.starter.metrics.IdemboxMetrics;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.TraceContext;
import io.micrometer.tracing.Tracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.util.ClassUtils;
import org.springframework.util.unit.DataSize;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.regex.Pattern;

public class DefaultOutboxPublisher implements OutboxPublisher {

    private static final Logger log = LoggerFactory.getLogger(DefaultOutboxPublisher.class);
    @SuppressWarnings("SpellCheckingInspection")
    private static final String TRACE_PARENT_HEADER = "traceparent";

    public static final Pattern TOPIC_NAME_PATTERN = Pattern.compile("^[a-zA-Z0-9._-]+$");
    public static final int MAX_TOPIC_NAME_LENGTH = 249;

    public static void validateTopic(String topic) {
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("Outbox destination topic cannot be null or blank");
        }
        String trimmed = topic.trim();
        if (!trimmed.equals(topic)) {
            throw new IllegalArgumentException("Outbox destination topic contains leading or trailing whitespace: '" + topic + "'");
        }
        if (topic.length() > MAX_TOPIC_NAME_LENGTH) {
            throw new IllegalArgumentException("Outbox destination topic length (" + topic.length() +
                    ") exceeds maximum Kafka limit of " + MAX_TOPIC_NAME_LENGTH + " characters: '" + topic + "'");
        }
        if (".".equals(topic) || "..".equals(topic)) {
            throw new IllegalArgumentException("Outbox destination topic cannot be '.' or '..'");
        }
        if (!TOPIC_NAME_PATTERN.matcher(topic).matches()) {
            throw new IllegalArgumentException("Outbox destination topic contains invalid characters (allowed: ASCII alphanumerics, '.', '_', '-'): '" + topic + "'");
        }
    }

    private final OutboxRepository outboxRepository;
    private final ObjectMapper objectMapper;
    private final IdemboxProperties properties;
    private final ApplicationEventPublisher eventPublisher;
    private final IdemboxMetrics metrics;
    private final Tracer tracer;

    public DefaultOutboxPublisher(
            OutboxRepository outboxRepository,
            ObjectMapper objectMapper,
            IdemboxProperties properties,
            ApplicationEventPublisher eventPublisher,
            IdemboxMetrics metrics) {
        this(outboxRepository, objectMapper, properties, eventPublisher, metrics, null);
    }

    public DefaultOutboxPublisher(
            OutboxRepository outboxRepository,
            ObjectMapper objectMapper,
            IdemboxProperties properties,
            ApplicationEventPublisher eventPublisher,
            IdemboxMetrics metrics,
            Tracer tracer) {
        this.outboxRepository = outboxRepository;
        this.objectMapper = objectMapper;
        this.properties = properties;
        this.eventPublisher = eventPublisher;
        this.metrics = metrics != null ? metrics : IdemboxMetrics.noOp();
        this.tracer = tracer;
    }

    @Override
    public UUID publish(String topic, String partitionKey, Object payload) {
        return publishWithMultiHeaders(topic, partitionKey, payload, Collections.emptyMap());
    }

    @Override
    public UUID publish(String topic, String partitionKey, Object payload, Map<String, String> headers) {
        return publishWithMultiHeaders(topic, partitionKey, payload, toMultiHeaders(headers));
    }

    @Override
    public UUID publishWithMultiHeaders(String topic, String partitionKey, Object payload, Map<String, List<String>> headers) {
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("Outbox destination topic cannot be null or blank");
        }
        String cleanTopic = sanitizeJsonb(topic);
        validateTopic(cleanTopic);

        if (TransactionSynchronizationManager.isCurrentTransactionReadOnly()) {
            throw new IllegalStateException("Cannot publish outbox message within a read-only transaction (@Transactional(readOnly = true)).");
        }

        Object effectivePayload = unwrapPayload(payload);
        ProcessedPayload processed = processPayload(effectivePayload);
        String jsonPayload = processed.jsonPayload();

        Map<String, List<String>> resolvedHeaders = copyMultiHeaders(headers);
        populatePayloadHeaders(effectivePayload, processed.isJson(), resolvedHeaders);
        validateMessageSizes(effectivePayload, jsonPayload, resolvedHeaders);

        String normalizedPartitionKey = normalizePartitionKey(partitionKey);
        OutboxMessage message = OutboxMessage.createPendingWithMultiHeaders(cleanTopic, normalizedPartitionKey, jsonPayload, resolvedHeaders);
        outboxRepository.save(message);

        log.debug("Enqueued outbox message id={} for topic={} (partitionKey={})", message.getId(), cleanTopic, normalizedPartitionKey);
        recordAndDispatchAfterCommit(cleanTopic, message.getId());
        return message.getId();
    }

    private static Map<String, List<String>> toMultiHeaders(Map<String, String> headers) {
        if (headers == null || headers.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, List<String>> multi = new LinkedHashMap<>(headers.size());
        headers.forEach((key, value) -> {
            if (key != null) {
                multi.put(key, value != null ? List.of(value) : Collections.emptyList());
            }
        });
        return multi;
    }

    private static Object unwrapPayload(Object payload) {
        Object effectivePayload = payload;
        while (effectivePayload instanceof Optional<?> optionalPayload) {
            effectivePayload = optionalPayload.orElse(null);
        }
        if (effectivePayload instanceof CharSequence charSequence && !(effectivePayload instanceof String)) {
            return charSequence.toString();
        }
        if (effectivePayload instanceof ByteBuffer byteBuffer) {
            ByteBuffer slice = byteBuffer.slice();
            byte[] bytes = new byte[slice.remaining()];
            slice.get(bytes);
            return bytes;
        }
        return effectivePayload;
    }

    private static Map<String, List<String>> copyMultiHeaders(Map<String, List<String>> multiHeaders) {
        Map<String, List<String>> resolvedMultiHeaders = new LinkedHashMap<>();
        if (multiHeaders != null) {
            multiHeaders.forEach((key, value) -> {
                if (key != null) {
                    resolvedMultiHeaders.put(key, value != null ? new ArrayList<>(value) : new ArrayList<>());
                }
            });
        }
        return resolvedMultiHeaders;
    }

    private void populatePayloadHeaders(
            Object effectivePayload,
            boolean isJson,
            Map<String, List<String>> headers) {
        if (isJson) {
            headers.putIfAbsent("contentType", List.of("application/json"));
            headers.putIfAbsent("__ContentType__", List.of("application/json"));
        }
        if (properties.getOutbox().isAddTypeIdHeader() && isDomainPojoForTypeId(effectivePayload)) {
            Class<?> userClass = ClassUtils.getUserClass(effectivePayload.getClass());
            String typeName = resolveTypeIdName(userClass);
            headers.putIfAbsent("__TypeId__", List.of(typeName));
        } else if (effectivePayload instanceof byte[]) {
            headers.putIfAbsent("__PayloadType__", List.of("bytes"));
            if (!isJson) {
                headers.putIfAbsent("contentType", List.of("application/octet-stream"));
                headers.putIfAbsent("__ContentType__", List.of("application/octet-stream"));
                headers.putIfAbsent("__Encoding__", List.of("base64"));
            }
        } else if (effectivePayload instanceof String && !isJson) {
            headers.putIfAbsent("contentType", List.of("text/plain"));
            headers.putIfAbsent("__ContentType__", List.of("text/plain"));
        }
        injectTracingHeaders(headers);
    }

    private static String resolveTypeIdName(Class<?> userClass) {
        if (userClass.isAnonymousClass()) {
            if (userClass.getSuperclass() != Object.class) {
                return userClass.getSuperclass().getName();
            }
            if (userClass.getInterfaces().length > 0) {
                return userClass.getInterfaces()[0].getName();
            }
        }
        return userClass.getName();
    }

    private void validateMessageSizes(
            Object effectivePayload,
            String jsonPayload,
            Map<String, List<String>> resolvedMultiHeaders) {
        long payloadBytesLength = effectivePayload instanceof byte[] rawBytes
                ? rawBytes.length
                : (jsonPayload != null ? jsonPayload.getBytes(StandardCharsets.UTF_8).length : 0);
        long headersSizeBytes = calculateHeadersSizeBytes(resolvedMultiHeaders);

        DataSize maxHeaderSize = properties.getOutbox().getMaxHeaderSize();
        validateSizeLimit("headers", headersSizeBytes, maxHeaderSize);

        DataSize maxPayloadSize = properties.getOutbox().getMaxPayloadSize();
        validateSizeLimit("payload", payloadBytesLength, maxPayloadSize);

        long maxTotalBytes = (maxPayloadSize != null ? maxPayloadSize.toBytes() : 0)
                + (maxHeaderSize != null ? maxHeaderSize.toBytes() : 0);
        long totalBytes = payloadBytesLength + headersSizeBytes;
        if (maxTotalBytes > 0 && totalBytes > maxTotalBytes) {
            throw new IllegalArgumentException(String.format(
                    "Outbox total message size of %d bytes (payload: %d bytes, headers: %d bytes) exceeds configured maximum limit of %d bytes.",
                    totalBytes, payloadBytesLength, headersSizeBytes, maxTotalBytes));
        }
    }

    private static void validateSizeLimit(String partName, long actualBytes, DataSize maxLimit) {
        if (maxLimit != null && actualBytes > maxLimit.toBytes()) {
            throw new IllegalArgumentException(String.format(
                    "Outbox %s size of %d bytes exceeds configured maximum limit of %s (%d bytes).",
                    partName, actualBytes, maxLimit, maxLimit.toBytes()));
        }
    }

    private static long calculateHeadersSizeBytes(Map<String, List<String>> resolvedMultiHeaders) {
        long headersSizeBytes = 0;
        for (Map.Entry<String, List<String>> entry : resolvedMultiHeaders.entrySet()) {
            String headerKey = entry.getKey();
            if ("__PayloadType__".equals(headerKey) || "__ContentType__".equals(headerKey) || "__Encoding__".equals(headerKey)) {
                continue;
            }
            if (headerKey != null) {
                headersSizeBytes += headerKey.getBytes(StandardCharsets.UTF_8).length;
            }
            if (entry.getValue() != null) {
                for (String headerValue : entry.getValue()) {
                    if (headerValue != null) {
                        headersSizeBytes += headerValue.getBytes(StandardCharsets.UTF_8).length;
                    }
                }
            }
        }
        return headersSizeBytes;
    }

    private void recordAndDispatchAfterCommit(String cleanTopic, UUID messageId) {
        boolean immediateFlush = properties.getOutbox().isImmediateFlush();
        if (TransactionSynchronizationManager.isActualTransactionActive()
                && TransactionSynchronizationManager.isSynchronizationActive()) {
            try {
                TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                    @Override
                    public void afterCommit() {
                        metrics.recordOutboxEnqueued(cleanTopic);
                        if (immediateFlush) {
                            eventPublisher.publishEvent(new OutboxCommittedEvent(messageId));
                        }
                    }
                });
            } catch (IllegalStateException exception) {
                metrics.recordOutboxEnqueued(cleanTopic);
                if (immediateFlush) {
                    eventPublisher.publishEvent(new OutboxCommittedEvent(messageId));
                }
            }
        } else {
            metrics.recordOutboxEnqueued(cleanTopic);
            if (immediateFlush) {
                log.warn("Outbox message id={} published outside of an active database transaction. Immediate flush dispatched directly.", messageId);
                eventPublisher.publishEvent(new OutboxCommittedEvent(messageId));
            }
        }
    }

    private void injectTracingHeaders(Map<String, List<String>> headers) {
        if (tracer == null || headers.containsKey(TRACE_PARENT_HEADER)) {
            return;
        }
        try {
            Span currentSpan = tracer.currentSpan();
            if (currentSpan != null) {
                TraceContext context = currentSpan.context();
                String normalizedTraceId = normalizeHexId(context.traceId(), 32);
                String normalizedSpanId = normalizeHexId(context.spanId(), 16);
                if (normalizedTraceId != null && normalizedSpanId != null) {
                    String flags = Boolean.parseBoolean(String.valueOf(context.sampled())) ? "01" : "00";
                    headers.put(TRACE_PARENT_HEADER, List.of("00-" + normalizedTraceId + "-" + normalizedSpanId + "-" + flags));
                    if (!headers.containsKey("b3")) {
                        headers.put("b3", List.of(normalizedTraceId + "-" + normalizedSpanId));
                    }
                }
            }
        } catch (Throwable throwable) {
            log.trace("Failed to extract current trace context", throwable);
        }
    }

    private static String normalizeHexId(String rawId, int targetLength) {
        if (rawId == null || rawId.isBlank()) {
            return null;
        }
        // Strip non-hex characters (e.g. AWS X-Ray dashes '1-5759e988-bd862e3c1be46a994272793e')
        String hexOnly = rawId.replaceAll("[^0-9a-fA-F]", "").toLowerCase();
        if (hexOnly.isEmpty()) {
            return null;
        }
        String normalized;
        if (hexOnly.length() < targetLength) {
            normalized = "0".repeat(targetLength - hexOnly.length()) + hexOnly;
        } else if (hexOnly.length() > targetLength) {
            // Standard downcast: take rightmost characters
            normalized = hexOnly.substring(hexOnly.length() - targetLength);
        } else {
            normalized = hexOnly;
        }
        // W3C Trace Context prohibits all-zero trace-id and parent-id
        if (normalized.chars().allMatch(character -> character == '0')) {
            return null;
        }
        return normalized;
    }

    private boolean isValidJson(String str) {
        try (JsonParser parser = objectMapper.getFactory().createParser(str)) {
            return validateSingleRootJson(parser);
        } catch (Exception ignored) {
            return false;
        }
    }

    private boolean validateSingleRootJson(JsonParser parser) throws IOException {
        JsonToken root = parser.nextToken();
        if (root == null) {
            return false;
        }
        if (root == JsonToken.START_OBJECT || root == JsonToken.START_ARRAY) {
            parser.skipChildren();
        }
        return parser.nextToken() == null;
    }

    record ProcessedPayload(String jsonPayload, boolean isJson) {}

    private ProcessedPayload processPayload(Object payload) {
        Object unwrapped = unwrapPayload(payload);
        if (unwrapped == null) {
            return new ProcessedPayload(null, false);
        }

        if (unwrapped instanceof String text) {
            return processStringPayload(text);
        }

        if (unwrapped instanceof byte[] bytes) {
            return processByteArrayPayload(bytes);
        }

        // Note (Senior Refinements v30): Non-JSON strings and scalar objects are serialized as JSON scalars
        // (e.g. "text" -> "\"text\"") to satisfy PostgreSQL JSONB column constraints.
        // For plain text (contentType=text/plain), OutboxPoller automatically unwraps external quotes before Kafka dispatch.
        try {
            return new ProcessedPayload(objectMapper.writeValueAsString(unwrapped), true);
        } catch (JsonProcessingException exception) {
            throw new IllegalArgumentException("Failed to serialize outbox payload to JSON", exception);
        }
    }

    private ProcessedPayload processStringPayload(String text) {
        String sanitized = sanitizeJsonb(text);
        String trimmed = sanitized.trim();
        // If already a valid JSON object or array, store as-is without constructing JsonNode tree
        if ((trimmed.startsWith("{") && trimmed.endsWith("}")) || (trimmed.startsWith("[") && trimmed.endsWith("]"))) {
            if (isValidJson(trimmed)) {
                return new ProcessedPayload(trimmed, true);
            }
        }
        try {
            return new ProcessedPayload(objectMapper.writeValueAsString(text), false);
        } catch (JsonProcessingException exception) {
            throw new IllegalArgumentException("Failed to serialize outbox payload to JSON", exception);
        }
    }

    private ProcessedPayload processByteArrayPayload(byte[] bytes) {
        int first = findFirstNonWhitespace(bytes);
        int last = findLastNonWhitespace(bytes);
        if (first >= 0 && last >= first) {
            byte firstByte = bytes[first];
            byte lastByte = bytes[last];
            if ((firstByte == '{' && lastByte == '}') || (firstByte == '[' && lastByte == ']')) {
                String decoded = new String(bytes, first, last - first + 1, StandardCharsets.UTF_8);
                decoded = sanitizeJsonb(decoded);
                if (isValidJson(decoded)) {
                    return new ProcessedPayload(decoded, true);
                }
            }
        }
        try {
            return new ProcessedPayload(objectMapper.writeValueAsString(bytes), false);
        } catch (JsonProcessingException exception) {
            throw new IllegalArgumentException("Failed to serialize outbox payload to JSON", exception);
        }
    }

    static String normalizePartitionKey(String partitionKey) {
        if (partitionKey == null || partitionKey.isBlank()) {
            return null;
        }
        String sanitized = sanitizeJsonb(partitionKey);
        String trimmed = sanitized.trim();
        if (trimmed.isEmpty()) {
            return null;
        }
        return trimmed;
    }

    static String sanitizeJsonb(String value) {
        if (value == null) {
            return null;
        }
        if (value.contains("\\u0000") || value.indexOf('\0') >= 0) {
            return value.replace("\\u0000", "").replace("\0", "");
        }
        return value;
    }

    static int findFirstNonWhitespace(byte[] bytes) {
        if (bytes == null) {
            return -1;
        }
        for (int index = 0; index < bytes.length; index++) {
            byte current = bytes[index];
            if (current != ' ' && current != '\t' && current != '\r' && current != '\n') {
                return index;
            }
        }
        return -1;
    }

    static int findLastNonWhitespace(byte[] bytes) {
        if (bytes == null) {
            return -1;
        }
        for (int index = bytes.length - 1; index >= 0; index--) {
            byte current = bytes[index];
            if (current != ' ' && current != '\t' && current != '\r' && current != '\n') {
                return index;
            }
        }
        return -1;
    }

    static boolean isDomainPojoForTypeId(Object payload) {
        if (payload == null
                || payload instanceof CharSequence
                || payload instanceof byte[]
                || payload instanceof ByteBuffer
                || payload instanceof Map<?, ?>
                || payload instanceof Iterable<?>
                || payload instanceof Optional<?>
                || payload instanceof Number
                || payload instanceof Boolean
                || payload instanceof UUID
                || payload instanceof Enum<?>
                || payload instanceof TreeNode
                || payload.getClass().isArray()) {
            return false;
        }
        Class<?> userClass = ClassUtils.getUserClass(payload.getClass());
        if (userClass.isArray()) {
            return false;
        }
        String packageName = userClass.getPackageName();
        return !packageName.startsWith("java.") && !packageName.startsWith("javax.") && !packageName.startsWith("jdk.");
    }

    public record OutboxCommittedEvent(UUID messageId) {}
}
