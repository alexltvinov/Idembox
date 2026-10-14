package io.github.alexlitvinov.idembox.starter.poller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.github.alexlitvinov.idembox.core.model.OutboxMessage;
import io.github.alexlitvinov.idembox.starter.config.IdemboxProperties;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * Component responsible for mapping an {@link OutboxMessage} to an Apache Kafka {@link ProducerRecord}.
 * Handles payload decoding (JSON, Base64, text unquoting) and Kafka record headers forwarding.
 */
public class OutboxMessageRecordMapper {

    private static final Logger log = LoggerFactory.getLogger(OutboxMessageRecordMapper.class);
    private static final OutboxMessageRecordMapper DEFAULT_MAPPER = new OutboxMessageRecordMapper();

    private final ObjectMapper objectMapper;

    public OutboxMessageRecordMapper() {
        this(null);
    }

    public OutboxMessageRecordMapper(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper != null ? objectMapper : new ObjectMapper()
                .findAndRegisterModules()
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    }

    /**
     * Converts an {@link OutboxMessage} into a Kafka {@link ProducerRecord}.
     *
     * @param msg the outbox message to convert
     * @param properties configuration properties for outbox behavior
     * @return populated Kafka ProducerRecord
     */
    public ProducerRecord<String, Object> toProducerRecord(OutboxMessage msg, IdemboxProperties properties) {
        Long timestamp = (properties != null && properties.getOutbox().isPreserveCreationTimestamp() && msg.getCreatedAt() != null)
                ? msg.getCreatedAt().toEpochMilli()
                : null;
        String rawKey = msg.getPartitionKey();
        String partitionKey = (rawKey != null && !rawKey.isBlank()) ? rawKey : null;
        ProducerRecord<String, Object> record = new ProducerRecord<>(
                msg.getTopic(),
                null,
                timestamp,
                partitionKey,
                resolvePayload(msg)
        );

        boolean hasCustomCreatedAt = false;
        boolean hasCustomOutboxId = false;
        boolean hasCustomIdempotencyKey = false;
        Map<String, List<String>> multiHeaders = msg.getMultiHeaders();
        if (multiHeaders != null && !multiHeaders.isEmpty()) {
            for (String k : multiHeaders.keySet()) {
                if (k != null) {
                    String trimmed = k.trim();
                    if (trimmed.equalsIgnoreCase("X-Created-At")) {
                        hasCustomCreatedAt = true;
                    }
                    if (trimmed.equalsIgnoreCase("X-Outbox-Id")) {
                        hasCustomOutboxId = true;
                    }
                    if (trimmed.equalsIgnoreCase("X-Idempotency-Key")) {
                        hasCustomIdempotencyKey = true;
                    }
                }
            }
        }

        if (!hasCustomCreatedAt && msg.getCreatedAt() != null) {
            record.headers().add(new RecordHeader("X-Created-At", String.valueOf(msg.getCreatedAt().toEpochMilli()).getBytes(StandardCharsets.UTF_8)));
        }

        if (msg.getId() != null) {
            byte[] idBytes = msg.getId().toString().getBytes(StandardCharsets.UTF_8);
            if (!hasCustomOutboxId) {
                record.headers().add(new RecordHeader("X-Outbox-Id", idBytes));
            }
            if (!hasCustomIdempotencyKey) {
                record.headers().add(new RecordHeader("X-Idempotency-Key", idBytes));
            }
        }

        if (multiHeaders != null && !multiHeaders.isEmpty()) {
            multiHeaders.forEach((k, values) -> {
                if (k != null && !k.isBlank()) {
                    String trimmedKey = k.trim();
                    if (isForwardableHeader(trimmedKey)) {
                        if (values == null || values.isEmpty()) {
                            record.headers().add(new RecordHeader(trimmedKey, null));
                        } else {
                            for (String v : values) {
                                byte[] valBytes = (v != null) ? v.getBytes(StandardCharsets.UTF_8) : null;
                                record.headers().add(new RecordHeader(trimmedKey, valBytes));
                            }
                        }
                    }
                }
            });
        }
        return record;
    }

    /**
     * Resolves and decodes the message payload suitable for Kafka serializers.
     */
    public Object resolvePayload(OutboxMessage msg) {
        String payload = msg.getPayload();
        if (payload == null) {
            return null;
        }
        Map<String, List<String>> multiHeaders = msg.getMultiHeaders();
        if (multiHeaders == null || multiHeaders.isEmpty()) {
            return payload;
        }

        String payloadType = findHeaderValue(multiHeaders, "__PayloadType__", "payloadType");
        String encoding = findHeaderValue(multiHeaders, "__Encoding__", "encoding");
        String internalContentType = findHeaderValue(multiHeaders, "__ContentType__", null);
        String contentType = findHeaderValue(multiHeaders, "contentType", "content-type");
        if (contentType == null) {
            contentType = internalContentType;
        }

        if ("base64".equalsIgnoreCase(encoding)) {
            return decodeBase64Payload(msg, payload, payloadType);
        }
        if ("bytes".equalsIgnoreCase(payloadType)) {
            return payload.getBytes(StandardCharsets.UTF_8);
        }
        if (isTextContentType(contentType, internalContentType)) {
            return unquoteTextPayload(payload);
        }
        return payload;
    }

    public static Object resolvePayloadForKafka(OutboxMessage msg) {
        return DEFAULT_MAPPER.resolvePayload(msg);
    }

    private static String findHeaderValue(
            Map<String, List<String>> multiHeaders,
            String primaryKey,
            String secondaryKey) {
        if (multiHeaders != null) {
            List<String> values = multiHeaders.get(primaryKey);
            if (values == null && secondaryKey != null) {
                values = multiHeaders.get(secondaryKey);
            }
            if (values != null && !values.isEmpty()) {
                return values.get(0);
            }
        }
        return null;
    }

    private Object decodeBase64Payload(OutboxMessage msg, String payload, String payloadType) {
        String trimmed = payload.trim();
        String base64String = trimmed;
        if (trimmed.length() >= 2 && trimmed.startsWith("\"") && trimmed.endsWith("\"")) {
            try {
                base64String = this.objectMapper.readValue(trimmed, String.class);
            } catch (Exception e) {
                base64String = trimmed.substring(1, trimmed.length() - 1);
            }
        }
        try {
            return java.util.Base64.getDecoder().decode(base64String.trim());
        } catch (IllegalArgumentException e) {
            log.warn("Failed to decode Base64 payload for message id={}: {}", msg.getId(), e.getMessage());
            if ("bytes".equalsIgnoreCase(payloadType)) {
                return base64String.getBytes(StandardCharsets.UTF_8);
            }
            return base64String;
        }
    }

    private static boolean isTextContentType(String contentType, String internalContentType) {
        String lowerContentType = contentType != null ? contentType.toLowerCase(java.util.Locale.ROOT) : "";
        String lowerInternalContentType = internalContentType != null ? internalContentType.toLowerCase(java.util.Locale.ROOT) : "";
        return lowerContentType.startsWith("text/")
                || lowerContentType.contains("xml")
                || lowerContentType.contains("yaml")
                || lowerContentType.contains("csv")
                || lowerContentType.startsWith("application/text")
                || lowerInternalContentType.startsWith("text/");
    }

    private Object unquoteTextPayload(String payload) {
        String trimmed = payload.trim();
        if (trimmed.length() >= 2 && trimmed.startsWith("\"") && trimmed.endsWith("\"")) {
            try {
                return this.objectMapper.readValue(trimmed, String.class);
            } catch (Exception e) {
                return trimmed.substring(1, trimmed.length() - 1)
                        .replace("\\\"", "\"")
                        .replace("\\\\", "\\")
                        .replace("\\n", "\n")
                        .replace("\\r", "\r")
                        .replace("\\t", "\t");
            }
        }
        return payload;
    }

    private static boolean isForwardableHeader(String key) {
        return !"__PayloadType__".equals(key)
                && !"__ContentType__".equals(key)
                && !"__Encoding__".equals(key);
    }
}
