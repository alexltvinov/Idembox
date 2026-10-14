package io.github.alexlitvinov.idembox.core.model;

import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

/**
 * Represents an Outbox record stored transactionally in PostgreSQL before publishing to Kafka.
 */
public class OutboxMessage {

    private final UUID id;
    private final String topic;
    private final String partitionKey;
    private final String payload;
    private final Map<String, List<String>> multiHeaders;
    private OutboxStatus status;
    private int retryCount;
    private final Instant createdAt;
    private Instant sentAt;
    private Instant nextRetryAt;
    private String lastError;
    private Instant failedAt;

    public OutboxMessage(
            UUID id,
            String topic,
            String partitionKey,
            String payload,
            Map<String, String> headers,
            OutboxStatus status,
            int retryCount,
            Instant createdAt,
            Instant sentAt,
            Instant nextRetryAt,
            String lastError) {
        this(id, topic, partitionKey, payload, headers, null, status, retryCount, createdAt, sentAt, nextRetryAt, lastError, null);
    }

    public OutboxMessage(
            UUID id,
            String topic,
            String partitionKey,
            String payload,
            Map<String, List<String>> multiHeaders,
            OutboxStatus status,
            int retryCount,
            Instant createdAt,
            Instant sentAt,
            Instant nextRetryAt,
            String lastError,
            Instant failedAt) {
        this(id, topic, partitionKey, payload, null, multiHeaders, status, retryCount, createdAt, sentAt, nextRetryAt, lastError, failedAt);
    }

    public OutboxMessage(
            UUID id,
            String topic,
            String partitionKey,
            String payload,
            Map<String, String> headers,
            Map<String, List<String>> multiHeaders,
            OutboxStatus status,
            int retryCount,
            Instant createdAt,
            Instant sentAt,
            Instant nextRetryAt,
            String lastError,
            Instant failedAt) {
        this.id = Objects.requireNonNull(id, "id must not be null");
        this.topic = Objects.requireNonNull(topic, "topic must not be null");
        this.partitionKey = partitionKey;
        this.payload = payload;
        this.multiHeaders = toMultiHeaders(headers, multiHeaders);
        this.status = Objects.requireNonNull(status, "status must not be null");
        this.retryCount = retryCount;
        this.createdAt = Objects.requireNonNull(createdAt, "createdAt must not be null");
        this.sentAt = sentAt;
        this.nextRetryAt = nextRetryAt;
        this.lastError = lastError;
        this.failedAt = failedAt;
    }

    private static Map<String, List<String>> toMultiHeaders(
            Map<String, String> headers,
            Map<String, List<String>> multiHeaders) {
        if ((headers == null || headers.isEmpty()) && (multiHeaders == null || multiHeaders.isEmpty())) {
            return Collections.emptyMap();
        }
        Map<String, List<String>> copy = new LinkedHashMap<>();
        if (multiHeaders != null) {
            multiHeaders.forEach((key, values) -> {
                if (key != null) {
                    copy.put(key, unmodifiableCopy(values));
                }
            });
        }
        if (headers != null) {
            headers.forEach((key, value) -> {
                if (key != null) {
                    List<String> existing = copy.get(key);
                    if (existing == null) {
                        copy.put(key, value != null ? List.of(value) : Collections.emptyList());
                    } else if (existing.isEmpty() && value != null && !value.isEmpty()) {
                        copy.put(key, List.of(value));
                    }
                }
            });
        }
        return Collections.unmodifiableMap(copy);
    }

    private static List<String> unmodifiableCopy(List<String> values) {
        if (values == null || values.isEmpty()) {
            return Collections.emptyList();
        }
        return List.copyOf(values);
    }

    public static OutboxMessage createPending(
            String topic,
            String partitionKey,
            String payload,
            Map<String, String> headers) {
        return createPending(topic, partitionKey, payload, headers, null);
    }

    public static OutboxMessage createPendingWithMultiHeaders(
            String topic,
            String partitionKey,
            String payload,
            Map<String, List<String>> multiHeaders) {
        return createPending(topic, partitionKey, payload, null, multiHeaders);
    }

    public static OutboxMessage createPending(
            String topic,
            String partitionKey,
            String payload,
            Map<String, String> headers,
            Map<String, List<String>> multiHeaders) {
        UuidUtils.TimestampedUuid generated = UuidUtils.generateV7WithTimestamp();
        return new OutboxMessage(
                generated.id(),
                topic,
                partitionKey,
                payload,
                headers,
                multiHeaders,
                OutboxStatus.PENDING,
                0,
                generated.createdAt(),
                null,
                null,
                null,
                null
        );
    }

    public UUID getId() {
        return id;
    }

    public String getTopic() {
        return topic;
    }

    public String getPartitionKey() {
        return partitionKey;
    }

    public String getPayload() {
        return payload;
    }

    public Map<String, String> getHeaders() {
        if (multiHeaders.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<String, String> flat = new LinkedHashMap<>(multiHeaders.size());
        multiHeaders.forEach((key, values) -> {
            if (values != null && !values.isEmpty()) {
                flat.put(key, values.get(0));
            }
        });
        return Collections.unmodifiableMap(flat);
    }

    public Map<String, List<String>> getMultiHeaders() {
        return multiHeaders;
    }

    public OutboxStatus getStatus() {
        return status;
    }

    public void setStatus(OutboxStatus status) {
        this.status = status;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public void incrementRetryCount() {
        this.retryCount++;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getSentAt() {
        return sentAt;
    }

    public void setSentAt(Instant sentAt) {
        this.sentAt = sentAt;
    }

    public Instant getNextRetryAt() {
        return nextRetryAt;
    }

    public void setNextRetryAt(Instant nextRetryAt) {
        this.nextRetryAt = nextRetryAt;
    }

    public String getLastError() {
        return lastError;
    }

    public void setLastError(String lastError) {
        this.lastError = lastError;
    }

    public Instant getFailedAt() {
        return failedAt;
    }

    public void setFailedAt(Instant failedAt) {
        this.failedAt = failedAt;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OutboxMessage that = (OutboxMessage) o;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        return "OutboxMessage{" +
                "id=" + id +
                ", topic='" + topic + '\'' +
                ", partitionKey='" + partitionKey + '\'' +
                ", status=" + status +
                ", retryCount=" + retryCount +
                ", createdAt=" + createdAt +
                ", nextRetryAt=" + nextRetryAt +
                '}';
    }
}
