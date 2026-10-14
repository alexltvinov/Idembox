package io.github.alexlitvinov.idembox.core.publisher;

import java.util.Map;
import java.util.UUID;

/**
 * Public contract for publishing transactional events to Kafka via the Outbox pattern.
 */
public interface OutboxPublisher {

    /**
     * Publishes a message using the outbox pattern within current database transaction.
     *
     * @param topic Target Kafka topic
     * @param partitionKey Kafka message key (can be null)
     * @param payload Payload object to serialize as JSON
     * @return Generated outbox message UUID
     */
    UUID publish(String topic, String partitionKey, Object payload);

    /**
     * Publishes a message with custom headers.
     *
     * @param topic Target Kafka topic
     * @param partitionKey Kafka message key (can be null)
     * @param payload Payload object to serialize as JSON
     * @param headers Custom key-value headers
     * @return Generated outbox message UUID
     */
    UUID publish(String topic, String partitionKey, Object payload, Map<String, String> headers);

    /**
     * Publishes a message with an explicit multi-tenant / domain namespace prefix.
     * The effective partition key stored in PostgreSQL will be formatted as {@code namespace + ":" + partitionKey}
     * (or {@code partitionKey} if namespace is null or blank), preventing Head-of-Line blocking between tenants.
     *
     * @param namespace Tenant or domain namespace (e.g. "tenant-acme", "billing")
     * @param topic Target Kafka topic
     * @param partitionKey Kafka message key
     * @param payload Payload object to serialize as JSON
     * @return Generated outbox message UUID
     */
    default UUID publishWithNamespace(String namespace, String topic, String partitionKey, Object payload) {
        return publishWithNamespace(namespace, topic, partitionKey, payload, null);
    }

    /**
     * Publishes a message with an explicit multi-tenant / domain namespace prefix and custom headers.
     * The effective partition key stored in PostgreSQL will be formatted as {@code namespace + ":" + partitionKey}
     * (or {@code partitionKey} if namespace is null or blank), preventing Head-of-Line blocking between tenants.
     *
     * @param namespace Tenant or domain namespace (e.g. "tenant-acme", "billing")
     * @param topic Target Kafka topic
     * @param partitionKey Kafka message key
     * @param payload Payload object to serialize as JSON
     * @param headers Custom key-value headers
     * @return Generated outbox message UUID
     */
    default UUID publishWithNamespace(String namespace, String topic, String partitionKey, Object payload, Map<String, String> headers) {
        String trimmedNamespace = (namespace != null && !namespace.isBlank()) ? namespace.trim() : null;
        String trimmedKey = (partitionKey != null && !partitionKey.isBlank()) ? partitionKey.trim() : null;
        String effectiveKey = trimmedNamespace != null
                ? (trimmedKey != null ? trimmedNamespace + ":" + trimmedKey : trimmedNamespace)
                : trimmedKey;
        return publish(topic, effectiveKey, payload, headers);
    }

    /**
     * Publishes a message with custom multi-value headers.
     *
     * @param topic Target Kafka topic
     * @param partitionKey Kafka message key (can be null)
     * @param payload Payload object to serialize as JSON
     * @param headers Custom multi-value headers
     * @return Generated outbox message UUID
     */
    default UUID publishWithMultiHeaders(String topic, String partitionKey, Object payload, Map<String, java.util.List<String>> headers) {
        Map<String, String> flat = new java.util.HashMap<>();
        if (headers != null) {
            headers.forEach((k, v) -> {
                if (k != null && v != null && !v.isEmpty()) {
                    flat.put(k, v.get(0));
                }
            });
        }
        return publish(topic, partitionKey, payload, flat);
    }
}
