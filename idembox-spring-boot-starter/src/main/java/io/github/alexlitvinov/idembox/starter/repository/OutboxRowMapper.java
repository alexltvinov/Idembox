package io.github.alexlitvinov.idembox.starter.repository;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.alexlitvinov.idembox.core.model.OutboxMessage;
import io.github.alexlitvinov.idembox.core.model.OutboxStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.lang.NonNull;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Maps database rows from the outbox table to {@link OutboxMessage} domain objects,
 * including JSON headers parsing (both single and multivalued).
 */
public record OutboxRowMapper(ObjectMapper mapper) implements RowMapper<OutboxMessage> {

    private static final Logger log = LoggerFactory.getLogger(OutboxRowMapper.class);

    private static String extractJsonNodeText(JsonNode node) {
        if (node == null || node.isNull()) {
            return null;
        }
        if (node.isTextual()) {
            return node.asText();
        }
        if (node.isContainerNode()) {
            return node.toString();
        }
        return node.asText();
    }

    @Override
    public OutboxMessage mapRow(@NonNull ResultSet rs, int rowNum) throws SQLException {
        UUID id = extractId(rs);
        String topic = rs.getString("topic");
        String partitionKey = rs.getString("partition_key");
        String payload = rs.getString("payload");
        String headersRaw = rs.getString("headers");

        Map<String, List<String>> multiHeaders = Collections.emptyMap();
        if (headersRaw != null && !headersRaw.isBlank()) {
            try {
                JsonNode root = mapper.readTree(headersRaw);
                if (root.isArray()) {
                    Map<String, List<String>> multi = new LinkedHashMap<>();
                    for (JsonNode item : root) {
                        String k = item.has("key") ? extractJsonNodeText(item.get("key")) : null;
                        String v = item.has("value") ? extractJsonNodeText(item.get("value")) : null;
                        if (k != null) {
                            if (v != null) {
                                multi.computeIfAbsent(k, key -> new ArrayList<>()).add(v);
                            } else {
                                multi.computeIfAbsent(k, key -> new ArrayList<>());
                            }
                        }
                    }
                    multiHeaders = multi;
                } else if (root.isObject()) {
                    Map<String, List<String>> multi = new LinkedHashMap<>();
                    Iterator<Map.Entry<String, JsonNode>> fields = root.fields();
                    while (fields.hasNext()) {
                        Map.Entry<String, JsonNode> field = fields.next();
                        String k = field.getKey();
                        JsonNode valNode = field.getValue();
                        if (valNode.isArray()) {
                            List<String> list = new ArrayList<>();
                            for (JsonNode element : valNode) {
                                list.add(extractJsonNodeText(element));
                            }
                            multi.put(k, list);
                        } else {
                            String v = extractJsonNodeText(valNode);
                            multi.put(k, v != null ? List.of(v) : Collections.emptyList());
                        }
                    }
                    multiHeaders = multi;
                }
            } catch (JsonProcessingException e) {
                log.error("Failed to parse headers JSON for message id {}", id, e);
            }
        }

        OutboxStatus status = OutboxStatus.valueOf(rs.getString("status"));
        int retryCount = rs.getInt("retry_count");
        OffsetDateTime createdAtOdt = rs.getObject("created_at", OffsetDateTime.class);
        Instant createdAt = createdAtOdt != null ? createdAtOdt.toInstant() : null;
        OffsetDateTime sentAtOdt = rs.getObject("sent_at", OffsetDateTime.class);
        Instant sentAt = sentAtOdt != null ? sentAtOdt.toInstant() : null;
        OffsetDateTime nextRetryOdt = rs.getObject("next_retry_at", OffsetDateTime.class);
        Instant nextRetryAt = nextRetryOdt != null ? nextRetryOdt.toInstant() : null;
        String lastError = rs.getString("last_error");

        OffsetDateTime failedAtOdt = null;
        try {
            failedAtOdt = rs.getObject("failed_at", OffsetDateTime.class);
        } catch (SQLException ignored) {
            // column might not be present in partial mocks
        }
        Instant failedAt = failedAtOdt != null ? failedAtOdt.toInstant() : null;

        return new OutboxMessage(id, topic, partitionKey, payload, multiHeaders, status, retryCount, createdAt, sentAt, nextRetryAt, lastError, failedAt);
    }

    private static UUID extractId(ResultSet rs) throws SQLException {
        Object obj = rs.getObject("id");
        if (obj == null) {
            return null;
        }
        if (obj instanceof UUID u) {
            return u;
        }
        return UUID.fromString(obj.toString());
    }
}
