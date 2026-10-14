package io.github.alexlitvinov.idembox.starter.poller;

import io.github.alexlitvinov.idembox.core.model.OutboxMessage;
import io.github.alexlitvinov.idembox.core.model.OutboxStatus;
import io.github.alexlitvinov.idembox.starter.config.IdemboxProperties;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class OutboxMessageRecordMapperTest {

    private final OutboxMessageRecordMapper mapper = new OutboxMessageRecordMapper();
    private final IdemboxProperties properties = new IdemboxProperties();

    @Test
    @DisplayName("Issue 3: toProducerRecord propagates X-Outbox-Id and X-Idempotency-Key headers from msg.getId()")
    void shouldPropagateOutboxIdAndIdempotencyKeyHeaders() {
        UUID id = UUID.randomUUID();
        Instant now = Instant.now();
        OutboxMessage msg = new OutboxMessage(
                id,
                "orders",
                "order-123",
                "{\"amount\":100}",
                Collections.emptyMap(),
                OutboxStatus.PENDING,
                0,
                now,
                null,
                null,
                null
        );

        ProducerRecord<String, Object> record = mapper.toProducerRecord(msg, properties);

        assertThat(record.topic()).isEqualTo("orders");
        assertThat(record.key()).isEqualTo("order-123");

        Header outboxIdHeader = record.headers().lastHeader("X-Outbox-Id");
        assertThat(outboxIdHeader).isNotNull();
        assertThat(new String(outboxIdHeader.value(), StandardCharsets.UTF_8)).isEqualTo(id.toString());

        Header idempotencyKeyHeader = record.headers().lastHeader("X-Idempotency-Key");
        assertThat(idempotencyKeyHeader).isNotNull();
        assertThat(new String(idempotencyKeyHeader.value(), StandardCharsets.UTF_8)).isEqualTo(id.toString());

        Header createdAtHeader = record.headers().lastHeader("X-Created-At");
        assertThat(createdAtHeader).isNotNull();
        assertThat(new String(createdAtHeader.value(), StandardCharsets.UTF_8)).isEqualTo(String.valueOf(now.toEpochMilli()));
    }

    @Test
    @DisplayName("Issue 3: toProducerRecord respects custom X-Outbox-Id and X-Idempotency-Key if present in headers")
    void shouldPreserveCustomHeadersWhenPresent() {
        UUID id = UUID.randomUUID();
        Instant now = Instant.now();
        OutboxMessage msg = new OutboxMessage(
                id,
                "orders",
                "order-123",
                "{\"amount\":100}",
                Map.of(
                        "X-Outbox-Id", "custom-outbox-uuid",
                        "X-Idempotency-Key", "custom-idem-key"
                ),
                OutboxStatus.PENDING,
                0,
                now,
                null,
                null,
                null
        );

        ProducerRecord<String, Object> record = mapper.toProducerRecord(msg, properties);

        Header outboxIdHeader = record.headers().lastHeader("X-Outbox-Id");
        assertThat(outboxIdHeader).isNotNull();
        assertThat(new String(outboxIdHeader.value(), StandardCharsets.UTF_8)).isEqualTo("custom-outbox-uuid");

        Header idempotencyKeyHeader = record.headers().lastHeader("X-Idempotency-Key");
        assertThat(idempotencyKeyHeader).isNotNull();
        assertThat(new String(idempotencyKeyHeader.value(), StandardCharsets.UTF_8)).isEqualTo("custom-idem-key");
    }
}
