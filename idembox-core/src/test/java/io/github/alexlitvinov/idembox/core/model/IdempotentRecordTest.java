package io.github.alexlitvinov.idembox.core.model;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IdempotentRecordTest {

    @Test
    @DisplayName("inFlight factory should initialize record in PROCESSING status with timestamps")
    void inFlightShouldInitializeDefaults() {
        Instant lockedUntil = Instant.now().plusSeconds(60);
        Instant expiresAt = Instant.now().plusSeconds(604800);

        IdempotentRecord record = IdempotentRecord.inFlight("PAY-999", "payment-group", lockedUntil, expiresAt);

        assertThat(record.getEventKey()).isEqualTo("PAY-999");
        assertThat(record.getConsumerGroup()).isEqualTo("payment-group");
        assertThat(record.getStatus()).isEqualTo(ProcessingStatus.PROCESSING);
        assertThat(record.getCreatedAt()).isNotNull().isBeforeOrEqualTo(Instant.now());
        assertThat(record.getCompletedAt()).isNull();
        assertThat(record.getLockedUntil()).isEqualTo(lockedUntil);
        assertThat(record.getExpiresAt()).isEqualTo(expiresAt);
    }

    @Test
    @DisplayName("Constructor should enforce non-null invariants for mandatory fields")
    void constructorShouldEnforceNonNullInvariants() {
        Instant now = Instant.now();

        assertThatThrownBy(() -> new IdempotentRecord(null, "grp", ProcessingStatus.PROCESSING, now, null, null, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("eventKey must not be null");

        assertThatThrownBy(() -> new IdempotentRecord("key", null, ProcessingStatus.PROCESSING, now, null, null, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("consumerGroup must not be null");

        assertThatThrownBy(() -> new IdempotentRecord("key", "grp", null, now, null, null, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("status must not be null");

        assertThatThrownBy(() -> new IdempotentRecord("key", "grp", ProcessingStatus.PROCESSING, null, null, null, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("createdAt must not be null");
    }

    @Test
    @DisplayName("Mutators should update status and lifecycle timestamps")
    void mutatorsShouldUpdateLifecycle() {
        Instant now = Instant.now();
        IdempotentRecord record = new IdempotentRecord("key-1", "group-1", ProcessingStatus.PROCESSING, now, null, null, null);

        record.setStatus(ProcessingStatus.COMPLETED);
        assertThat(record.getStatus()).isEqualTo(ProcessingStatus.COMPLETED);

        Instant completedTime = Instant.now();
        record.setCompletedAt(completedTime);
        assertThat(record.getCompletedAt()).isEqualTo(completedTime);

        Instant lockTime = Instant.now().plusSeconds(30);
        record.setLockedUntil(lockTime);
        assertThat(record.getLockedUntil()).isEqualTo(lockTime);

        Instant expiryTime = Instant.now().plusSeconds(86400);
        record.setExpiresAt(expiryTime);
        assertThat(record.getExpiresAt()).isEqualTo(expiryTime);
    }

    @Test
    @DisplayName("equals and hashCode should be compound based on eventKey and consumerGroup")
    void equalsAndHashCodeShouldBeCompound() {
        Instant now = Instant.now();
        IdempotentRecord rec1 = new IdempotentRecord("k1", "g1", ProcessingStatus.PROCESSING, now, null, null, null);
        IdempotentRecord rec1DiffStatus = new IdempotentRecord("k1", "g1", ProcessingStatus.COMPLETED, now, now, null, now.plusSeconds(3600));
        IdempotentRecord rec2DiffGroup = new IdempotentRecord("k1", "g2", ProcessingStatus.PROCESSING, now, null, null, null);
        IdempotentRecord rec3DiffKey = new IdempotentRecord("k2", "g1", ProcessingStatus.PROCESSING, now, null, null, null);

        assertThat(rec1).isEqualTo(rec1DiffStatus);
        assertThat(rec1.hashCode()).isEqualTo(rec1DiffStatus.hashCode());

        assertThat(rec1).isNotEqualTo(rec2DiffGroup);
        assertThat(rec1).isNotEqualTo(rec3DiffKey);
        assertThat(rec1).isNotEqualTo(null);
        assertThat(rec1).isNotEqualTo(new Object());
    }
}
