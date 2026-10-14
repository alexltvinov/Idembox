package io.github.alexlitvinov.idembox.core.model;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OutboxMessageTest {

    @BeforeEach
    @AfterEach
    void resetUuidUtilsState() {
        UuidUtils.resetForTesting();
    }

    @Test
    @DisplayName("createPending should construct valid pending message with defaults")
    void createPendingShouldInitializeDefaults() {
        Map<String, String> headers = Map.of("source", "checkout-service", "version", "1.0");

        OutboxMessage msg = OutboxMessage.createPending("orders-topic", "ORD-123", "{\"id\":123}", headers);

        assertThat(msg.getId()).isNotNull();
        assertThat(msg.getTopic()).isEqualTo("orders-topic");
        assertThat(msg.getPartitionKey()).isEqualTo("ORD-123");
        assertThat(msg.getPayload()).isEqualTo("{\"id\":123}");
        assertThat(msg.getStatus()).isEqualTo(OutboxStatus.PENDING);
        assertThat(msg.getRetryCount()).isEqualTo(0);
        assertThat(msg.getCreatedAt()).isNotNull().isBeforeOrEqualTo(Instant.now());
        assertThat(msg.getSentAt()).isNull();
        assertThat(msg.getNextRetryAt()).isNull();
        assertThat(msg.getLastError()).isNull();
        assertThat(msg.getFailedAt()).isNull();
        assertThat(msg.getHeaders()).containsEntry("source", "checkout-service").containsEntry("version", "1.0");
    }

    @Test
    @DisplayName("Constructor should enforce non-null invariants for mandatory fields")
    void constructorShouldEnforceNonNullInvariants() {
        UUID id = UUID.randomUUID();
        Instant now = Instant.now();

        assertThatThrownBy(() -> new OutboxMessage(null, "t", "k", "p", null, OutboxStatus.PENDING, 0, now, null, null, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("id must not be null");

        assertThatThrownBy(() -> new OutboxMessage(id, null, "k", "p", null, OutboxStatus.PENDING, 0, now, null, null, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("topic must not be null");

        assertThatThrownBy(() -> new OutboxMessage(id, "t", "k", "p", null, null, 0, now, null, null, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("status must not be null");

        assertThatThrownBy(() -> new OutboxMessage(id, "t", "k", "p", null, OutboxStatus.PENDING, 0, null, null, null, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("createdAt must not be null");
    }

    @Test
    @DisplayName("Headers should be defensively copied and unmodifiable")
    void headersShouldBeDefensivelyCopiedAndUnmodifiable() {
        Map<String, String> mutableHeaders = new HashMap<>();
        mutableHeaders.put("k1", "v1");

        OutboxMessage msg = OutboxMessage.createPending("topic", "key", "payload", mutableHeaders);

        // Mutating external map must not alter message headers
        mutableHeaders.put("k1", "mutated");
        mutableHeaders.put("k2", "v2");
        assertThat(msg.getHeaders()).hasSize(1);
        assertThat(msg.getHeaders().get("k1")).isEqualTo("v1");

        // Mutating returned headers must throw UnsupportedOperationException
        assertThatThrownBy(() -> msg.getHeaders().put("k3", "v3"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    @DisplayName("Constructor with null headers should initialize empty unmodifiable map")
    void constructorWithNullHeadersShouldInitializeEmptyMap() {
        OutboxMessage msg = OutboxMessage.createPending("topic", "key", "payload", null);

        assertThat(msg.getHeaders()).isNotNull().isEmpty();
        assertThatThrownBy(() -> msg.getHeaders().put("k", "v"))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    @DisplayName("Mutators should update state correctly")
    void mutatorsShouldUpdateState() {
        OutboxMessage msg = OutboxMessage.createPending("topic", "key", "payload", null);

        msg.setStatus(OutboxStatus.IN_PROGRESS);
        assertThat(msg.getStatus()).isEqualTo(OutboxStatus.IN_PROGRESS);

        msg.incrementRetryCount();
        assertThat(msg.getRetryCount()).isEqualTo(1);
        msg.incrementRetryCount();
        assertThat(msg.getRetryCount()).isEqualTo(2);

        Instant sentTime = Instant.now();
        msg.setSentAt(sentTime);
        assertThat(msg.getSentAt()).isEqualTo(sentTime);

        Instant retryTime = Instant.now().plusSeconds(60);
        msg.setNextRetryAt(retryTime);
        assertThat(msg.getNextRetryAt()).isEqualTo(retryTime);

        msg.setLastError("Connection timed out");
        assertThat(msg.getLastError()).isEqualTo("Connection timed out");

        Instant failedTime = Instant.now();
        msg.setFailedAt(failedTime);
        assertThat(msg.getFailedAt()).isEqualTo(failedTime);
    }

    @Test
    @DisplayName("equals and hashCode should be identity-based on UUID id")
    void equalsAndHashCodeShouldBeIdentityBasedOnId() {
        UUID id1 = UUID.randomUUID();
        UUID id2 = UUID.randomUUID();
        Instant now = Instant.now();

        OutboxMessage msg1 = new OutboxMessage(id1, "topic1", "k1", "p1", null, OutboxStatus.PENDING, 0, now, null, null, null);
        OutboxMessage msg1DiffState = new OutboxMessage(id1, "topic2", "k2", "p2", null, OutboxStatus.SENT, 5, now, now, null, "err");
        OutboxMessage msg2 = new OutboxMessage(id2, "topic1", "k1", "p1", null, OutboxStatus.PENDING, 0, now, null, null, null);

        assertThat(msg1).isEqualTo(msg1DiffState);
        assertThat(msg1.hashCode()).isEqualTo(msg1DiffState.hashCode());

        assertThat(msg1).isNotEqualTo(msg2);
        assertThat(msg1).isNotEqualTo(null);
        assertThat(msg1).isNotEqualTo(new Object());
    }

    @Test
    @DisplayName("toString should contain key identifiers and state")
    void toStringShouldContainKeyIdentifiers() {
        UUID id = UUID.randomUUID();
        OutboxMessage msg = new OutboxMessage(id, "orders-topic", "ORD-1", "{}", null, OutboxStatus.PENDING, 0, Instant.now(), null, null, null);

        String str = msg.toString();
        assertThat(str)
                .contains(id.toString())
                .contains("orders-topic")
                .contains("ORD-1")
                .contains("PENDING");
    }

    @Test
    @DisplayName("createPending should generate monotonic UUID v7 IDs preserving causality")
    void createPendingShouldGenerateMonotonicUuidV7() {
        OutboxMessage m1 = OutboxMessage.createPending("topic", "key", "p1", null);
        OutboxMessage m2 = OutboxMessage.createPending("topic", "key", "p2", null);

        assertThat(m1.getId().version()).isEqualTo(7);
        assertThat(m2.getId().version()).isEqualTo(7);
        assertThat(m1.getId().compareTo(m2.getId())).isLessThan(0);
    }

    @Test
    @DisplayName("OutboxMessage should preserve multiHeaders and expose flat headers for compatibility")
    void shouldSupportMultiHeadersAndFlatHeaders() {
        Map<String, List<String>> multi = Map.of(
                "X-Trace", List.of("step-1", "step-2"),
                "Content-Type", List.of("application/json")
        );

        OutboxMessage msg = OutboxMessage.createPending("topic", "key", "{}", null, multi);

        assertThat(msg.getMultiHeaders()).containsKey("X-Trace");
        assertThat(msg.getMultiHeaders().get("X-Trace")).containsExactly("step-1", "step-2");
        assertThat(msg.getHeaders()).containsEntry("Content-Type", "application/json");
        assertThat(msg.getHeaders().get("X-Trace")).isEqualTo("step-1");

        OutboxMessage msgDirect = OutboxMessage.createPendingWithMultiHeaders("topic", "key", "{}", multi);
        assertThat(msgDirect.getMultiHeaders()).containsKey("X-Trace");
        assertThat(msgDirect.getHeaders().get("X-Trace")).isEqualTo("step-1");
    }

    @Test
    @DisplayName("Senior Refinements Round 72: Concurrent createPending calls never invert (createdAt, id) order relative to id order")
    void concurrentCreatePendingShouldNeverInvertCreatedAtAndIdOrdering() throws InterruptedException {
        int threads = 8;
        int perThread = 250;
        CountDownLatch startLatch = new CountDownLatch(1);
        List<OutboxMessage> allMessages = Collections.synchronizedList(new ArrayList<>(threads * perThread));

        List<Thread> workers = new ArrayList<>(threads);
        for (int t = 0; t < threads; t++) {
            Thread worker = new Thread(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < perThread; i++) {
                        allMessages.add(OutboxMessage.createPending("orders", "partition-1", "{}", null));
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            worker.start();
            workers.add(worker);
        }
        startLatch.countDown();
        for (Thread worker : workers) {
            worker.join(10_000);
        }

        List<OutboxMessage> sortedById = new ArrayList<>(allMessages);
        sortedById.sort(Comparator.comparing(OutboxMessage::getId));

        List<OutboxMessage> sortedByCreatedAtThenId = new ArrayList<>(allMessages);
        sortedByCreatedAtThenId.sort(Comparator.comparing(OutboxMessage::getCreatedAt).thenComparing(OutboxMessage::getId));

        assertThat(sortedByCreatedAtThenId).containsExactlyElementsOf(sortedById);
    }
}
