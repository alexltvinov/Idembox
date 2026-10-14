package io.github.alexlitvinov.idembox.starter.test;

import io.github.alexlitvinov.idembox.core.model.OutboxMessage;
import io.github.alexlitvinov.idembox.core.repository.OutboxRepository;
import io.github.alexlitvinov.idembox.starter.test.service.TransactionalOrderService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PostgresOutboxIntegrationTest extends AbstractIntegrationTest {

    @Autowired
    private TransactionalOrderService orderService;

    @Autowired
    private OutboxRepository outboxRepository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private TransactionTemplate transactionTemplate;

    @BeforeEach
    void cleanUp() {
        if (postgres != null && postgres.isRunning()) {
            jdbcTemplate.execute("DELETE FROM outbox_messages");
            if (outboxRepository instanceof io.github.alexlitvinov.idembox.starter.repository.PostgresOutboxRepository pgRepo) {
                pgRepo.invalidatePendingCountCache();
            }
        }
    }

    @Test
    @DisplayName("Dual-write: Outbox message is committed when business transaction succeeds")
    void shouldPersistOutboxMessageWhenTransactionCommits() {
        String orderId = "ORD-" + UUID.randomUUID().toString().substring(0, 6);

        UUID messageId = orderService.placeOrderSuccessfully(orderId);
        assertThat(messageId).isNotNull();

        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM outbox_messages WHERE id = ?",
                Integer.class,
                messageId
        );
        assertThat(count).isEqualTo(1);

        String status = jdbcTemplate.queryForObject(
                "SELECT status FROM outbox_messages WHERE id = ?",
                String.class,
                messageId
        );
        assertThat(status).isEqualTo("PENDING");
    }

    @Test
    @DisplayName("Dual-write: Outbox message is rolled back when business transaction fails")
    void shouldRollbackOutboxMessageWhenTransactionRollsBack() {
        String orderId = "ORD-" + UUID.randomUUID().toString().substring(0, 6);

        assertThatThrownBy(() -> orderService.placeOrderWithRollback(orderId))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("transaction must rollback");

        Integer count = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM outbox_messages WHERE partition_key = ?",
                Integer.class,
                orderId
        );
        assertThat(count).isEqualTo(0);
    }

    @Test
    @DisplayName("SKIP LOCKED: Concurrent workers must fetch disjoint message batches with zero overlap")
    void shouldFetchWithSkipLockedAcrossMultipleWorkers() throws InterruptedException {
        // Insert 20 pending messages
        for (int i = 0; i < 20; i++) {
            outboxRepository.save(OutboxMessage.createPending(
                    "test-topic",
                    "key-" + i,
                    "{\"data\":" + i + "}",
                    Collections.emptyMap()
            ));
        }

        Set<UUID> worker1Ids = new HashSet<>();
        Set<UUID> worker2Ids = new HashSet<>();

        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch worker1HoldLatch = new CountDownLatch(1);
        CountDownLatch finishLatch = new CountDownLatch(2);

        // Worker 1 holds a transaction open with 10 rows locked
        Thread worker1 = new Thread(() -> {
            try {
                startLatch.await();
                transactionTemplate.execute(status -> {
                    List<OutboxMessage> batch = outboxRepository.claimBatchForProcessing(10, java.time.Duration.ofMinutes(1));
                    batch.forEach(m -> worker1Ids.add(m.getId()));
                    worker1HoldLatch.countDown(); // notify worker 2 to query
                    try {
                        Thread.sleep(500); // hold lock temporarily
                    } catch (InterruptedException ignored) {}
                    return null;
                });
            } catch (Exception ignored) {
            } finally {
                finishLatch.countDown();
            }
        }, "outbox-test-worker-1");

        // Worker 2 queries while worker 1 still holds locks -> SKIP LOCKED should give non-overlapping batch!
        Thread worker2 = new Thread(() -> {
            try {
                startLatch.await();
                worker1HoldLatch.await(); // wait until worker 1 acquired its locks
                transactionTemplate.execute(status -> {
                    List<OutboxMessage> batch = outboxRepository.claimBatchForProcessing(10, java.time.Duration.ofMinutes(1));
                    batch.forEach(m -> worker2Ids.add(m.getId()));
                    return null;
                });
            } catch (Exception ignored) {
            } finally {
                finishLatch.countDown();
            }
        }, "outbox-test-worker-2");

        worker1.start();
        worker2.start();

        startLatch.countDown();
        boolean finished = finishLatch.await(5, TimeUnit.SECONDS);

        assertThat(finished).isTrue();
        assertThat(worker1Ids).hasSize(10);
        assertThat(worker2Ids).hasSize(10);

        // Assert 0 overlap between the two worker batches!
        Set<UUID> intersection = new HashSet<>(worker1Ids);
        intersection.retainAll(worker2Ids);
        assertThat(intersection)
                .as("Worker 1 and Worker 2 must have zero overlapping messages due to SKIP LOCKED")
                .isEmpty();
    }

    @Test
    @DisplayName("Fix 3 & 8: Tombstone with null payload persists in Postgres and getPendingMessagesCount counts pending messages")
    void shouldPersistTombstoneAndCountPendingMessages() {
        if (outboxRepository instanceof io.github.alexlitvinov.idembox.starter.repository.PostgresOutboxRepository pgRepo) {
            pgRepo.invalidatePendingCountCache();
        }
        assertThat(outboxRepository.getPendingMessagesCount()).isEqualTo(0);

        // Save a regular message
        outboxRepository.save(OutboxMessage.createPending("topic-a", "key-1", "{\"val\":1}", Collections.emptyMap()));

        // Save a tombstone message (null payload)
        OutboxMessage tombstone = OutboxMessage.createPending("topic-a", "key-2", null, Collections.emptyMap());
        outboxRepository.save(tombstone);

        // Verify pending count is 2
        assertThat(outboxRepository.getPendingMessagesCount()).isEqualTo(2);

        // Verify tombstone payload in DB is genuinely null
        List<String> payloads = jdbcTemplate.query(
                "SELECT payload FROM outbox_messages WHERE id = ?",
                (rs, rowNum) -> rs.getString("payload"),
                tombstone.getId()
        );
        assertThat(payloads).hasSize(1);
        assertThat(payloads.get(0)).isNull();
    }
}
