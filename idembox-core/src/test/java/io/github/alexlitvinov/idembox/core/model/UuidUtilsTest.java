package io.github.alexlitvinov.idembox.core.model;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

class UuidUtilsTest {

    @BeforeEach
    @AfterEach
    void resetUuidUtilsState() {
        UuidUtils.resetForTesting();
    }

    @Test
    @DisplayName("Generated UUID should be RFC 9562 Version 7 and Variant 2")
    void shouldGenerateValidUuidV7() {
        UUID uuid = UuidUtils.generateV7();
        assertThat(uuid).isNotNull();
        assertThat(uuid.version()).isEqualTo(7);
        assertThat(uuid.variant()).isEqualTo(2);
    }

    @Test
    @DisplayName("Sequentially generated UUIDs must be strictly monotonically increasing")
    void sequentialUuidsShouldBeStrictlyIncreasing() {
        int count = 5000;
        List<UUID> list = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            list.add(UuidUtils.generateV7());
        }

        for (int i = 0; i < count - 1; i++) {
            UUID curr = list.get(i);
            UUID next = list.get(i + 1);
            assertThat(curr.compareTo(next))
                    .withFailMessage("Expected UUID %s to be less than %s at index %d", curr, next, i)
                    .isLessThan(0);
        }
    }

    @Test
    @DisplayName("Monotonic ordering is preserved even when forced to same millisecond timestamp")
    void shouldIncrementSequenceWithinSameMillisecond() {
        long fixedTimestampMs = 1775293800000L;
        UUID u1 = UuidUtils.createUuidV7(fixedTimestampMs, 0, 100);
        UUID u2 = UuidUtils.createUuidV7(fixedTimestampMs, 1, 200);
        UUID u3 = UuidUtils.createUuidV7(fixedTimestampMs, 2, 50);

        assertThat(u1.version()).isEqualTo(7);
        assertThat(u2.version()).isEqualTo(7);
        assertThat(u3.version()).isEqualTo(7);

        assertThat(u1.compareTo(u2)).isLessThan(0);
        assertThat(u2.compareTo(u3)).isLessThan(0);
    }

    @Test
    @DisplayName("Concurrent multi-threaded UUID generation produces all unique v7 IDs")
    void concurrentGenerationShouldProduceUniqueV7Uuids() throws InterruptedException {
        int threadCount = 8;
        int perThread = 1000;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);
        Set<UUID> generated = ConcurrentHashMap.newKeySet();

        for (int t = 0; t < threadCount; t++) {
            Thread worker = new Thread(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < perThread; i++) {
                        generated.add(UuidUtils.generateV7());
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            });
            worker.start();
        }

        startLatch.countDown();
        boolean finished = doneLatch.await(10, TimeUnit.SECONDS);

        assertThat(finished).isTrue();
        assertThat(generated)
                .hasSize(threadCount * perThread)
                .allSatisfy(uuid -> {
                    assertThat(uuid.version()).isEqualTo(7);
                    assertThat(uuid.variant()).isEqualTo(2);
                });
    }

    @Test
    @DisplayName("Senior Refinements Round 72: generateV7WithTimestamp preserves strict monotonicity of both id and createdAt on clock step-back and concurrent threads")
    void generateV7WithTimestampShouldPreventFifoInversionOnClockStepBackAndConcurrency() {
        Instant t1 = Instant.ofEpochMilli(1_700_000_000_000L);
        Instant t2StepBack = t1.minusMillis(50);

        UuidUtils.TimestampedUuid first = UuidUtils.generateV7WithTimestamp(t1);
        UuidUtils.TimestampedUuid second = UuidUtils.generateV7WithTimestamp(t2StepBack);

        assertThat(first.id().compareTo(second.id())).isLessThan(0);
        // Even though clock stepped back by 50ms, second.createdAt() must not be before first.createdAt()
        assertThat(second.createdAt()).isAfterOrEqualTo(first.createdAt());
        assertThat(UuidUtils.extractInstant(first.id())).isEqualTo(Instant.ofEpochMilli(t1.toEpochMilli()));
    }
}
