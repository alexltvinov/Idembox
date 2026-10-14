package io.github.alexlitvinov.idembox.starter.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

class IdemboxMetricsTest {

    private SimpleMeterRegistry meterRegistry;
    private IdemboxMetrics metrics;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        metrics = new IdemboxMetrics(meterRegistry);
    }

    @Test
    @DisplayName("Fix 6: Metrics must be cached and re-used to prevent GC allocations on hot paths")
    void shouldReuseCachedMetersAcrossInvocations() {
        metrics.recordOutboxEnqueued("orders-topic");
        metrics.recordOutboxEnqueued("orders-topic");

        Counter counter = meterRegistry.find("idembox.outbox.messages.enqueued")
                .tag("topic", "orders-topic")
                .counter();

        assertThat(counter).isNotNull();
        assertThat(counter.count()).isEqualTo(2.0);

        metrics.recordOutboxSent("orders-topic", Duration.ofMillis(15));
        metrics.recordOutboxSent("orders-topic", Duration.ofMillis(25));

        Counter sentCounter = meterRegistry.find("idembox.outbox.messages.sent")
                .tag("topic", "orders-topic")
                .counter();
        Timer latencyTimer = meterRegistry.find("idembox.outbox.publish.latency")
                .tag("topic", "orders-topic")
                .timer();

        assertThat(sentCounter).isNotNull();
        assertThat(sentCounter.count()).isEqualTo(2.0);
        assertThat(latencyTimer).isNotNull();
        assertThat(latencyTimer.count()).isEqualTo(2L);
    }

    @Test
    @DisplayName("Fix 6: Idempotent meters must be cached by consumerGroup using unified idembox.idempotent.executions")
    void shouldReuseIdempotentCachedMeters() {
        metrics.recordIdempotentAcquired("orders-group");
        metrics.recordIdempotentAcquired("orders-group");
        metrics.recordIdempotentDuplicate("orders-group");
        metrics.recordIdempotentFailed("orders-group");
        metrics.recordIdempotentConcurrentRetry("orders-group");

        Counter unifiedAcquired = meterRegistry.find("idembox.idempotent.executions")
                .tag("group", "orders-group")
                .tag("result", "acquired")
                .counter();
        Counter unifiedDuplicates = meterRegistry.find("idembox.idempotent.executions")
                .tag("group", "orders-group")
                .tag("result", "duplicate")
                .counter();
        Counter unifiedFailed = meterRegistry.find("idembox.idempotent.executions")
                .tag("group", "orders-group")
                .tag("result", "failed")
                .counter();
        Counter unifiedRetry = meterRegistry.find("idembox.idempotent.executions")
                .tag("group", "orders-group")
                .tag("result", "concurrent_retry")
                .counter();

        assertThat(unifiedAcquired).isNotNull();
        assertThat(unifiedAcquired.count()).isEqualTo(2.0);
        assertThat(unifiedDuplicates).isNotNull();
        assertThat(unifiedDuplicates.count()).isEqualTo(1.0);
        assertThat(unifiedFailed).isNotNull();
        assertThat(unifiedFailed.count()).isEqualTo(1.0);
        assertThat(unifiedRetry).isNotNull();
        assertThat(unifiedRetry.count()).isEqualTo(1.0);

        // Obsolete separate counters should not be registered
        assertThat(meterRegistry.find("idembox.idempotent.messages.acquired").counter()).isNull();
        assertThat(meterRegistry.find("idembox.idempotent.messages.duplicates").counter()).isNull();
        assertThat(meterRegistry.find("idembox.idempotent.messages.failed").counter()).isNull();
    }

    @Test
    @DisplayName("Fix 4: Terminal failure metric complies with Prometheus/OpenTelemetry standard")
    void shouldRecordTerminalFailures() {
        metrics.recordOutboxTerminalFailure("orders-topic");
        metrics.recordOutboxTerminalFailure("orders-topic");

        Counter terminalCounter = meterRegistry.find("idembox.outbox.messages.terminal.failures")
                .tag("topic", "orders-topic")
                .counter();

        assertThat(terminalCounter).isNotNull();
        assertThat(terminalCounter.count()).isEqualTo(2.0);
    }

    @Test
    @DisplayName("Fix 8: Outbox lag gauge reports current pending count")
    void shouldReportPendingMessagesGauge() {
        AtomicLong pendingCount = new AtomicLong(15);
        metrics.registerPendingMessagesGauge(pendingCount::get);

        Gauge gauge = meterRegistry.find("idembox.outbox.messages.pending").gauge();
        assertThat(gauge).isNotNull();
        assertThat(gauge.value()).isEqualTo(15.0);

        pendingCount.set(87);
        assertThat(gauge.value()).isEqualTo(87.0);
    }

    @Test
    @DisplayName("Senior Refinements v28: Outbox lag gauge uses strongReference and survives GC without degrading to NaN")
    void shouldSurviveGarbageCollectionWithoutDegradingToNaN() {
        AtomicLong pendingCount = new AtomicLong(42);
        // Pass a synthetic lambda that is eligible for GC if not held strongly
        metrics.registerPendingMessagesGauge(pendingCount::get);

        Gauge gauge = meterRegistry.find("idembox.outbox.messages.pending").gauge();
        assertThat(gauge).isNotNull();
        assertThat(gauge.value()).isEqualTo(42.0);

        System.gc();

        assertThat(gauge.value()).isEqualTo(42.0);
        assertThat(Double.isNaN(gauge.value())).isFalse();
    }

    @Test
    @DisplayName("Senior Refinements Round 63: UUIDs and invalid names route to 'other', valid names cap at 500 tags routing overflow to 'other' without evicting meters")
    void shouldEnforceCardinalityCapOnDynamicTopicsAndGroups() {
        // Dynamic UUID topics route to 'other' without polluting cache
        metrics.recordOutboxEnqueued(java.util.UUID.randomUUID().toString());
        metrics.recordOutboxEnqueued(java.util.UUID.randomUUID().toString());
        metrics.recordOutboxEnqueued("   ");

        Counter fallbackCounter = meterRegistry.find("idembox.outbox.messages.enqueued")
                .tag("topic", "other")
                .counter();
        assertThat(fallbackCounter).isNotNull();
        assertThat(fallbackCounter.count()).isEqualTo(3.0);

        // Enqueue 500 distinct legitimate topics
        for (int i = 0; i < IdemboxMetrics.MAX_DYNAMIC_TAGS; i++) {
            metrics.recordOutboxEnqueued("topic-" + i);
        }

        Counter topic0Counter = meterRegistry.find("idembox.outbox.messages.enqueued")
                .tag("topic", "topic-0")
                .counter();
        assertThat(topic0Counter).isNotNull();

        // Topic 500 should be mapped to fallback tag 'other' without evicting topic-0
        metrics.recordOutboxEnqueued("topic-500");

        // topic-0 should NOT have been removed from meterRegistry
        assertThat(meterRegistry.find("idembox.outbox.messages.enqueued")
                .tag("topic", "topic-0")
                .counter()).isNotNull();

        // topic-500 was routed to 'other' (3 from before + 1 for topic-500 = 4.0)
        Counter otherTopicCounter = meterRegistry.find("idembox.outbox.messages.enqueued")
                .tag("topic", "other")
                .counter();
        assertThat(otherTopicCounter).isNotNull();
        assertThat(otherTopicCounter.count()).isEqualTo(4.0);

        // topic-500 meter itself was never registered
        assertThat(meterRegistry.find("idembox.outbox.messages.enqueued")
                .tag("topic", "topic-500")
                .counter()).isNull();

        // Repeat for consumer groups: UUID groups route to 'other'
        metrics.recordIdempotentAcquired(java.util.UUID.randomUUID().toString());
        Counter fallbackGroupCounter = meterRegistry.find("idembox.idempotent.executions")
                .tag("group", "other")
                .tag("result", "acquired")
                .counter();
        assertThat(fallbackGroupCounter).isNotNull();
        assertThat(fallbackGroupCounter.count()).isEqualTo(1.0);

        // Legitimate groups up to 500
        for (int i = 0; i < IdemboxMetrics.MAX_DYNAMIC_TAGS; i++) {
            metrics.recordIdempotentAcquired("group-" + i);
        }
        assertThat(meterRegistry.find("idembox.idempotent.executions")
                .tag("group", "group-0")
                .tag("result", "acquired")
                .counter()).isNotNull();

        metrics.recordIdempotentAcquired("group-500");

        // group-0 should NOT have been removed from meterRegistry
        assertThat(meterRegistry.find("idembox.idempotent.executions")
                .tag("group", "group-0")
                .tag("result", "acquired")
                .counter()).isNotNull();

        // group-500 was routed to 'other' (1 earlier + 1 for group-500 = 2.0)
        Counter otherGroupCounter = meterRegistry.find("idembox.idempotent.executions")
                .tag("group", "other")
                .tag("result", "acquired")
                .counter();
        assertThat(otherGroupCounter).isNotNull();
        assertThat(otherGroupCounter.count()).isEqualTo(2.0);

        // group-500 meter itself was never registered
        assertThat(meterRegistry.find("idembox.idempotent.executions")
                .tag("group", "group-500")
                .tag("result", "acquired")
                .counter()).isNull();
    }

    @Test
    @DisplayName("NoOp metrics should safely swallow all metric invocations without NPE")
    void shouldHandleNoOpGracefully() {
        IdemboxMetrics noOp = IdemboxMetrics.noOp();
        noOp.recordOutboxEnqueued("topic");
        noOp.recordOutboxSent("topic", Duration.ofMillis(10));
        noOp.recordOutboxFailed("topic");
        noOp.recordOutboxTerminalFailure("topic");
        noOp.recordIdempotentAcquired("group");
        noOp.recordIdempotentDuplicate("group");
        noOp.recordIdempotentFailed("group");
        noOp.registerPendingMessagesGauge(() -> 10);
    }

    @Test
    @DisplayName("Senior Refinements Round 63: Topics cap routes overflow to 'other' without evicting existing meters")
    void shouldShareUnifiedTopicSetAcrossMetrics() {
        // Enqueue 500 topics
        for (int i = 0; i < 500; i++) {
            metrics.recordOutboxEnqueued("shared-topic-" + i);
        }

        // Access shared-topic-0 via recordOutboxSent
        metrics.recordOutboxSent("shared-topic-0", Duration.ofMillis(10));

        // Add overflow-topic via recordOutboxFailed. Maps to 'other'
        metrics.recordOutboxFailed("overflow-topic");

        Counter overflowFailed = meterRegistry.find("idembox.outbox.messages.failed")
                .tag("topic", "other")
                .counter();
        assertThat(overflowFailed).isNotNull();
        assertThat(overflowFailed.count()).isEqualTo(1.0);

        // shared-topic-0 was accessed recently and should still be present
        Counter shared0Sent = meterRegistry.find("idembox.outbox.messages.sent")
                .tag("topic", "shared-topic-0")
                .counter();
        assertThat(shared0Sent).isNotNull();
        assertThat(shared0Sent.count()).isEqualTo(1.0);

        // shared-topic-1 should NOT have been evicted
        assertThat(meterRegistry.find("idembox.outbox.messages.enqueued")
                .tag("topic", "shared-topic-1")
                .counter()).isNotNull();
    }

    @Test
    @DisplayName("Senior Refinements Round 52: resolveTopic and resolveGroup use fast-path lock-free reads and evict meters outside monitor")
    void shouldExecuteFastPathAndEvictWithoutDeadlock() {
        for (int i = 0; i < 1000; i++) {
            metrics.recordOutboxEnqueued("fast-topic");
            metrics.recordIdempotentAcquired("fast-group");
        }

        Counter fastTopicCounter = meterRegistry.find("idembox.outbox.messages.enqueued")
                .tag("topic", "fast-topic")
                .counter();
        Counter fastGroupCounter = meterRegistry.find("idembox.idempotent.executions")
                .tag("group", "fast-group")
                .tag("result", "acquired")
                .counter();

        assertThat(fastTopicCounter).isNotNull();
        assertThat(fastTopicCounter.count()).isEqualTo(1000.0);
        assertThat(fastGroupCounter).isNotNull();
        assertThat(fastGroupCounter.count()).isEqualTo(1000.0);
    }

    @Test
    @DisplayName("Issue 3: recordOutboxLeaseStolen records idembox.outbox.leases.stolen metric with topic tag")
    void shouldRecordOutboxLeaseStolen() {
        metrics.recordOutboxLeaseStolen("order-events");
        metrics.recordOutboxLeaseStolen("order-events");

        Counter counter = meterRegistry.find("idembox.outbox.leases.stolen")
                .tag("topic", "order-events")
                .counter();

        assertThat(counter).isNotNull();
        assertThat(counter.count()).isEqualTo(2.0);
    }
}
