package io.github.alexlitvinov.idembox.starter.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.regex.Pattern;

/**
 * Enterprise metrics publisher using Micrometer.
 * Meters are cached in ConcurrentHashMap to eliminate GC pressure on hot publishing and consumption paths.
 * Protects against metric cardinality explosion with MAX_DYNAMIC_TAGS cap.
 * Gracefully acts as a no-op if no MeterRegistry is configured.
 */
public class IdemboxMetrics {

    private static final Logger log = LoggerFactory.getLogger(IdemboxMetrics.class);

    public static final int MAX_DYNAMIC_TAGS = 500;
    public static final String FALLBACK_TAG = "other";

    private final MeterRegistry meterRegistry;

    private static final Pattern UUID_PATTERN =
            Pattern.compile("^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$");

    private final BoundedTagRegistry topicRegistry = new BoundedTagRegistry("outbox topics", "topic");
    private final BoundedTagRegistry groupRegistry = new BoundedTagRegistry("idempotent consumer groups", "group");

    private final ConcurrentMap<String, Counter> enqueuedCounters = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Counter> sentCounters = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Counter> failedCounters = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Counter> terminalFailureCounters = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Timer> latencyTimers = new ConcurrentHashMap<>();

    private final ConcurrentMap<String, Counter> idempotentExecutionCounters = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Counter> leaseStolenCounters = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Counter> outboxLeaseStolenCounters = new ConcurrentHashMap<>();

    public IdemboxMetrics(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    public static IdemboxMetrics noOp() {
        return new IdemboxMetrics(null);
    }

    private String resolveTopic(String topic) {
        return topicRegistry.resolve(topic);
    }

    private String resolveGroup(String group) {
        return groupRegistry.resolve(group);
    }

    private static final class BoundedTagRegistry {
        private final String categoryDescription;
        private final String itemName;
        private final Set<String> knownValues = new HashSet<>();
        private final ConcurrentMap<String, Boolean> fastKnownValues = new ConcurrentHashMap<>();
        private final AtomicBoolean limitReached = new AtomicBoolean(false);
        private final AtomicBoolean limitWarned = new AtomicBoolean(false);

        private BoundedTagRegistry(String categoryDescription, String itemName) {
            this.categoryDescription = categoryDescription;
            this.itemName = itemName;
        }

        private String resolve(String rawValue) {
            if (rawValue == null || rawValue.isBlank() || rawValue.length() > 255) {
                return FALLBACK_TAG;
            }
            String cleanValue = rawValue.trim();
            if (UUID_PATTERN.matcher(cleanValue).matches()) {
                return FALLBACK_TAG;
            }
            if (fastKnownValues.containsKey(cleanValue)) {
                return cleanValue;
            }
            if (limitReached.get()) {
                warnLimitReachedOnce(cleanValue);
                return FALLBACK_TAG;
            }
            synchronized (knownValues) {
                if (fastKnownValues.containsKey(cleanValue)) {
                    return cleanValue;
                }
                if (knownValues.size() >= MAX_DYNAMIC_TAGS) {
                    limitReached.set(true);
                    warnLimitReachedOnce(cleanValue);
                    return FALLBACK_TAG;
                }
                knownValues.add(cleanValue);
                fastKnownValues.put(cleanValue, Boolean.TRUE);
                return cleanValue;
            }
        }

        private void warnLimitReachedOnce(String cleanValue) {
            if (limitWarned.compareAndSet(false, true)) {
                log.warn("Metric cardinality limit ({}) reached for {}. Mapping unmapped {} '{}' to fallback tag '{}' to prevent cardinality explosion.",
                        MAX_DYNAMIC_TAGS, categoryDescription, itemName, cleanValue, FALLBACK_TAG);
            }
        }
    }

    public void registerPendingMessagesGauge(Supplier<Number> countSupplier) {
        if (meterRegistry != null) {
            Gauge.builder("idembox.outbox.messages.pending", () -> {
                if (countSupplier == null) {
                    return 0L;
                }
                try {
                    Number val = countSupplier.get();
                    if (val == null) {
                        return 0L;
                    }
                    return Math.max(val.longValue(), 0L);
                } catch (Exception e) {
                    return 0L;
                }
            })
                    .description("Current number of pending outbox messages in database")
                    .strongReference(true)
                    .register(meterRegistry);
        }
    }

    public void recordOutboxEnqueued(String topic) {
        if (meterRegistry != null) {
            String tag = resolveTopic(topic);
            enqueuedCounters.computeIfAbsent(tag, t ->
                    Counter.builder("idembox.outbox.messages.enqueued")
                            .tag("topic", t)
                            .description("Total number of outbox messages inserted into DB")
                            .register(meterRegistry)
            ).increment();
        }
    }

    public void recordOutboxSent(String topic, Duration duration) {
        if (meterRegistry != null) {
            String tag = resolveTopic(topic);
            sentCounters.computeIfAbsent(tag, t ->
                    Counter.builder("idembox.outbox.messages.sent")
                            .tag("topic", t)
                            .description("Total number of outbox messages successfully delivered to Kafka")
                            .register(meterRegistry)
            ).increment();

            latencyTimers.computeIfAbsent(tag, t ->
                    Timer.builder("idembox.outbox.publish.latency")
                            .tag("topic", t)
                            .description("Time taken to publish outbox message to Kafka broker")
                            .register(meterRegistry)
            ).record(duration);
        }
    }

    public void recordOutboxFailed(String topic) {
        if (meterRegistry != null) {
            String tag = resolveTopic(topic);
            failedCounters.computeIfAbsent(tag, t ->
                    Counter.builder("idembox.outbox.messages.failed")
                            .tag("topic", t)
                            .description("Total number of outbox delivery failures to Kafka")
                            .register(meterRegistry)
            ).increment();
        }
    }

    public void recordOutboxTerminalFailure(String topic) {
        if (meterRegistry != null) {
            String tag = resolveTopic(topic);
            terminalFailureCounters.computeIfAbsent(tag, t ->
                    Counter.builder("idembox.outbox.messages.terminal.failures")
                            .tag("topic", t)
                            .description("Total number of messages transitioned to FAILED status after exceeding max-retries")
                            .register(meterRegistry)
            ).increment();
        }
    }

    public void recordIdempotentExecution(String consumerGroup, String result) {
        if (meterRegistry != null) {
            String tag = resolveGroup(consumerGroup);
            String metricKey = tag + ":" + result;
            idempotentExecutionCounters.computeIfAbsent(metricKey, k ->
                    Counter.builder("idembox.idempotent.executions")
                            .tag("group", tag)
                            .tag("result", result)
                            .description("Idempotent consumer executions partitioned by result (acquired, duplicate, failed, concurrent_retry)")
                            .register(meterRegistry)
            ).increment();
        }
    }

    public void recordIdempotentAcquired(String consumerGroup) {
        recordIdempotentExecution(consumerGroup, "acquired");
    }

    public void recordIdempotentDuplicate(String consumerGroup) {
        recordIdempotentExecution(consumerGroup, "duplicate");
    }

    public void recordIdempotentFailed(String consumerGroup) {
        recordIdempotentExecution(consumerGroup, "failed");
    }

    public void recordIdempotentConcurrentRetry(String consumerGroup) {
        recordIdempotentExecution(consumerGroup, "concurrent_retry");
    }

    public void recordIdempotentLeaseStolen(String consumerGroup) {
        if (meterRegistry != null) {
            String tag = resolveGroup(consumerGroup);
            leaseStolenCounters.computeIfAbsent(tag, t ->
                    Counter.builder("idembox.idempotent.lease_stolen")
                            .tag("group", t)
                            .description("Total number of idempotent completions where lease was overtaken/stolen by another worker")
                            .register(meterRegistry)
            ).increment();
        }
    }

    public void recordOutboxLeaseStolen(String topic) {
        if (meterRegistry != null) {
            String tag = resolveTopic(topic);
            outboxLeaseStolenCounters.computeIfAbsent(tag, t ->
                    Counter.builder("idembox.outbox.leases.stolen")
                            .tag("topic", t)
                            .description("Total number of outbox send completions where lease expired or was overtaken before marking as SENT")
                            .register(meterRegistry)
            ).increment();
        }
    }
}
