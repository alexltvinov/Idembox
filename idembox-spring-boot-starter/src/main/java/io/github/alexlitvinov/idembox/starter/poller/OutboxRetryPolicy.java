package io.github.alexlitvinov.idembox.starter.poller;

import io.github.alexlitvinov.idembox.starter.config.IdemboxProperties;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.SerializationException;
import io.github.alexlitvinov.idembox.starter.util.ThreadTypeClassifier;

import java.net.ConnectException;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;

/**
 * Component responsible for outbox retry calculations, exponential backoff with jitter,
 * broker cooldown calculations, and Kafka exception classification (broker outages vs. fatal errors).
 */
public class OutboxRetryPolicy {

    /**
     * Calculates exponential backoff with full jitter (80% - 120%) based on retry count.
     */
    public Duration calculateBackoff(int retryCount, IdemboxProperties properties) {
        if (properties == null || properties.getOutbox() == null) {
            return Duration.ofMillis(100L);
        }
        Duration initial = properties.getOutbox().getInitialBackoff();
        Duration max = properties.getOutbox().getMaxBackoff();
        double multiplier = properties.getOutbox().getBackoffMultiplier();

        long maxMs = (max != null && !max.isNegative()) ? Math.max(100L, max.toMillis()) : 60_000L;
        long initialMs = (initial != null && !initial.isNegative()) ? initial.toMillis() : 100L;
        double effectiveMultiplier = multiplier > 0 ? multiplier : 1.0;

        int safePower = Math.min(Math.max(0, retryCount), 30);
        double calculated = initialMs * Math.pow(effectiveMultiplier, safePower);
        long backoffMs = (calculated >= Long.MAX_VALUE || calculated < 0) ? maxMs : (long) calculated;
        long baseMs = Math.max(100L, Math.min(backoffMs, maxMs));

        // Full Jitter (80% - 120%) to avoid Thundering Herd on retries, capped at maxBackoff and floored at 100ms
        long minJitter = (long) (baseMs * 0.8);
        long maxJitter = (long) (baseMs * 1.2);
        long jitteredMs = ThreadLocalRandom.current().nextLong(minJitter, maxJitter + 1);
        long finalMs = Math.max(100L, Math.min(jitteredMs, maxMs));
        return Duration.ofMillis(finalMs);
    }

    /**
     * Calculates a jittered broker cooldown duration (80% - 120% of configured cooldown).
     */
    public long calculateJitteredCooldownMs(IdemboxProperties properties) {
        Duration cooldown = properties != null && properties.getOutbox() != null ? properties.getOutbox().getBrokerCooldown() : null;
        long baseCooldownMs = (cooldown != null && !cooldown.isNegative()) ? cooldown.toMillis() : 10_000L;
        double jitter = ThreadLocalRandom.current().nextDouble(0.8, 1.2);
        return (long) (baseCooldownMs * jitter);
    }

    /**
     * Determines whether an exception represents a transient broker outage or network issue.
     */
    public boolean isBrokerOutage(Throwable t) {
        Throwable current = t;
        while (current != null) {
            if (current instanceof TimeoutException
                    || current instanceof RetriableException
                    || current instanceof ConnectException
                    || current instanceof AuthenticationException
                    || current instanceof AuthorizationException) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    /**
     * Determines whether an exception represents a non-retryable fatal producer error.
     */
    public boolean isFatalProducerError(Throwable t) {
        Throwable current = t;
        while (current != null) {
            if (current instanceof RecordTooLargeException
                    || current instanceof SerializationException
                    || current instanceof InvalidTopicException
                    || current instanceof ClassCastException) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    /**
     * Extracts and sanitizes human-readable error reason from an exception.
     */
    public String extractErrorReason(Throwable ex, String defaultReason) {
        if (ex == null) {
            String fallback = defaultReason != null ? defaultReason : "Unknown error";
            return fallback.replace("\u0000", "").replace("\0", "");
        }
        String msg = ex.getMessage();
        String reason;
        if (msg == null || msg.isBlank()) {
            reason = ex.getClass().getSimpleName();
        } else {
            reason = ex.getClass().getSimpleName() + ": " + msg;
        }
        reason = reason.replace("\u0000", "").replace("\0", "");
        if (reason.length() > 2000) {
            int cutLength = 1997;
            if (Character.isHighSurrogate(reason.charAt(cutLength - 1))) {
                cutLength--;
            }
            reason = reason.substring(0, cutLength) + "...";
        }
        return reason;
    }

    /**
     * Checks if the current thread is an internal Kafka network producer thread.
     */
    public static boolean isKafkaThread(Thread thread) {
        return ThreadTypeClassifier.isKafkaNetworkThread(thread);
    }
}
