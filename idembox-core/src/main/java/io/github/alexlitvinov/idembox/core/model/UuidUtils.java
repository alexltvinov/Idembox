package io.github.alexlitvinov.idembox.core.model;

import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Utility for generating time-ordered, monotonic UUID version 7 according to RFC 9562.
 * <p>
 * Standard UUID v7 layout:
 * <ul>
 *     <li>0-47 bits (48 bits): Unix timestamp in milliseconds</li>
 *     <li>48-51 bits (4 bits): Version 7 ({@code 0111})</li>
 *     <li>52-63 bits (12 bits): Monotonic sequence counter within the millisecond</li>
 *     <li>64-65 bits (2 bits): Variant 2 ({@code 10})</li>
 *     <li>66-127 bits (62 bits): Cryptographically secure random entropy</li>
 * </ul>
 * <p>
 * This guarantees strict monotonic ordering (both in PostgreSQL {@code earlier.id < m.id} byte-order
 * and Java {@link UUID#compareTo(UUID)}) even when multiple messages are generated in the exact same millisecond.
 */
public final class UuidUtils {

    private static final Object LOCK = new Object();
    private static long lastTimestampMs = -1L;
    private static long counter = 0L;
    private static Instant lastCreatedAt = Instant.EPOCH;

    /**
     * Atomically paired monotonic UUIDv7 and monotonic creation timestamp.
     */
    public record TimestampedUuid(UUID id, Instant createdAt) {}

    private UuidUtils() {
    }

    /**
     * Generates a time-ordered monotonic UUID v7.
     *
     * @return Monotonically ordered UUID v7
     */
    public static UUID generateV7() {
        return generateV7WithTimestamp().id();
    }

    /**
     * Atomically generates a monotonically ordered UUID v7 and its corresponding monotonic {@link Instant}
     * within the same critical section. Prevents FIFO inversion between {@code id} and {@code createdAt}
     * under OS thread preemption or NTP clock step-backs.
     */
    public static TimestampedUuid generateV7WithTimestamp() {
        return generateV7WithTimestamp(Instant.now());
    }

    static TimestampedUuid generateV7WithTimestamp(Instant currentInstant) {
        long timestampMs;
        long sequence;
        Instant createdAt;

        synchronized (LOCK) {
            Instant now = currentInstant != null ? currentInstant : Instant.now();
            timestampMs = now.toEpochMilli();
            if (timestampMs < lastTimestampMs) {
                // System clock moved backwards: preserve monotonicity
                timestampMs = lastTimestampMs;
            }

            if (timestampMs == lastTimestampMs) {
                counter++;
                if (counter > 0x0FFFL) { // 12-bit overflow (4095)
                    // If counter overflows within the same millisecond, advance timestamp
                    timestampMs++;
                    lastTimestampMs = timestampMs;
                    counter = 0L;
                }
            } else {
                lastTimestampMs = timestampMs;
                counter = 0L;
            }

            sequence = counter;
            if (now.isBefore(lastCreatedAt)) {
                createdAt = lastCreatedAt;
            } else {
                createdAt = now;
                lastCreatedAt = now;
            }
        }

        long randB = ThreadLocalRandom.current().nextLong();
        return new TimestampedUuid(createUuidV7(timestampMs, sequence, randB), createdAt);
    }

    /**
     * Extracts the 48-bit Unix epoch millisecond timestamp embedded in a UUID v7.
     */
    public static Instant extractInstant(UUID uuidV7) {
        long timestampMs = (uuidV7.getMostSignificantBits() >>> 16) & 0xFFFFFFFFFFFFL;
        return Instant.ofEpochMilli(timestampMs);
    }

    /**
     * Helper to construct a UUID v7 from raw components (useful for deterministic tests).
     */
    static UUID createUuidV7(long timestampMs, long sequence, long randB) {
        // msb: 48 bits timestamp | 4 bits version (7) | 12 bits sequence
        long mostSigBits = ((timestampMs & 0xFFFFFFFFFFFFL) << 16) | 0x7000L | (sequence & 0x0FFFL);

        // lsb: 2 bits variant (10) | 62 bits random entropy
        long leastSigBits = (randB & 0x3FFFFFFFFFFFFFFFL) | 0x8000000000000000L;

        return new UUID(mostSigBits, leastSigBits);
    }

    /**
     * Resets internal monotonic clock and sequence state. Intended strictly for test isolation.
     */
    static void resetForTesting() {
        synchronized (LOCK) {
            lastTimestampMs = -1L;
            counter = 0L;
            lastCreatedAt = Instant.EPOCH;
        }
    }
}
