package io.github.alexlitvinov.idembox.core.model;

import java.time.Instant;

/**
 * Result of an acquire operation along with the acquired lease expiration timestamp if successfully acquired.
 *
 * @param result AcquireResult indicating if execution was acquired, already completed, or currently locked in-flight
 * @param lockedUntil The timestamp when the acquired lease expires, or null if not acquired
 */
public record AcquireOutcome(AcquireResult result, Instant lockedUntil) {
}
