package io.github.alexlitvinov.idembox.core.model;

/**
 * Thrown when an unrecoverable failure occurs in the idempotency layer,
 * such as a failure to commit idempotent state or a stolen lease during completion.
 */
public class IdempotencyException extends RuntimeException {

    public IdempotencyException(String message) {
        super(message);
    }

    public IdempotencyException(String message, Throwable cause) {
        super(message, cause);
    }
}
