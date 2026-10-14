package io.github.alexlitvinov.idembox.core.util;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.HexFormat;

/**
 * Common internal utility methods for text sanitization, date/time conversions, and cryptographic hashing.
 */
public final class IdemboxUtils {

    private IdemboxUtils() {
    }

    /**
     * Strips null bytes ({@code \0}) and literal {@code "\\u0000"} strings to prevent PostgreSQL UTF-8 column errors.
     *
     * @param text input text
     * @return sanitized text, or {@code null} if the input was {@code null}
     */
    public static String sanitizeText(String text) {
        if (text == null) {
            return null;
        }
        if (text.contains("\\u0000") || text.indexOf('\0') >= 0) {
            return text.replace("\\u0000", "").replace("\0", "");
        }
        return text;
    }

    /**
     * Converts an {@link Instant} to an {@link OffsetDateTime} in UTC,
     * truncated to microsecond precision for PostgreSQL timestamp compatibility.
     *
     * @param instant the instant to convert
     * @return truncated OffsetDateTime, or {@code null} if instant is {@code null}
     */
    public static OffsetDateTime toOffsetDateTime(Instant instant) {
        if (instant == null) {
            return null;
        }
        Instant truncated = instant.truncatedTo(ChronoUnit.MICROS);
        return OffsetDateTime.ofInstant(truncated, ZoneOffset.UTC);
    }

    /**
     * Computes the SHA-256 hexadecimal hash of the given byte array.
     *
     * @param input byte array
     * @return lowercase 64-character hex string, or {@code null} if input is {@code null}
     */
    public static String sha256Hex(byte[] input) {
        if (input == null) {
            return null;
        }
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(input);
            return HexFormat.of().formatHex(hash);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 algorithm not available in JVM environment", e);
        }
    }

    /**
     * Computes the SHA-256 hexadecimal hash of the given string using UTF-8 encoding.
     *
     * @param input string
     * @return lowercase 64-character hex string, or {@code null} if input is {@code null}
     */
    public static String sha256Hex(String input) {
        if (input == null) {
            return null;
        }
        return sha256Hex(input.getBytes(StandardCharsets.UTF_8));
    }
}
