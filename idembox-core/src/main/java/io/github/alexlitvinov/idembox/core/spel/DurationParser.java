package io.github.alexlitvinov.idembox.core.spel;

import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class DurationParser {

    private static final Pattern DURATION_PATTERN = Pattern.compile("^(\\d+)\\s*(d|h|m|s|ms)?$", Pattern.CASE_INSENSITIVE);
    public static final long MAX_DURATION_DAYS = 3650L;
    static final int MAX_CACHE_SIZE = 500;
    private static final Map<String, Duration> CACHE = new ConcurrentHashMap<>(64);

    private DurationParser() {
    }

    public static final int MAX_DURATION_LENGTH = 128;

    /**
     * Parses human-readable durations like "7d", "24h", "30m", "10s", "500ms" or ISO-8601 strings.
     * Uses specified default duration if input is null or blank.
     * Thread-safe bounded cache (max 500 entries) with lock-free concurrent reads.
     */
    public static Duration parse(String input, Duration defaultDuration) {
        if (input == null || input.isBlank()) {
            return defaultDuration;
        }

        String trimmed = input.trim();
        if (trimmed.length() > MAX_DURATION_LENGTH) {
            throw new IllegalArgumentException("Duration string length exceeds maximum limit of " + MAX_DURATION_LENGTH + " characters (actual: " + trimmed.length() + ")");
        }
        Duration cached = CACHE.get(trimmed);
        if (cached != null) {
            return cached;
        }
        Duration parsed = parseInternal(trimmed);
        if (CACHE.size() < MAX_CACHE_SIZE) {
            CACHE.putIfAbsent(trimmed, parsed);
        }
        return parsed;
    }

    static int getCacheSize() {
        return CACHE.size();
    }

    static void clearCache() {
        CACHE.clear();
    }

    private static Duration parseInternal(String trimmed) {
        String sanitized = trimmed.replaceAll("\\s+", "").toUpperCase(Locale.ROOT);
        if (sanitized.startsWith("P") || sanitized.startsWith("+P") || sanitized.startsWith("-P")) {
            return validateDuration(parseIso8601(sanitized, trimmed), trimmed);
        }

        Matcher matcher = DURATION_PATTERN.matcher(trimmed);
        if (matcher.matches()) {
            long value;
            try {
                value = Long.parseLong(matcher.group(1));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Duration value overflows numeric limits: " + matcher.group(1), e);
            }
            if (value <= 0) {
                throw new IllegalArgumentException("Duration must be strictly positive (greater than zero): " + trimmed);
            }
            String unit = matcher.group(2) != null ? matcher.group(2).toLowerCase(Locale.ROOT) : "s";
            try {
                Duration d = switch (unit) {
                    case "d" -> {
                        if (value > MAX_DURATION_DAYS) {
                            throw new IllegalArgumentException(String.format(
                                    "Duration days value '%d' exceeds maximum limit of %d days (~10 years).", value, MAX_DURATION_DAYS));
                        }
                        yield Duration.ofDays(value);
                    }
                    case "h" -> {
                        if (value > MAX_DURATION_DAYS * 24L) {
                            throw new IllegalArgumentException(String.format(
                                    "Duration hours value '%d' exceeds maximum limit of %d hours (~10 years).", value, MAX_DURATION_DAYS * 24L));
                        }
                        yield Duration.ofHours(value);
                    }
                    case "m" -> {
                        if (value > MAX_DURATION_DAYS * 24L * 60L) {
                            throw new IllegalArgumentException(String.format(
                                    "Duration minutes value '%d' exceeds maximum limit of %d minutes (~10 years).", value, MAX_DURATION_DAYS * 24L * 60L));
                        }
                        yield Duration.ofMinutes(value);
                    }
                    case "s" -> {
                        if (value > MAX_DURATION_DAYS * 24L * 3600L) {
                            throw new IllegalArgumentException(String.format(
                                    "Duration seconds value '%d' exceeds maximum limit of %d seconds (~10 years).", value, MAX_DURATION_DAYS * 24L * 3600L));
                        }
                        yield Duration.ofSeconds(value);
                    }
                    case "ms" -> {
                        if (value > MAX_DURATION_DAYS * 24L * 3600L * 1000L) {
                            throw new IllegalArgumentException(String.format(
                                    "Duration milliseconds value '%d' exceeds maximum limit of %d ms (~10 years).", value, MAX_DURATION_DAYS * 24L * 3600L * 1000L));
                        }
                        yield Duration.ofMillis(value);
                    }
                    default -> Duration.ofSeconds(value);
                };
                return validateDuration(d, trimmed);
            } catch (ArithmeticException ae) {
                throw new IllegalArgumentException("Duration value causes numeric overflow: " + trimmed, ae);
            }
        }

        return validateDuration(parseIso8601(sanitized, trimmed), trimmed);
    }

    private static Duration parseIso8601(String sanitized, String original) {
        try {
            return Duration.parse(sanitized);
        } catch (ArithmeticException ae) {
            throw new IllegalArgumentException("Duration value causes numeric overflow: " + original, ae);
        } catch (DateTimeParseException exception) {
            if (exception.getCause() instanceof ArithmeticException || exception.getCause() instanceof NumberFormatException) {
                throw new IllegalArgumentException("Duration value overflows numeric limits: " + original, exception);
            }
            throw exception;
        }
    }

    private static Duration validateDuration(Duration d, String original) {
        if (d.isNegative() || d.isZero()) {
            throw new IllegalArgumentException("Duration must be strictly positive (greater than zero): " + original);
        }
        if (d.toDays() > MAX_DURATION_DAYS) {
            throw new IllegalArgumentException(String.format(
                    "Duration '%s' exceeds maximum limit of %d days (~10 years).", original, MAX_DURATION_DAYS));
        }
        return d;
    }

    /**
     * Parses duration with standard default of 7 days.
     */
    public static Duration parse(String input) {
        return parse(input, Duration.ofDays(7));
    }
}
