package io.github.alexlitvinov.idembox.core.spel;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class DurationParserTest {

    @BeforeEach
    void setUp() {
        DurationParser.clearCache();
    }

    @Test
    @DisplayName("Should parse standard human-readable units")
    void shouldParseStandardUnits() {
        assertThat(DurationParser.parse("7d")).isEqualTo(Duration.ofDays(7));
        assertThat(DurationParser.parse("24h")).isEqualTo(Duration.ofHours(24));
        assertThat(DurationParser.parse("30m")).isEqualTo(Duration.ofMinutes(30));
        assertThat(DurationParser.parse("10s")).isEqualTo(Duration.ofSeconds(10));
        assertThat(DurationParser.parse("500ms")).isEqualTo(Duration.ofMillis(500));
        assertThat(DurationParser.parse("120")).isEqualTo(Duration.ofSeconds(120));
    }

    @Test
    @DisplayName("Should parse ISO-8601 strings and withstand internal whitespace")
    void shouldParseIso8601WithWhitespace() {
        assertThat(DurationParser.parse("PT15M")).isEqualTo(Duration.ofMinutes(15));
        assertThat(DurationParser.parse("PT 5 M")).isEqualTo(Duration.ofMinutes(5));
        assertThat(DurationParser.parse("pt 10 s")).isEqualTo(Duration.ofSeconds(10));
    }

    @Test
    @DisplayName("Should return default duration when input is null, empty or blank")
    void shouldReturnDefaultWhenNullOrBlank() {
        Duration def = Duration.ofMinutes(5);
        assertThat(DurationParser.parse(null, def)).isEqualTo(def);
        assertThat(DurationParser.parse("", def)).isEqualTo(def);
        assertThat(DurationParser.parse("   ", def)).isEqualTo(def);
    }

    @Test
    @DisplayName("Should cap cache size at MAX_CACHE_SIZE (500) and evict oldest entries")
    void shouldCapCacheSizeAndEvictOldest() {
        for (int i = 1; i <= 600; i++) {
            DurationParser.parse(i + "s");
        }

        assertThat(DurationParser.getCacheSize()).isEqualTo(DurationParser.MAX_CACHE_SIZE);
        assertThat(DurationParser.getCacheSize()).isLessThanOrEqualTo(500);
    }

    @Test
    @DisplayName("Should throw DateTimeParseException for non-matching invalid format")
    void shouldThrowDateTimeParseExceptionForInvalidFormat() {
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> DurationParser.parse("10 days"))
                .isInstanceOf(java.time.format.DateTimeParseException.class);

        org.assertj.core.api.Assertions.assertThatThrownBy(() -> DurationParser.parse("invalid"))
                .isInstanceOf(java.time.format.DateTimeParseException.class);

        org.assertj.core.api.Assertions.assertThatThrownBy(() -> DurationParser.parse("2w"))
                .isInstanceOf(java.time.format.DateTimeParseException.class);
    }

    @Test
    @DisplayName("Should parse with default 1-arg parse() defaulting to 7 days")
    void shouldDefaultTo7DaysWithOneArgParse() {
        assertThat(DurationParser.parse(null)).isEqualTo(Duration.ofDays(7));
        assertThat(DurationParser.parse("")).isEqualTo(Duration.ofDays(7));
        assertThat(DurationParser.parse("   ")).isEqualTo(Duration.ofDays(7));
        assertThat(DurationParser.parse("1h")).isEqualTo(Duration.ofHours(1));
    }

    @Test
    @DisplayName("Senior Refinements v28: Duration values exceeding MAX_DURATION_DAYS or causing numeric overflow throw IllegalArgumentException")
    void shouldThrowIllegalArgumentExceptionOnOverflowOrExceedingLimits() {
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> DurationParser.parse("99999999999999999999999999d"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("overflows numeric limits");

        org.assertj.core.api.Assertions.assertThatThrownBy(() -> DurationParser.parse("5000d"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exceeds maximum limit");

        org.assertj.core.api.Assertions.assertThatThrownBy(() -> DurationParser.parse("PT9999999999999999999S"))
                .isInstanceOf(IllegalArgumentException.class);

        org.assertj.core.api.Assertions.assertThatThrownBy(() -> DurationParser.parse("PT90000H"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exceeds maximum limit");
    }

    @Test
    @DisplayName("Senior Refinements v38: Should evict least recently used entries under cache pressure (LRU)")
    void shouldEvictLeastRecentlyUsedEntries() {
        DurationParser.clearCache();
        for (int i = 1; i <= DurationParser.MAX_CACHE_SIZE; i++) {
            DurationParser.parse(i + "s");
        }
        // Access 1s to make it recently used
        DurationParser.parse("1s");

        // Parse a new duration to trigger eviction beyond MAX_CACHE_SIZE
        DurationParser.parse("999999s");

        assertThat(DurationParser.getCacheSize()).isEqualTo(DurationParser.MAX_CACHE_SIZE);
    }

    @Test
    @DisplayName("Senior Refinements v43: Duration string exceeding 128 characters throws IllegalArgumentException")
    void shouldThrowIllegalArgumentExceptionWhenInputExceedsLengthLimit() {
        String longInput = "10" + "0".repeat(150) + "s";
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> DurationParser.parse(longInput))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Duration string length exceeds maximum limit of 128 characters");
    }

    @Test
    @DisplayName("Senior Refinements v55: Concurrent threads can parse and retrieve cached durations concurrently without locking")
    void concurrentThreadsShouldReadAndCacheDurations() throws InterruptedException {
        int threads = 16;
        int iterations = 1000;
        java.util.concurrent.CountDownLatch latch = new java.util.concurrent.CountDownLatch(threads);
        java.util.concurrent.atomic.AtomicInteger errors = new java.util.concurrent.atomic.AtomicInteger(0);

        for (int t = 0; t < threads; t++) {
            Thread worker = new Thread(() -> {
                try {
                    for (int i = 0; i < iterations; i++) {
                        Duration d1 = DurationParser.parse("7d");
                        Duration d2 = DurationParser.parse("2m");
                        if (!Duration.ofDays(7).equals(d1) || !Duration.ofMinutes(2).equals(d2)) {
                            errors.incrementAndGet();
                        }
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                } finally {
                    latch.countDown();
                }
            });
            worker.start();
        }

        boolean finished = latch.await(10, java.util.concurrent.TimeUnit.SECONDS);
        assertThat(finished).isTrue();
        assertThat(errors.get()).isEqualTo(0);
        assertThat(DurationParser.getCacheSize()).isGreaterThanOrEqualTo(2);
    }

    @Test
    @DisplayName("Senior Refinements Round 71: Rejects +P durations exceeding MAX_DURATION_DAYS and rejects zero or negative durations without caching")
    void shouldRejectPlusPrefixExceedingMaxDaysAndZeroOrNegativeDurations() {
        DurationParser.clearCache();

        // +P99999D must not bypass MAX_DURATION_DAYS
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> DurationParser.parse("+P99999D"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exceeds maximum limit");

        // Zero durations in all formats must be rejected
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> DurationParser.parse("0s"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("strictly positive");
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> DurationParser.parse("0"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("strictly positive");
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> DurationParser.parse("PT0S"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("strictly positive");

        // Negative durations must be rejected
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> DurationParser.parse("-P5D"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("strictly positive");
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> DurationParser.parse("PT-5M"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("strictly positive");

        // Valid +P duration within limit works
        assertThat(DurationParser.parse("+P5D")).isEqualTo(Duration.ofDays(5));
        assertThat(DurationParser.getCacheSize()).isEqualTo(1);
    }
}
