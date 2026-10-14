package io.github.alexlitvinov.idembox.core.util;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;

class IdemboxUtilsTest {

    @Test
    @DisplayName("sanitizeText strips null bytes and literal \\u0000 strings")
    void testSanitizeText() {
        assertThat(IdemboxUtils.sanitizeText(null)).isNull();
        assertThat(IdemboxUtils.sanitizeText("safe-text")).isEqualTo("safe-text");
        assertThat(IdemboxUtils.sanitizeText("hello\0world")).isEqualTo("helloworld");
        assertThat(IdemboxUtils.sanitizeText("foo\\u0000bar")).isEqualTo("foobar");
        assertThat(IdemboxUtils.sanitizeText("\0\0\\u0000")).isEqualTo("");
    }

    @Test
    @DisplayName("toOffsetDateTime converts Instant to UTC OffsetDateTime truncated to microseconds")
    void testToOffsetDateTime() {
        assertThat(IdemboxUtils.toOffsetDateTime(null)).isNull();

        Instant now = Instant.parse("2026-10-13T12:34:56.123456789Z");
        OffsetDateTime odt = IdemboxUtils.toOffsetDateTime(now);

        assertThat(odt).isNotNull();
        assertThat(odt.getOffset()).isEqualTo(ZoneOffset.UTC);
        // Nanoseconds must be truncated to microseconds (ends with 000)
        assertThat(odt.getNano()).isEqualTo(123456000);
        assertThat(odt.toInstant()).isEqualTo(Instant.parse("2026-10-13T12:34:56.123456Z"));
    }

    @Test
    @DisplayName("sha256Hex produces lowercase 64-character hexadecimal hash for bytes and string")
    void testSha256Hex() {
        assertThat(IdemboxUtils.sha256Hex((byte[]) null)).isNull();
        assertThat(IdemboxUtils.sha256Hex((String) null)).isNull();

        String input = "test-input";
        String expected = "ae1608896372720b6ebb58261e0c0092c608324b0804bc99267c1753990faaa8";

        assertThat(IdemboxUtils.sha256Hex(input.getBytes(StandardCharsets.UTF_8))).isEqualTo(expected);
        assertThat(IdemboxUtils.sha256Hex(input)).isEqualTo(expected);
    }
}
