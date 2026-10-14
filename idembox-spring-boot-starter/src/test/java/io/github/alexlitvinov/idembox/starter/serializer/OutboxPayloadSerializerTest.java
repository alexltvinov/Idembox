package io.github.alexlitvinov.idembox.starter.serializer;

import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OutboxPayloadSerializerTest {

    private OutboxPayloadSerializer serializer;

    @BeforeEach
    void setUp() {
        serializer = new OutboxPayloadSerializer();
    }

    @Test
    @DisplayName("serialize returns null when data is null")
    void shouldReturnNullWhenDataIsNull() {
        byte[] result = serializer.serialize("test-topic", null);
        assertThat(result).isNull();
    }

    @Test
    @DisplayName("serialize preserves byte[] without ClassCastException")
    void shouldSerializeByteArray() {
        byte[] expected = new byte[]{1, 2, 3, 4, 5};
        byte[] result = serializer.serialize("test-topic", expected);
        assertThat(result).isSameAs(expected);
    }

    @Test
    @DisplayName("serialize encodes String to UTF-8 bytes")
    void shouldSerializeString() {
        String json = "{\"orderId\":\"12345\"}";
        byte[] result = serializer.serialize("test-topic", json);
        assertThat(result).isEqualTo(json.getBytes(StandardCharsets.UTF_8));
    }

    record SampleOrderEvent(String orderId, int amount) {}

    static class UnserializablePayload {
        @com.fasterxml.jackson.annotation.JsonProperty
        public String getBroken() {
            throw new IllegalStateException("Jackson serialization error");
        }
    }

    @Test
    @DisplayName("Senior Refinements Round 75: serialize converts arbitrary domain POJO to UTF-8 JSON bytes for DLT publishing")
    void shouldSerializeDomainPojoToJsonBytes() {
        SampleOrderEvent event = new SampleOrderEvent("ORD-75", 250);
        byte[] result = serializer.serialize("orders.DLT", event);
        assertThat(new String(result, StandardCharsets.UTF_8))
                .isEqualTo("{\"orderId\":\"ORD-75\",\"amount\":250}");
    }

    @Test
    @DisplayName("serialize throws SerializationException when Jackson fails to serialize object")
    void shouldThrowSerializationExceptionWhenJsonSerializationFails() {
        UnserializablePayload payload = new UnserializablePayload();
        assertThatThrownBy(payload::getBroken)
                .isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> serializer.serialize("test-topic", payload))
                .isInstanceOf(SerializationException.class)
                .hasMessageContaining("Failed to serialize payload of type")
                .hasMessageContaining("UnserializablePayload");
    }
}

