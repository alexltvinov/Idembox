package io.github.alexlitvinov.idembox.starter.serializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * Universal Kafka serializer for outbox and DLT payloads.
 * Supports byte[] (binary/Protobuf/Avro/failed deserialization data), String (JSON/text),
 * and arbitrary domain POJOs (serialized to JSON UTF-8 bytes via Jackson {@link ObjectMapper}),
 * preventing ClassCastException and SerializationException when publishing to outbox or .DLT topics.
 */
public class OutboxPayloadSerializer implements Serializer<Object> {

    public static final String OBJECT_MAPPER_CONFIG = "idembox.object-mapper";

    private static final ObjectMapper DEFAULT_OBJECT_MAPPER = new ObjectMapper()
            .findAndRegisterModules()
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private ObjectMapper objectMapper;

    public OutboxPayloadSerializer() {
        this(null);
    }

    public OutboxPayloadSerializer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper != null ? objectMapper : DEFAULT_OBJECT_MAPPER;
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        if (configs != null && configs.containsKey(OBJECT_MAPPER_CONFIG)) {
            Object mapper = configs.get(OBJECT_MAPPER_CONFIG);
            if (mapper instanceof ObjectMapper om) {
                this.objectMapper = om;
            }
        }
    }

    @Override
    public byte[] serialize(String topic, Object data) {
        if (data == null) {
            return null;
        }
        if (data instanceof byte[] bytes) {
            return bytes;
        }
        if (data instanceof String str) {
            return str.getBytes(StandardCharsets.UTF_8);
        }
        try {
            return (objectMapper != null ? objectMapper : DEFAULT_OBJECT_MAPPER).writeValueAsBytes(data);
        } catch (JsonProcessingException e) {
            throw new SerializationException(
                    "Failed to serialize payload of type: " + data.getClass().getName() + " to JSON", e);
        }
    }

    @Override
    public void close() {
        // No resources to close
    }
}

