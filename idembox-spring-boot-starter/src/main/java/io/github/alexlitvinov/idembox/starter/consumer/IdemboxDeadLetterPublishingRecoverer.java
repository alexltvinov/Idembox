package io.github.alexlitvinov.idembox.starter.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.github.alexlitvinov.idembox.core.model.ConcurrentProcessingException;
import io.github.alexlitvinov.idembox.core.model.DuplicateMessageException;
import io.github.alexlitvinov.idembox.core.repository.IdempotentRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.core.NestedExceptionUtils;
import org.springframework.expression.EvaluationException;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.kafka.support.KafkaUtils;
import org.springframework.kafka.support.serializer.DeserializationException;
import org.springframework.messaging.converter.MessageConversionException;
import org.springframework.messaging.handler.annotation.support.MethodArgumentNotValidException;
import org.springframework.messaging.handler.invocation.MethodArgumentResolutionException;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import io.github.alexlitvinov.idembox.starter.aop.IdempotentExceptionClassifier;
import io.github.alexlitvinov.idembox.starter.aop.IdempotentMetadataException;
import io.github.alexlitvinov.idembox.starter.config.IdemboxProperties;

/**
 * Enterprise Dead Letter Topic (DLT) recoverer tightly integrated with {@link IdempotentRepository}.
 * <p>
 * Key capabilities:
 * <ul>
 *   <li>Skips DLT publication and terminal failure marking for {@link DuplicateMessageException}
 *       (e.g., when {@code swallowDuplicate = false} on {@code @Idempotent}), preventing DLQ pollution.</li>
 *   <li>Detects {@link ConcurrentProcessingException} (e.g. partition rebalances or lock lease overlaps)
 *       and forwards the record to DLT for deferred replay <em>without</em> marking it terminally failed.</li>
 *   <li>Automatically marks unrecoverable errors as terminally failed in {@link IdempotentRepository}
 *       to prevent endless replay loops on topic offset resets.</li>
 *   <li>Extracts idempotency event keys from headers, JSON payloads, POJOs, or raw deserialization exception streams.</li>
 *   <li>Provides helper {@link #prepareReplayRecord} to replay dead-lettered records while resetting failure status.</li>
 * </ul>
 */
public class IdemboxDeadLetterPublishingRecoverer extends DeadLetterPublishingRecoverer {

    private static final Logger log = LoggerFactory.getLogger(IdemboxDeadLetterPublishingRecoverer.class);
    private static final ObjectMapper DEFAULT_OBJECT_MAPPER = new ObjectMapper()
            .findAndRegisterModules()
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private final ObjectProvider<IdempotentRepository> repositoryProvider;
    private final IdempotentRepository directRepository;
    private final ObjectMapper objectMapper;
    private final Duration terminalFailureRetention;
    private final List<String> keyFields;
    private final IdemboxProperties.DltProperties dltProperties;

    public IdemboxDeadLetterPublishingRecoverer(
            KafkaOperations<?, ?> kafkaOperations,
            ObjectProvider<IdempotentRepository> repositoryProvider) {
        this(kafkaOperations, repositoryProvider, null, null, Duration.ofDays(7));
    }

    public IdemboxDeadLetterPublishingRecoverer(
            KafkaOperations<?, ?> kafkaOperations,
            IdempotentRepository directRepository) {
        this(kafkaOperations, null, directRepository, null, Duration.ofDays(7));
    }

    public IdemboxDeadLetterPublishingRecoverer(
            KafkaOperations<?, ?> kafkaOperations,
            ObjectProvider<IdempotentRepository> repositoryProvider,
            IdempotentRepository directRepository,
            ObjectMapper objectMapper,
            Duration terminalFailureRetention) {
        this(kafkaOperations, repositoryProvider, directRepository, objectMapper, terminalFailureRetention, null, null);
    }

    public IdemboxDeadLetterPublishingRecoverer(
            KafkaOperations<?, ?> kafkaOperations,
            ObjectProvider<IdempotentRepository> repositoryProvider,
            IdempotentRepository directRepository,
            ObjectMapper objectMapper,
            Duration terminalFailureRetention,
            List<String> keyFields,
            IdemboxProperties.DltProperties dltProperties) {
        this(new RecovererContext(kafkaOperations, repositoryProvider, directRepository, objectMapper, terminalFailureRetention, keyFields, dltProperties));
    }

    private IdemboxDeadLetterPublishingRecoverer(RecovererContext ctx) {
        super(ctx.kafkaOperations(),
                (record, ex) -> resolveDestination(record, ex, ctx.repositoryProvider(), ctx.directRepository(), ctx.objectMapper(), ctx.terminalFailureRetention(), ctx.keyFields(), ctx.dltProperties()));
        this.repositoryProvider = ctx.repositoryProvider();
        this.directRepository = ctx.directRepository();
        this.objectMapper = ctx.objectMapper() != null ? ctx.objectMapper() : DEFAULT_OBJECT_MAPPER;
        this.terminalFailureRetention = ctx.terminalFailureRetention() != null ? ctx.terminalFailureRetention() : Duration.ofDays(7);
        this.keyFields = ctx.keyFields() != null ? new ArrayList<>(ctx.keyFields()) : new ArrayList<>(List.of("id", "orderId", "key"));
        this.dltProperties = ctx.dltProperties() != null ? ctx.dltProperties() : new IdemboxProperties.DltProperties();
    }

    private record RecovererContext(
            KafkaOperations<?, ?> kafkaOperations,
            ObjectProvider<IdempotentRepository> repositoryProvider,
            IdempotentRepository directRepository,
            ObjectMapper objectMapper,
            Duration terminalFailureRetention,
            List<String> keyFields,
            IdemboxProperties.DltProperties dltProperties
    ) {}

    public static TopicPartition resolveDestination(
            ConsumerRecord<?, ?> record,
            Exception ex,
            ObjectProvider<IdempotentRepository> repositoryProvider,
            IdempotentRepository directRepository,
            ObjectMapper objectMapper,
            Duration terminalFailureRetention,
            List<String> keyFields,
            IdemboxProperties.DltProperties dltProperties) {

        Throwable root = ex != null ? NestedExceptionUtils.getMostSpecificCause(ex) : null;
        if (root instanceof DuplicateMessageException) {
            log.info("Duplicate message detected for record key={}, topic={}, partition={}, offset={}. Skipping DLT publication and terminal failure marking.",
                    record.key(), record.topic(), record.partition(), record.offset());
            return null;
        }

        String baseTopic = stripDltSuffix(record.topic(), dltProperties != null ? dltProperties.getTopicSuffix() : null);
        String dltTopic = (dltProperties != null)
                ? dltProperties.resolveDltTopic(baseTopic)
                : baseTopic + ".DLT";

        if (isConcurrentProcessingException(ex)) {
            log.warn("Concurrent processing lock active for record key={}, topic={}, partition={}, offset={}. " +
                            "Skipping terminal failure marking and forwarding to {} for safe replay after lease expiry.",
                    record.key(), record.topic(), record.partition(), record.offset(), dltTopic);
            return new TopicPartition(dltTopic, -1);
        }

        IdempotentRepository repository = directRepository != null
                ? directRepository
                : (repositoryProvider != null ? repositoryProvider.getIfAvailable() : null);

        if (repository != null) {
            String eventKey = extractEventKey(record, ex, objectMapper, keyFields);
            if (eventKey != null && !eventKey.isBlank()) {
                String consumerGroup = extractConsumerGroup(record, ex);
                String reason = ex != null ? ex.getMessage() : "Terminal consumer failure";
                Duration retention = terminalFailureRetention != null ? terminalFailureRetention : Duration.ofDays(7);
                repository.markTerminalFailed(eventKey, consumerGroup, reason, retention);
            }
        }
        return new TopicPartition(dltTopic, -1);
    }

    public IdempotentRepository getRepository() {
        return directRepository != null ? directRepository : (repositoryProvider != null ? repositoryProvider.getIfAvailable() : null);
    }

    public Duration getTerminalFailureRetention() {
        return terminalFailureRetention;
    }

    public String extractEventKey(ConsumerRecord<?, ?> record) {
        return extractEventKey(record, null, this.objectMapper, this.keyFields);
    }

    public static String extractEventKey(ConsumerRecord<?, ?> record, Throwable ex, ObjectMapper objectMapper) {
        return extractEventKey(record, ex, objectMapper, null);
    }

    public static String extractEventKey(
            ConsumerRecord<?, ?> record,
            Throwable ex,
            ObjectMapper objectMapper,
            List<String> customKeyFields) {
        if (record == null) {
            return null;
        }

        IdempotentMetadataException metadata = IdempotentExceptionClassifier.findMetadata(ex);
        if (metadata != null && metadata.getKey() != null && !metadata.getKey().isBlank()) {
            return metadata.getKey();
        }

        ObjectMapper mapper = objectMapper != null ? objectMapper : DEFAULT_OBJECT_MAPPER;
        boolean valueDeserializationFailed = hasValueDeserializationFailure(record, ex);

        List<String> effectiveKeyFields = (customKeyFields != null && !customKeyFields.isEmpty())
                ? customKeyFields
                : List.of("id", "orderId", "key");

        if (record.value() != null) {
            String payloadKey = extractKeyFromPayloadObject(record.value(), mapper, effectiveKeyFields);
            if (payloadKey != null && !payloadKey.isBlank()) {
                return payloadKey;
            }
        } else if (valueDeserializationFailed) {
            byte[] rawData = extractFailedDeserializationBytes(record, ex);
            if (rawData != null && rawData.length > 0) {
                String recoveredKey = extractKeyFromPayloadObject(rawData, mapper, effectiveKeyFields);
                if (recoveredKey != null && !recoveredKey.isBlank()) {
                    return recoveredKey;
                }
            }
        }

        if (record.headers() != null) {
            List<String> candidateHeaders = new ArrayList<>(List.of(
                    "X-Idempotent-Key", "x-idempotent-key", "X-Idempotency-Key", "idempotency-key", "idempotencyKey"));
            for (String kf : effectiveKeyFields) {
                if (!candidateHeaders.contains(kf)) {
                    candidateHeaders.add(kf);
                }
            }
            for (String headerName : candidateHeaders) {
                Header header = record.headers().lastHeader(headerName);
                if (header != null && header.value() != null) {
                    String val = new String(header.value(), StandardCharsets.UTF_8).trim();
                    if (!val.isBlank()) {
                        return val;
                    }
                }
            }
        }

        if (valueDeserializationFailed) {
            return null;
        }

        // Natural fallback to record.key() when headers/payload do not supply key (Issue 4 resolved)
        if (record.key() != null) {
            if (record.key() instanceof byte[] bytes) {
                return HexFormat.of().formatHex(bytes);
            }
            String keyStr = record.key().toString().trim();
            if (!keyStr.isBlank()) {
                return keyStr;
            }
        }
        return null;
    }

    public static String extractConsumerGroup(ConsumerRecord<?, ?> record) {
        return extractConsumerGroup(record, null);
    }

    public static String extractConsumerGroup(ConsumerRecord<?, ?> record, Throwable ex) {
        IdempotentMetadataException metadata = IdempotentExceptionClassifier.findMetadata(ex);
        if (metadata != null && metadata.getConsumerGroup() != null && !metadata.getConsumerGroup().isBlank()) {
            return metadata.getConsumerGroup();
        }
        if (record != null && record.headers() != null) {
            for (String headerName : List.of("X-Idempotent-Group", "x-idempotent-group", "kafka_dlt-original-consumer-group")) {
                Header dltGroupHeader = record.headers().lastHeader(headerName);
                if (dltGroupHeader != null && dltGroupHeader.value() != null) {
                    String originalGroup = new String(dltGroupHeader.value(), StandardCharsets.UTF_8).trim();
                    if (!originalGroup.isBlank()) {
                        return originalGroup;
                    }
                }
            }
        }
        if (ex instanceof ListenerExecutionFailedException listenerEx && listenerEx.getGroupId() != null && !listenerEx.getGroupId().isBlank()) {
            return listenerEx.getGroupId().trim();
        }
        try {
            String currentGroupId = KafkaUtils.getConsumerGroupId();
            if (currentGroupId != null && !currentGroupId.isBlank()) {
                return currentGroupId;
            }
        } catch (Throwable ignored) {
        }
        String effectiveTopic = record != null ? stripDltSuffix(record.topic()) : null;
        return effectiveTopic != null ? effectiveTopic + "-group" : "default-consumer-group";
    }

    public static boolean isConcurrentProcessingException(Throwable ex) {
        Throwable current = ex;
        while (current != null) {
            if (current instanceof ConcurrentProcessingException) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    public static String stripDltSuffix(String topic) {
        return stripDltSuffix(topic, null);
    }

    public static String stripDltSuffix(String topic, String customSuffix) {
        if (topic == null) {
            return null;
        }
        String t = topic;
        if (customSuffix != null && !customSuffix.isBlank() && t.endsWith(customSuffix)) {
            t = t.substring(0, t.length() - customSuffix.length());
        }
        for (String s : List.of(".dlt", "-dlt", ".dlq", "-dlq")) {
            while (t.length() >= s.length() && t.regionMatches(true, t.length() - s.length(), s, 0, s.length())) {
                t = t.substring(0, t.length() - s.length());
            }
        }
        return t;
    }

    public ProducerRecord<Object, Object> prepareReplayRecord(
            ConsumerRecord<Object, Object> dltRecord,
            String targetTopic) {
        return prepareReplayRecord(dltRecord, targetTopic, getRepository(), this.objectMapper, this.keyFields,
                this.dltProperties != null ? this.dltProperties.getTopicSuffix() : null);
    }

    public static ProducerRecord<Object, Object> prepareReplayRecord(
            ConsumerRecord<Object, Object> dltRecord,
            String targetTopic,
            IdempotentRepository repository,
            ObjectMapper objectMapper) {
        return prepareReplayRecord(dltRecord, targetTopic, repository, objectMapper, null, null);
    }

    public static ProducerRecord<Object, Object> prepareReplayRecord(
            ConsumerRecord<Object, Object> dltRecord,
            String targetTopic,
            IdempotentRepository repository,
            ObjectMapper objectMapper,
            List<String> keyFields,
            String customDltSuffix) {
        ObjectMapper mapper = objectMapper != null ? objectMapper : DEFAULT_OBJECT_MAPPER;
        String eventKey = extractEventKey(dltRecord, null, mapper, keyFields);
        String consumerGroup = extractConsumerGroup(dltRecord);
        if (repository != null && eventKey != null) {
            repository.resetFailed(eventKey, consumerGroup);
        }
        String topic = targetTopic != null ? targetTopic : (dltRecord != null && dltRecord.topic() != null ? stripDltSuffix(dltRecord.topic(), customDltSuffix) : null);
        if (topic == null || topic.isBlank()) {
            throw new IllegalArgumentException("Target topic must be specified or present in DLT record");
        }
        boolean valueDeserializationFailed = hasValueDeserializationFailure(dltRecord, null);
        boolean keyDeserializationFailed = hasKeyDeserializationFailure(dltRecord, null);
        Object keyToReplay = dltRecord.key();
        if (keyToReplay == null && keyDeserializationFailed) {
            keyToReplay = extractFailedKeyDeserializationBytes(dltRecord, null);
        }
        Object payloadToReplay = dltRecord.value();
        if (payloadToReplay == null && valueDeserializationFailed) {
            payloadToReplay = extractFailedDeserializationBytes(dltRecord, null);
        }
        ProducerRecord<Object, Object> record =
                new ProducerRecord<>(topic, keyToReplay, payloadToReplay);
        if (dltRecord.headers() != null) {
            dltRecord.headers().forEach(h -> {
                String key = h.key();
                if (key != null
                        && !key.startsWith("kafka_dlt-")
                        && !key.equals("springDeserializerExceptionValue")
                        && !key.equals("springDeserializerExceptionKey")
                        && !key.equalsIgnoreCase("X-Idempotent-Replay")
                        && !(valueDeserializationFailed && (key.equals("__TypeId__") || key.equals("__ContentTypeId__")))
                        && !(keyDeserializationFailed && key.equals("__KeyTypeId__"))) {
                    record.headers().add(h);
                }
            });
        }
        record.headers().add("X-Idempotent-Replay", "true".getBytes(StandardCharsets.UTF_8));
        if (consumerGroup != null && !consumerGroup.isBlank()) {
            boolean hasGroupHeader = false;
            for (Header h : record.headers()) {
                if ("X-Idempotent-Group".equalsIgnoreCase(h.key())) {
                    hasGroupHeader = true;
                    break;
                }
            }
            if (!hasGroupHeader) {
                record.headers().add("X-Idempotent-Group", consumerGroup.getBytes(StandardCharsets.UTF_8));
            }
        }
        if (eventKey != null && !eventKey.isBlank()) {
            boolean hasKeyHeader = false;
            for (Header h : record.headers()) {
                if ("X-Idempotent-Key".equalsIgnoreCase(h.key())) {
                    hasKeyHeader = true;
                    break;
                }
            }
            if (!hasKeyHeader) {
                record.headers().add("X-Idempotent-Key", eventKey.getBytes(StandardCharsets.UTF_8));
            }
        }
        validateReplayRecordSize(record, topic);
        return record;
    }

    public static final long MAX_REPLAY_MESSAGE_BYTES = 1_048_576L; // 1 MB standard Kafka broker limit

    static void validateReplayRecordSize(ProducerRecord<Object, Object> record, String topic) {
        long headerSizeBytes = 0;
        if (record.headers() != null) {
            for (Header h : record.headers()) {
                if (h.key() != null) {
                    headerSizeBytes += h.key().getBytes(StandardCharsets.UTF_8).length;
                }
                if (h.value() != null) {
                    headerSizeBytes += h.value().length;
                }
            }
        }
        long payloadSizeBytes = 0;
        if (record.value() instanceof byte[] b) {
            payloadSizeBytes = b.length;
        } else if (record.value() instanceof String s) {
            payloadSizeBytes = s.getBytes(StandardCharsets.UTF_8).length;
        }
        long keySizeBytes = 0;
        if (record.key() instanceof byte[] b) {
            keySizeBytes = b.length;
        } else if (record.key() instanceof String s) {
            keySizeBytes = s.getBytes(StandardCharsets.UTF_8).length;
        }

        long totalBytes = payloadSizeBytes + keySizeBytes + headerSizeBytes;
        if (totalBytes > MAX_REPLAY_MESSAGE_BYTES) {
            log.warn("Replay message size of {} bytes (payload: {} bytes, headers: {} bytes) for topic '{}' exceeds standard broker limit of {} bytes (1 MB). Publication may fail with RecordTooLargeException.",
                    totalBytes, payloadSizeBytes, headerSizeBytes, topic, MAX_REPLAY_MESSAGE_BYTES);
        }
    }

    private static String extractKeyFromPayloadObject(Object val, ObjectMapper mapper, List<String> keyFields) {
        if (val == null) {
            return null;
        }
        try {
            JsonNode root;
            if (val instanceof String str) {
                root = mapper.readTree(str);
            } else if (val instanceof byte[] bytes) {
                root = mapper.readTree(bytes);
            } else {
                root = mapper.valueToTree(val);
            }
            if (root != null && root.isObject()) {
                List<String> fieldsToCheck = (keyFields != null && !keyFields.isEmpty())
                        ? keyFields
                        : List.of("id", "orderId", "key");
                for (String fieldName : fieldsToCheck) {
                    if (root.hasNonNull(fieldName)) {
                        String fieldVal = root.get(fieldName).asText();
                        if (fieldVal != null && !fieldVal.isBlank()) {
                            return fieldVal.trim();
                        }
                    }
                }
            }
        } catch (Exception ignored) {
        }
        return null;
    }

    public static boolean hasValueDeserializationFailure(ConsumerRecord<?, ?> record, Throwable ex) {
        Throwable current = ex;
        while (current != null) {
            if (current instanceof DeserializationException deserializationEx) {
                return !deserializationEx.isKey();
            }
            current = current.getCause();
        }
        if (record != null && record.headers() != null) {
            if (record.headers().lastHeader("springDeserializerExceptionValue") != null) {
                return true;
            }
            if (record.headers().lastHeader("springDeserializerExceptionKey") != null) {
                return false;
            }
            if ((record.headers().lastHeader("kafka_dlt-key-exception-fqcn") != null
                    || record.headers().lastHeader("kafka_dlt-key-exception-cause-fqcn") != null)
                    && record.value() != null) {
                return false;
            }
            for (String dltExHeader : new String[]{"kafka_dlt-exception-fqcn", "kafka_dlt-exception-cause-fqcn"}) {
                Header h = record.headers().lastHeader(dltExHeader);
                if (h != null && h.value() != null) {
                    String exceptionClassName = new String(h.value(), StandardCharsets.UTF_8);
                    if (exceptionClassName.contains("DeserializationException")) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    public static boolean hasKeyDeserializationFailure(ConsumerRecord<?, ?> record, Throwable ex) {
        Throwable current = ex;
        while (current != null) {
            if (current instanceof DeserializationException deserializationEx) {
                return deserializationEx.isKey();
            }
            current = current.getCause();
        }
        if (record != null && record.headers() != null) {
            if (record.headers().lastHeader("springDeserializerExceptionKey") != null) {
                return true;
            }
            for (String dltKeyExHeader : new String[]{"kafka_dlt-key-exception-fqcn", "kafka_dlt-key-exception-cause-fqcn"}) {
                Header h = record.headers().lastHeader(dltKeyExHeader);
                if (h != null && h.value() != null) {
                    String exceptionClassName = new String(h.value(), StandardCharsets.UTF_8);
                    if (exceptionClassName.contains("DeserializationException")) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    public static byte[] extractFailedDeserializationBytes(ConsumerRecord<?, ?> record, Throwable ex) {
        Throwable current = ex;
        while (current != null) {
            if (current instanceof DeserializationException deserializationEx && !deserializationEx.isKey()) {
                if (deserializationEx.getData() != null && deserializationEx.getData().length > 0) {
                    return deserializationEx.getData();
                }
            }
            current = current.getCause();
        }
        if (record != null && record.headers() != null) {
            Header header = record.headers().lastHeader("springDeserializerExceptionValue");
            if (header != null && header.value() != null && header.value().length > 0) {
                return extractPayloadBytesWithoutDeserialization(header.value());
            }
        }
        return null;
    }

    public static byte[] extractFailedKeyDeserializationBytes(ConsumerRecord<?, ?> record, Throwable ex) {
        Throwable current = ex;
        while (current != null) {
            if (current instanceof DeserializationException deserializationEx && deserializationEx.isKey()) {
                if (deserializationEx.getData() != null && deserializationEx.getData().length > 0) {
                    return deserializationEx.getData();
                }
            }
            current = current.getCause();
        }
        if (record != null && record.headers() != null) {
            Header header = record.headers().lastHeader("springDeserializerExceptionKey");
            if (header != null && header.value() != null && header.value().length > 0) {
                return extractPayloadBytesWithoutDeserialization(header.value());
            }
        }
        return null;
    }

    static byte[] extractPayloadBytesWithoutDeserialization(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return null;
        }
        if (bytes.length < 4 || (bytes[0] & 0xFF) != 0xAC || (bytes[1] & 0xFF) != 0xED) {
            return bytes;
        }

        // Scan for byte array pattern: TC_ARRAY (0x75), class descriptor (0x72), UTF len (0x00, 0x02), "[B" (0x5B, 0x42)
        byte[] pattern = new byte[]{0x75, 0x72, 0x00, 0x02, 0x5B, 0x42};
        int idx = -1;
        for (int i = 4; i <= bytes.length - pattern.length; i++) {
            boolean match = true;
            for (int j = 0; j < pattern.length; j++) {
                if (bytes[i + j] != pattern[j]) {
                    match = false;
                    break;
                }
            }
            if (match) {
                idx = i;
                break;
            }
        }

        if (idx != -1) {
            // After pattern: 8 bytes (UID) + 1 byte (flags) + 2 bytes (numFields) + 1 byte (0x78) + 1 byte (0x70) = 13 bytes
            int lengthPos = idx + pattern.length + 13;
            if (lengthPos + 4 <= bytes.length) {
                int len = ((bytes[lengthPos] & 0xFF) << 24)
                        | ((bytes[lengthPos + 1] & 0xFF) << 16)
                        | ((bytes[lengthPos + 2] & 0xFF) << 8)
                        | (bytes[lengthPos + 3] & 0xFF);
                int dataStart = lengthPos + 4;
                if (len >= 0 && dataStart + len <= bytes.length) {
                    byte[] payload = new byte[len];
                    System.arraycopy(bytes, dataStart, payload, 0, len);
                    return payload;
                }
            }
        }

        // Fallback: if byte array descriptor was referenced via TC_REFERENCE (0x71)
        for (int i = 4; i <= bytes.length - 10; i++) {
            if (bytes[i] == 0x75 && bytes[i + 1] == 0x71) {
                int lengthPos = i + 6;
                int len = ((bytes[lengthPos] & 0xFF) << 24)
                        | ((bytes[lengthPos + 1] & 0xFF) << 16)
                        | ((bytes[lengthPos + 2] & 0xFF) << 8)
                        | (bytes[lengthPos + 3] & 0xFF);
                int dataStart = lengthPos + 4;
                if (len >= 0 && dataStart + len <= bytes.length) {
                    byte[] payload = new byte[len];
                    System.arraycopy(bytes, dataStart, payload, 0, len);
                    return payload;
                }
            }
        }

        return null;
    }

    public static DefaultErrorHandler createDefaultErrorHandler(DeadLetterPublishingRecoverer recoverer) {
        return createDefaultErrorHandler(recoverer, 3, 1000L, 2.0, 10000L);
    }

    public static DefaultErrorHandler createDefaultErrorHandler(
            DeadLetterPublishingRecoverer recoverer,
            int maxRetries,
            long initialIntervalMs,
            double multiplier,
            long maxIntervalMs) {
        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(maxRetries);
        backOff.setInitialInterval(initialIntervalMs);
        backOff.setMultiplier(multiplier);
        backOff.setMaxInterval(maxIntervalMs);

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backOff);

        errorHandler.addNotRetryableExceptions(
                DeserializationException.class,
                SerializationException.class,
                RecordTooLargeException.class,
                MessageConversionException.class,
                MethodArgumentResolutionException.class,
                MethodArgumentNotValidException.class,
                NoSuchMethodException.class,
                ClassCastException.class,
                IllegalArgumentException.class,
                EvaluationException.class,
                IllegalStateException.class,
                DuplicateMessageException.class
        );
        addOptionalNotRetryableException(errorHandler, "jakarta.validation.ValidationException");
        addOptionalNotRetryableException(errorHandler, "javax.validation.ValidationException");
        addOptionalNotRetryableException(errorHandler, "org.springframework.web.bind.MethodArgumentNotValidException");

        return errorHandler;
    }

    private static void addOptionalNotRetryableException(DefaultErrorHandler errorHandler, String className) {
        try {
            Class<?> clazz = Class.forName(className, false, IdemboxDeadLetterPublishingRecoverer.class.getClassLoader());
            if (Exception.class.isAssignableFrom(clazz)) {
                errorHandler.addNotRetryableExceptions(clazz.asSubclass(Exception.class));
            }
        } catch (Throwable ignored) {
        }
    }
}
