package io.github.alexlitvinov.sample.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.github.alexlitvinov.idembox.core.repository.IdempotentRepository;
import io.github.alexlitvinov.idembox.starter.consumer.IdemboxDeadLetterPublishingRecoverer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;

/**
 * Kafka consumer error handler configuration using idembox starter components.
 */
@Configuration
public class KafkaConsumerConfig {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .findAndRegisterModules()
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    @Bean
    public CommonErrorHandler commonErrorHandler(
            KafkaOperations<Object, Object> kafkaOperations,
            ObjectProvider<IdempotentRepository> idempotentRepositoryProvider) {
        DeadLetterPublishingRecoverer recoverer = createRecoverer(kafkaOperations, idempotentRepositoryProvider);
        return IdemboxDeadLetterPublishingRecoverer.createDefaultErrorHandler(recoverer);
    }

    DeadLetterPublishingRecoverer createRecoverer(
            KafkaOperations<Object, Object> kafkaOperations,
            ObjectProvider<IdempotentRepository> idempotentRepositoryProvider) {
        return new IdemboxDeadLetterPublishingRecoverer(kafkaOperations, idempotentRepositoryProvider);
    }

    String extractConsumerGroup(ConsumerRecord<?, ?> record) {
        return IdemboxDeadLetterPublishingRecoverer.extractConsumerGroup(record);
    }

    String extractEventKey(ConsumerRecord<?, ?> record) {
        return IdemboxDeadLetterPublishingRecoverer.extractEventKey(record, null, OBJECT_MAPPER);
    }

    String extractEventKey(ConsumerRecord<?, ?> record, Exception ex) {
        return IdemboxDeadLetterPublishingRecoverer.extractEventKey(record, ex, OBJECT_MAPPER);
    }

    byte[] extractFailedDeserializationBytes(ConsumerRecord<?, ?> record, Throwable ex) {
        return IdemboxDeadLetterPublishingRecoverer.extractFailedDeserializationBytes(record, ex);
    }

    byte[] extractFailedKeyDeserializationBytes(ConsumerRecord<?, ?> record, Throwable ex) {
        return IdemboxDeadLetterPublishingRecoverer.extractFailedKeyDeserializationBytes(record, ex);
    }

    public ProducerRecord<Object, Object> prepareReplayRecord(
            ConsumerRecord<Object, Object> dltRecord,
            String targetTopic,
            IdempotentRepository repository) {
        return IdemboxDeadLetterPublishingRecoverer.prepareReplayRecord(dltRecord, targetTopic, repository, OBJECT_MAPPER);
    }

    public static String stripDltSuffix(String topic) {
        return IdemboxDeadLetterPublishingRecoverer.stripDltSuffix(topic);
    }

    public static boolean isConcurrentProcessingException(Throwable ex) {
        return IdemboxDeadLetterPublishingRecoverer.isConcurrentProcessingException(ex);
    }
}
