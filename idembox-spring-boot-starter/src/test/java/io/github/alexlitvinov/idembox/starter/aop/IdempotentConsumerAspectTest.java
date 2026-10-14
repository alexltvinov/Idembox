package io.github.alexlitvinov.idembox.starter.aop;

import io.github.alexlitvinov.idembox.core.annotation.Idempotent;
import io.github.alexlitvinov.idembox.core.model.AcquireResult;
import io.github.alexlitvinov.idembox.core.model.ConcurrentProcessingException;
import io.github.alexlitvinov.idembox.core.model.DuplicateMessageException;
import io.github.alexlitvinov.idembox.core.model.IdempotencyException;
import io.github.alexlitvinov.idembox.core.model.ProcessingState;
import io.github.alexlitvinov.idembox.core.model.ProcessingStatus;
import io.github.alexlitvinov.idembox.core.repository.IdempotentRepository;
import io.github.alexlitvinov.idembox.core.spel.SpelKeyExtractor;
import io.github.alexlitvinov.idembox.starter.config.IdemboxProperties;
import io.github.alexlitvinov.idembox.starter.metrics.IdemboxMetrics;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.reflect.MethodSignature;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.support.KafkaHeaders;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class IdempotentConsumerAspectTest {

    @Mock
    private IdempotentRepository idempotentRepository;

    @Mock
    private ProceedingJoinPoint joinPoint;

    @Mock
    private MethodSignature methodSignature;

    @Mock
    private Idempotent idempotent;

    private SpelKeyExtractor spelKeyExtractor;
    private IdemboxProperties properties;
    private IdempotentConsumerAspect aspect;

    @SafeVarargs
    private static Class<? extends Throwable>[] exceptionTypes(Class<? extends Throwable>... types) {
        return types;
    }

    static class TestService {
        public int computePrimitiveInt(String id) { return id.length(); }
        public boolean computePrimitiveBoolean(String id) { return !id.isEmpty(); }
        public String handleString(String id) { return id; }
        public java.util.Optional<String> findOptionalString(String id) { return java.util.Optional.of(id); }
        public java.util.List<String> findList(String id) { return java.util.List.of(id); }
        public java.util.Set<String> findSet(String id) { return java.util.Set.of(id); }
        public java.util.Map<String, String> findMap(String id) { return java.util.Map.of("k", id); }
        public java.util.concurrent.CompletableFuture<String> asyncCompute(String id) { return java.util.concurrent.CompletableFuture.completedFuture(id); }
        public java.util.concurrent.CompletionStage<String> asyncStage(String id) { return java.util.concurrent.CompletableFuture.completedFuture(id); }
        public reactor.core.publisher.Mono<String> reactiveMonoMethod(String id) { return reactor.core.publisher.Mono.just(id); }
        public void processWithAcknowledgment(String id, org.springframework.kafka.support.Acknowledgment ack) {
            java.util.Objects.requireNonNull(id);
            ack.acknowledge();
        }
        @org.springframework.kafka.annotation.KafkaListener(topics = "orders", batch = "true")
        public void handleBatch(java.util.List<String> items) {
            java.util.Objects.requireNonNull(items);
        }
        @org.springframework.kafka.annotation.KafkaListener(topics = "orders", batch = "true")
        public void handleBatchWithAcknowledgment(org.springframework.kafka.support.Acknowledgment ack, java.util.List<String> items) {
            ack.acknowledge();
            java.util.Objects.requireNonNull(items);
        }
        public void handleBatchRecords(org.apache.kafka.clients.consumer.ConsumerRecord<String, String>[] records) {
            java.util.Objects.requireNonNull(records);
        }
        @org.springframework.kafka.annotation.KafkaListener(topics = "orders", batch = "true")
        public void handleIterable(Iterable<String> items) {
            java.util.Objects.requireNonNull(items);
        }
        public void handleBusinessList(String id, java.util.List<String> items) {
            java.util.Objects.requireNonNull(id);
            java.util.Objects.requireNonNull(items);
        }
        @org.springframework.kafka.annotation.KafkaListener(topics = "orders")
        public void handleSingleMessageWithList(java.util.List<String> items) {
            java.util.Objects.requireNonNull(items);
        }
        @org.springframework.kafka.annotation.KafkaListener(topics = "orders")
        public void handleSingleMessageWithArray(String[] items) {
            java.util.Objects.requireNonNull(items);
        }
        @org.springframework.kafka.annotation.KafkaListener(topics = "orders")
        public void handleBatchRecordsGeneric(java.util.List<org.apache.kafka.clients.consumer.ConsumerRecord<String, String>> records) {
            java.util.Objects.requireNonNull(records);
        }
        public void handleWithRecord(String payload, org.apache.kafka.clients.consumer.ConsumerRecord<String, String> record) {
            java.util.Objects.requireNonNull(payload);
            java.util.Objects.requireNonNull(record);
        }
        public void handleJson(com.fasterxml.jackson.databind.JsonNode node) {
            java.util.Objects.requireNonNull(node);
        }

        @org.springframework.transaction.annotation.Transactional
        public void transactionalMethodWithoutRollback(String id) throws Exception {
            throw new Exception("checked error in tx: " + id);
        }

        @org.springframework.transaction.annotation.Transactional(rollbackFor = Exception.class)
        public void transactionalMethodWithRollback(String id) throws Exception {
            throw new Exception("checked error with rollback: " + id);
        }

        @org.springframework.transaction.annotation.Transactional(noRollbackForClassName = "Validation")
        public void transactionalMethodWithNoRollbackForValidationName(String id) {
            throw new DatabaseValidationFailureException("DB validation fail: " + id);
        }

        @org.springframework.transaction.annotation.Transactional(noRollbackFor = CustomNoRollbackException.class)
        public void transactionalMethodWithNoRollbackForRuntimeException(String id) {
            throw new CustomNoRollbackException("Expected business error that commits: " + id);
        }

        public void nonTransactionalMethodThrowingChecked(String id) throws Exception {
            throw new Exception("checked error outside tx: " + id);
        }

        @org.springframework.messaging.handler.annotation.SendTo("reply-topic")
        public String replyMethod(String id) { return "reply:" + id; }

        @org.springframework.transaction.annotation.Transactional
        public java.util.concurrent.CompletableFuture<String> asyncTransactionalMethod(String id) {
            return java.util.concurrent.CompletableFuture.completedFuture(id);
        }

        @jakarta.transaction.Transactional
        public void jakartaTransactionalMethodWithoutRollback(String id) throws Exception {
            throw new Exception("checked error in JTA tx: " + id);
        }

        @jakarta.transaction.Transactional(rollbackOn = Exception.class)
        public void jakartaTransactionalMethodWithRollback(String id) throws Exception {
            throw new Exception("checked error with JTA rollback: " + id);
        }

        @jakarta.transaction.Transactional(dontRollbackOn = CustomNoRollbackException.class)
        public void jakartaTransactionalMethodWithNoRollbackOn(String id) {
            throw new CustomNoRollbackException("Expected business error in JTA tx that commits: " + id);
        }

        @jakarta.transaction.Transactional
        public java.util.concurrent.CompletableFuture<String> jakartaAsyncTransactionalMethod(String id) {
            return java.util.concurrent.CompletableFuture.completedFuture(id);
        }

        @org.springframework.transaction.annotation.Transactional(
                rollbackFor = SpecificBusinessException.class,
                noRollbackFor = RuntimeException.class
        )
        public void transactionalMethodWithSpecificRollbackAndGeneralNoRollback(String id) {
            java.util.Objects.requireNonNull(id);
        }

        @jakarta.transaction.Transactional(
                rollbackOn = SpecificBusinessException.class,
                dontRollbackOn = RuntimeException.class
        )
        public void jakartaMethodWithSpecificRollbackAndGeneralNoRollback(String id) {
            java.util.Objects.requireNonNull(id);
        }

        @org.springframework.transaction.annotation.Transactional(propagation = org.springframework.transaction.annotation.Propagation.NOT_SUPPORTED)
        public void transactionalMethodWithNotSupported(String id) throws Exception {
            throw new Exception("checked error in not supported tx: " + id);
        }

        @org.springframework.transaction.annotation.Transactional(propagation = org.springframework.transaction.annotation.Propagation.NEVER)
        public void transactionalMethodWithNever(String id) throws Exception {
            throw new Exception("checked error in never tx: " + id);
        }

        @jakarta.transaction.Transactional(jakarta.transaction.Transactional.TxType.NOT_SUPPORTED)
        public void jakartaTransactionalMethodWithNotSupported(String id) throws Exception {
            throw new Exception("checked error in JTA not supported tx: " + id);
        }

        @jakarta.transaction.Transactional(jakarta.transaction.Transactional.TxType.NEVER)
        public void jakartaTransactionalMethodWithNever(String id) throws Exception {
            throw new Exception("checked error in JTA never tx: " + id);
        }

        @org.springframework.transaction.annotation.Transactional
        public void transactionalMethodWithTerminalCheckedException(String id) throws CustomTerminalCheckedException {
            throw new CustomTerminalCheckedException("terminal checked error: " + id);
        }
    }

    static class CustomTerminalCheckedException extends Exception {
        public CustomTerminalCheckedException(String msg) {
            super(msg);
        }
    }

    interface BusinessMarkerInterface {}

    static class ExceptionImplementingMarkerInterface extends Exception implements BusinessMarkerInterface {
        public ExceptionImplementingMarkerInterface(String msg) {
            super(msg);
        }
    }

    static class SpecificBusinessException extends RuntimeException {
        public SpecificBusinessException(String msg) {
            super(msg);
        }
    }

    static class OtherBusinessException extends RuntimeException {
        public OtherBusinessException(String msg) {
            super(msg);
        }
    }

    static class DatabaseValidationFailureException extends RuntimeException {
        public DatabaseValidationFailureException(String msg) {
            super(msg);
        }
    }

    static class CustomNoRollbackException extends RuntimeException {
        public CustomNoRollbackException(String msg) {
            super(msg);
        }
    }

    @BeforeEach
    void setUp() {
        spelKeyExtractor = new SpelKeyExtractor();
        properties = new IdemboxProperties();
        properties.getIdempotency().setInFlightWaitTimeout(Duration.ofMillis(100)); // fast wait for test
        properties.getIdempotency().setFailOnExistingTransaction(false);

        aspect = new IdempotentConsumerAspect(idempotentRepository, spelKeyExtractor, properties, IdemboxMetrics.noOp());
        org.mockito.Mockito.lenient().when(idempotentRepository.markCompleted(any(), any(), any())).thenReturn(true);
        org.mockito.Mockito.lenient().when(idempotentRepository.markCompleted(any(), any(), any(), any())).thenReturn(true);
    }

    @org.junit.jupiter.api.AfterEach
    void tearDown() {
        org.springframework.transaction.support.TransactionSynchronizationManager.setActualTransactionActive(false);
        org.springframework.transaction.support.TransactionSynchronizationManager.clear();
    }

    @Test
    @DisplayName("Fix 3.2: Should safely return primitive default (0) instead of throwing NPE on duplicate")
    void shouldReturnDefaultPrimitiveIntOnDuplicate() throws Throwable {
        Method method = TestService.class.getMethod("computePrimitiveInt", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getArgs()).thenReturn(new Object[]{"ORD-123"});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#root.args[0]");
        when(idempotent.consumerGroup()).thenReturn("orders-group");
        when(idempotent.ttl()).thenReturn("7d");
        when(idempotent.lockTimeout()).thenReturn("2m");
        when(idempotent.swallowDuplicate()).thenReturn(true);

        when(idempotentRepository.tryAcquire(any(), any(), any(), any()))
                .thenReturn(AcquireResult.DUPLICATE_COMPLETED);

        Object result = aspect.enforceIdempotency(joinPoint, idempotent);

        assertThat(result).isEqualTo(0);
        verify(joinPoint, never()).proceed();
    }

    @Test
    @DisplayName("Fix 3.2: Should safely return primitive default (false) on duplicate boolean method")
    void shouldReturnDefaultPrimitiveBooleanOnDuplicate() throws Throwable {
        Method method = TestService.class.getMethod("computePrimitiveBoolean", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getArgs()).thenReturn(new Object[]{"ORD-123"});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0");
        when(idempotent.consumerGroup()).thenReturn("orders-group");
        when(idempotent.ttl()).thenReturn("7d");
        when(idempotent.lockTimeout()).thenReturn("2m");
        when(idempotent.swallowDuplicate()).thenReturn(true);

        when(idempotentRepository.tryAcquire(any(), any(), any(), any()))
                .thenReturn(AcquireResult.DUPLICATE_COMPLETED);

        Object result = aspect.enforceIdempotency(joinPoint, idempotent);

        assertThat(result).isEqualTo(false);
    }

    @Test
    @DisplayName("Fix 2.3: In-flight locked event should throw ConcurrentProcessingException to trigger Kafka retry")
    void shouldThrowConcurrentProcessingExceptionWhenInFlightLocked() throws NoSuchMethodException {
        Method method = TestService.class.getMethod("handleString", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getArgs()).thenReturn(new Object[]{"IN-FLIGHT-99"});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0");
        when(idempotent.consumerGroup()).thenReturn("group-a");
        when(idempotent.ttl()).thenReturn("7d");
        when(idempotent.lockTimeout()).thenReturn("2m");

        when(idempotentRepository.tryAcquire(any(), any(), any(), any()))
                .thenReturn(AcquireResult.IN_FLIGHT_LOCKED);

        assertThatThrownBy(() -> aspect.enforceIdempotency(joinPoint, idempotent))
                .isInstanceOf(ConcurrentProcessingException.class)
                .hasMessageContaining("currently being processed by another worker");
    }

    @Test
    @DisplayName("Should throw DuplicateMessageException when swallowDuplicate is false")
    void shouldThrowDuplicateMessageExceptionWhenNotSwallowed() throws NoSuchMethodException {
        Method method = TestService.class.getMethod("handleString", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getArgs()).thenReturn(new Object[]{"DUP-1"});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0");
        when(idempotent.consumerGroup()).thenReturn("group-a");
        when(idempotent.ttl()).thenReturn("7d");
        when(idempotent.lockTimeout()).thenReturn("2m");
        when(idempotent.swallowDuplicate()).thenReturn(false);

        when(idempotentRepository.tryAcquire(any(), any(), any(), any()))
                .thenReturn(AcquireResult.DUPLICATE_COMPLETED);

        assertThatThrownBy(() -> aspect.enforceIdempotency(joinPoint, idempotent))
                .isInstanceOf(DuplicateMessageException.class);
    }

    @Test
    @DisplayName("Should call markFailed and release lock when target business method throws exception")
    void shouldReleaseLockOnMethodFailure() throws Throwable {
        Method method = TestService.class.getMethod("handleString", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getArgs()).thenReturn(new Object[]{"FAIL-1"});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0");
        when(idempotent.consumerGroup()).thenReturn("group-a");
        when(idempotent.ttl()).thenReturn("7d");
        when(idempotent.lockTimeout()).thenReturn("2m");

        when(idempotentRepository.tryAcquire(any(), any(), any(), any()))
                .thenReturn(AcquireResult.ACQUIRED);
        when(joinPoint.proceed()).thenThrow(new RuntimeException("External API timeout"));

        assertThatThrownBy(() -> aspect.enforceIdempotency(joinPoint, idempotent))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("External API timeout");

        verify(idempotentRepository).markFailed(eq("FAIL-1"), eq("group-a"), any(), org.mockito.ArgumentMatchers.nullable(Instant.class));
        verify(idempotentRepository, never()).markCompleted(any(), any(), any());
        verify(idempotentRepository, never()).markCompleted(any(), any(), any(), any());
    }

    @Test
    @DisplayName("Refinement 2: In-flight wait polls getProcessingState and resolves to duplicate completed")
    void shouldResolveInFlightViaLightweightStatePolling() throws Throwable {
        properties.getIdempotency().setInFlightWaitTimeout(Duration.ofMillis(400));
        Method method = TestService.class.getMethod("handleString", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getArgs()).thenReturn(new Object[]{"POLL-1"});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0");
        when(idempotent.consumerGroup()).thenReturn("group-a");
        when(idempotent.ttl()).thenReturn("7d");
        when(idempotent.lockTimeout()).thenReturn("2m");
        when(idempotent.swallowDuplicate()).thenReturn(true);

        when(idempotentRepository.tryAcquire(any(), any(), any(), any()))
                .thenReturn(AcquireResult.IN_FLIGHT_LOCKED);

        // First poll: still processing; Second poll: completed!
        when(idempotentRepository.getProcessingState("POLL-1", "group-a"))
                .thenReturn(new ProcessingState(ProcessingStatus.PROCESSING, Instant.now().plusSeconds(60), Instant.now().plusSeconds(3600)))
                .thenReturn(new ProcessingState(ProcessingStatus.COMPLETED, null, Instant.now().plusSeconds(3600)));

        Object result = aspect.enforceIdempotency(joinPoint, idempotent);

        assertThat(result).isNull(); // swallowed duplicate for String return type
        verify(idempotentRepository, org.mockito.Mockito.times(1)).tryAcquire(any(), any(), any(), any());
        verify(idempotentRepository, org.mockito.Mockito.atLeast(2)).getProcessingState("POLL-1", "group-a");
        verify(joinPoint, never()).proceed();
    }

    @Test
    @DisplayName("Refinement 7: Should return Optional.empty() instead of null for Optional return types on duplicate")
    void shouldReturnOptionalEmptyOnDuplicate() throws Throwable {
        Method method = TestService.class.getMethod("findOptionalString", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getArgs()).thenReturn(new Object[]{"OPT-123"});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0");
        when(idempotent.consumerGroup()).thenReturn("orders-group");
        when(idempotent.ttl()).thenReturn("7d");
        when(idempotent.lockTimeout()).thenReturn("2m");
        when(idempotent.swallowDuplicate()).thenReturn(true);

        when(idempotentRepository.tryAcquire(any(), any(), any(), any()))
                .thenReturn(AcquireResult.DUPLICATE_COMPLETED);

        Object result = aspect.enforceIdempotency(joinPoint, idempotent);

        assertThat(result).isNotNull();
        assertThat(result).isEqualTo(java.util.Optional.empty());
        verify(joinPoint, never()).proceed();
    }

    @Test
    @DisplayName("Senior Refinements Round 64: Should retry markCompleted 3 times and throw IdempotencyException on failure")
    void shouldRetryAndReturnResultWhenMarkCompletedFailsAllAttempts() throws Throwable {
        Method method = TestService.class.getMethod("handleString", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getArgs()).thenReturn(new Object[]{"SAFE-1"});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0");
        when(idempotent.consumerGroup()).thenReturn("orders-group");
        when(idempotent.ttl()).thenReturn("7d");
        when(idempotent.lockTimeout()).thenReturn("2m");

        when(idempotentRepository.tryAcquire(any(), any(), any(), any()))
                .thenReturn(AcquireResult.ACQUIRED);
        when(joinPoint.proceed()).thenReturn("business-success");
        org.mockito.Mockito.doThrow(new RuntimeException("Transient DB error during markCompleted"))
                .when(idempotentRepository).markCompleted(any(), any(), any(), any());

        assertThatThrownBy(() -> aspect.enforceIdempotency(joinPoint, idempotent))
                .isInstanceOf(IdempotencyException.class)
                .hasMessageContaining("Failed to set COMPLETED status in idempotency store");

        verify(idempotentRepository, times(3)).markCompleted(eq("SAFE-1"), eq("orders-group"), any(), org.mockito.ArgumentMatchers.nullable(Instant.class));
        verify(idempotentRepository, never()).markFailed(any(), any(), any());
        verify(joinPoint, times(1)).proceed();
    }

    @Test
    @DisplayName("Fix 1b: Should succeed when markCompleted recovers on retry")
    void shouldRecoverWhenMarkCompletedSucceedsOnRetry() throws Throwable {
        Method method = TestService.class.getMethod("handleString", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getArgs()).thenReturn(new Object[]{"RETRY-1"});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0");
        when(idempotent.consumerGroup()).thenReturn("orders-group");
        when(idempotent.ttl()).thenReturn("7d");
        when(idempotent.lockTimeout()).thenReturn("2m");

        when(idempotentRepository.tryAcquire(any(), any(), any(), any()))
                .thenReturn(AcquireResult.ACQUIRED);
        when(joinPoint.proceed()).thenReturn("business-success");
        org.mockito.Mockito.doThrow(new RuntimeException("Temporary DB glitch"))
                .doReturn(true)
                .when(idempotentRepository).markCompleted(any(), any(), any(), any());

        Object result = aspect.enforceIdempotency(joinPoint, idempotent);

        assertThat(result).isEqualTo("business-success");
        verify(idempotentRepository, times(2)).markCompleted(eq("RETRY-1"), eq("orders-group"), any(), org.mockito.ArgumentMatchers.nullable(Instant.class));
        verify(idempotentRepository, never()).markFailed(any(), any(), any());
    }

    @Test
    @DisplayName("Fix 2: Should resolve property placeholders ${...} in consumerGroup, ttl, and lockTimeout")
    void shouldResolvePlaceholdersInConsumerGroupAndDurations() throws Throwable {
        aspect.setEmbeddedValueResolver(str -> switch (str) {
            case "${spring.kafka.consumer.group-id:orders-group}" -> "resolved-orders-group";
            case "${app.idempotency.ttl:7d}" -> "10d";
            case "${app.idempotency.lock:2m}" -> "45s";
            default -> str;
        });

        Method method = TestService.class.getMethod("handleString", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getArgs()).thenReturn(new Object[]{"PLACEHOLDER-1"});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0");
        when(idempotent.consumerGroup()).thenReturn("${spring.kafka.consumer.group-id:orders-group}");
        when(idempotent.ttl()).thenReturn("${app.idempotency.ttl:7d}");
        when(idempotent.lockTimeout()).thenReturn("${app.idempotency.lock:2m}");

        when(idempotentRepository.tryAcquire(any(), any(), any(), any()))
                .thenReturn(AcquireResult.ACQUIRED);
        when(joinPoint.proceed()).thenReturn("ok");

        Object result = aspect.enforceIdempotency(joinPoint, idempotent);

        assertThat(result).isEqualTo("ok");
        verify(idempotentRepository).tryAcquire(
                eq("PLACEHOLDER-1"),
                eq("resolved-orders-group"),
                eq(Duration.ofSeconds(45)),
                eq(Duration.ofDays(10))
        );
        verify(idempotentRepository).markCompleted(
                eq("PLACEHOLDER-1"),
                eq("resolved-orders-group"),
                eq(Duration.ofDays(10)),
                org.mockito.ArgumentMatchers.nullable(Instant.class)
        );
    }

    @Test
    @DisplayName("Fix 6: Should return empty collection instead of null for List, Set, Map, and completed Future on duplicate")
    void shouldReturnEmptyCollectionsAndFutureOnDuplicate() throws Throwable {
        when(idempotent.key()).thenReturn("#p0");
        when(idempotent.consumerGroup()).thenReturn("orders-group");
        when(idempotent.ttl()).thenReturn("7d");
        when(idempotent.lockTimeout()).thenReturn("2m");
        when(idempotent.swallowDuplicate()).thenReturn(true);
        when(idempotentRepository.tryAcquire(any(), any(), any(), any()))
                .thenReturn(AcquireResult.DUPLICATE_COMPLETED);

        // 1. List
        Method listMethod = TestService.class.getMethod("findList", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(listMethod);
        when(joinPoint.getArgs()).thenReturn(new Object[]{"DUP-LIST"});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        Object listResult = aspect.enforceIdempotency(joinPoint, idempotent);
        assertThat(listResult).isInstanceOf(java.util.List.class);
        assertThat((java.util.List<?>) listResult).isEmpty();

        // 2. Set
        Method setMethod = TestService.class.getMethod("findSet", String.class);
        when(methodSignature.getMethod()).thenReturn(setMethod);
        Object setResult = aspect.enforceIdempotency(joinPoint, idempotent);
        assertThat(setResult).isInstanceOf(java.util.Set.class);
        assertThat((java.util.Set<?>) setResult).isEmpty();

        // 3. Map
        Method mapMethod = TestService.class.getMethod("findMap", String.class);
        when(methodSignature.getMethod()).thenReturn(mapMethod);
        Object mapResult = aspect.enforceIdempotency(joinPoint, idempotent);
        assertThat(mapResult).isInstanceOf(java.util.Map.class);
        assertThat((java.util.Map<?, ?>) mapResult).isEmpty();

        // 4. CompletableFuture
        Method futureMethod = TestService.class.getMethod("asyncCompute", String.class);
        when(methodSignature.getMethod()).thenReturn(futureMethod);
        Object futureResult = aspect.enforceIdempotency(joinPoint, idempotent);
        assertThat(futureResult).isInstanceOf(java.util.concurrent.CompletableFuture.class);
        java.util.concurrent.CompletableFuture<?> future = (java.util.concurrent.CompletableFuture<?>) futureResult;
        assertThat(future.isDone()).isTrue();
        assertThat(future.get()).isNull();
    }

    @Test
    @DisplayName("Fix 2: Should resolve property placeholders ${...} in idempotent.key() expression")
    void shouldResolvePropertyPlaceholdersInKeyExpression() throws Throwable {
        aspect.setEmbeddedValueResolver(str -> {
            if ("'${app.tenant:default}-' + #p0".equals(str)) {
                return "'tenant-prod-' + #p0";
            }
            return str;
        });

        Method method = TestService.class.getMethod("handleString", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getArgs()).thenReturn(new Object[]{"ORDER-99"});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("'${app.tenant:default}-' + #p0");
        when(idempotent.consumerGroup()).thenReturn("orders-group");
        when(idempotent.ttl()).thenReturn("7d");
        when(idempotent.lockTimeout()).thenReturn("2m");

        when(idempotentRepository.tryAcquire(any(), any(), any(), any()))
                .thenReturn(AcquireResult.ACQUIRED);
        when(joinPoint.proceed()).thenReturn("ok");

        aspect.enforceIdempotency(joinPoint, idempotent);

        // Key expression must evaluate to 'tenant-prod-ORDER-99'
        verify(idempotentRepository).tryAcquire(eq("tenant-prod-ORDER-99"), eq("orders-group"), any(), any());
    }

    @Test
    @DisplayName("NTP Drift Fix: waitForInFlightCompletion retries acquisition when state isLocked is false")
    void shouldRetryAcquisitionWhenInFlightLockIsExpiredAccordingToDbState() throws Throwable {
        Method method = TestService.class.getMethod("handleString", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getArgs()).thenReturn(new Object[]{"INFLIGHT-1"});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0");
        when(idempotent.consumerGroup()).thenReturn("orders-group");
        when(idempotent.ttl()).thenReturn("7d");
        when(idempotent.lockTimeout()).thenReturn("2m");

        // 1st attempt: IN_FLIGHT_LOCKED. 2nd attempt after lock expired: ACQUIRED.
        when(idempotentRepository.tryAcquire(eq("INFLIGHT-1"), eq("orders-group"), any(), any()))
                .thenReturn(AcquireResult.IN_FLIGHT_LOCKED)
                .thenReturn(AcquireResult.ACQUIRED);

        // Database reports is_locked = false (lock expired on PostgreSQL server)
        ProcessingState state = new ProcessingState(ProcessingStatus.PROCESSING, null, null, false);
        when(idempotentRepository.getProcessingState(eq("INFLIGHT-1"), eq("orders-group")))
                .thenReturn(state);

        when(joinPoint.proceed()).thenReturn("ok");

        Object result = aspect.enforceIdempotency(joinPoint, idempotent);

        assertThat(result).isEqualTo("ok");
        verify(idempotentRepository, times(2)).tryAcquire(eq("INFLIGHT-1"), eq("orders-group"), any(), any());
        verify(joinPoint, times(1)).proceed();
    }

    @Test
    @DisplayName("Auto ConsumerGroup: Should fallback to DeclaringClass#methodName when consumerGroup is default or blank")
    void shouldFallbackToMethodNameWhenConsumerGroupIsDefaultOrBlank() throws Throwable {
        Method method = TestService.class.getMethod("handleString", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getArgs()).thenReturn(new Object[]{"KEY-AUTO-1"});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0");
        when(idempotent.consumerGroup()).thenReturn(""); // default value
        when(idempotent.ttl()).thenReturn("7d");
        when(idempotent.lockTimeout()).thenReturn("2m");

        when(idempotentRepository.tryAcquire(any(), any(), any(), any()))
                .thenReturn(AcquireResult.ACQUIRED);
        when(joinPoint.proceed()).thenReturn("ok");

        aspect.enforceIdempotency(joinPoint, idempotent);

        String expectedGroup = TestService.class.getName() + "#handleString";
        verify(idempotentRepository).tryAcquire(eq("KEY-AUTO-1"), eq(expectedGroup), any(), any());
        verify(idempotentRepository).markCompleted(eq("KEY-AUTO-1"), eq(expectedGroup), any(), org.mockito.ArgumentMatchers.nullable(Instant.class));
    }

    @Test
    @DisplayName("Senior Refinements v8: Checked exception during business method releases lock and is rethrown")
    void shouldReleaseLockAndRethrowWhenCheckedExceptionThrown() throws Throwable {
        Method method = TestService.class.getMethod("handleString", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getArgs()).thenReturn(new Object[]{"FAIL-KEY"});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0");
        when(idempotent.consumerGroup()).thenReturn("orders-group");
        when(idempotent.ttl()).thenReturn("7d");
        when(idempotent.lockTimeout()).thenReturn("2m");

        when(idempotentRepository.tryAcquire(any(), any(), any(), any()))
                .thenReturn(AcquireResult.ACQUIRED);
        when(joinPoint.proceed()).thenThrow(new java.io.IOException("Disk read failed"));

        assertThatThrownBy(() -> aspect.enforceIdempotency(joinPoint, idempotent))
                .isInstanceOf(java.io.IOException.class)
                .hasMessage("Disk read failed");

        verify(idempotentRepository).markFailed(eq("FAIL-KEY"), eq("orders-group"), eq("Disk read failed"), org.mockito.ArgumentMatchers.nullable(Instant.class));
        verify(idempotentRepository, never()).markCompleted(any(), any(), any());
        verify(idempotentRepository, never()).markCompleted(any(), any(), any(), any());
    }

    @Test
    @DisplayName("Senior Refinements v11 & Round 74: Checked exception from @Transactional without rollbackFor marks COMPLETED and does not set rollbackOnly")
    void shouldRetainLockAndSkipMarkFailedWhenCheckedExceptionThrownFromTransactionalWithoutRollbackFor() throws Throwable {
        org.springframework.transaction.support.TransactionSynchronizationManager.setActualTransactionActive(true);
        Method method = TestService.class.getMethod("transactionalMethodWithoutRollback", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getArgs()).thenReturn(new Object[]{"TX-FAIL-1"});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0");
        when(idempotent.consumerGroup()).thenReturn("orders-group");
        when(idempotent.ttl()).thenReturn("7d");
        when(idempotent.lockTimeout()).thenReturn("2m");

        when(idempotentRepository.tryAcquire(any(), any(), any(), any()))
                .thenReturn(AcquireResult.ACQUIRED);
        when(joinPoint.proceed()).thenThrow(new Exception("checked error in tx"));

        assertThatThrownBy(() -> aspect.enforceIdempotency(joinPoint, idempotent))
                .isInstanceOf(Exception.class)
                .hasMessage("checked error in tx");

        // Senior Refinements Round 74: Because Spring's TransactionInterceptor commits on Checked Exception
        // by default (unless rollbackFor is specified), calling setRollbackOnly() would cause commit() to throw
        // UnexpectedRollbackException, swallowing the original Checked Exception. Therefore, aspect must mark
        // COMPLETED and never call markFailed!
        verify(idempotentRepository).markCompleted(eq("TX-FAIL-1"), eq("orders-group"), eq(Duration.ofDays(7)), org.mockito.ArgumentMatchers.nullable(Instant.class));
        verify(idempotentRepository, never()).markFailed(any(), any(), any());
        verify(idempotentRepository, never()).markFailed(any(), any(), any(), any());
    }

    @Test
    @DisplayName("Senior Refinements v11: Checked exception from @Transactional WITH rollbackFor releases lock for retry")
    void shouldReleaseLockWhenCheckedExceptionThrownFromTransactionalWithRollbackForException() throws Throwable {
        Method method = TestService.class.getMethod("transactionalMethodWithRollback", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getArgs()).thenReturn(new Object[]{"TX-FAIL-2"});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0");
        when(idempotent.consumerGroup()).thenReturn("orders-group");
        when(idempotent.ttl()).thenReturn("7d");
        when(idempotent.lockTimeout()).thenReturn("2m");

        when(idempotentRepository.tryAcquire(any(), any(), any(), any()))
                .thenReturn(AcquireResult.ACQUIRED);
        when(joinPoint.proceed()).thenThrow(new Exception("checked error with rollback"));

        assertThatThrownBy(() -> aspect.enforceIdempotency(joinPoint, idempotent))
                .isInstanceOf(Exception.class)
                .hasMessage("checked error with rollback");

        // Rollback configured, so markFailed is invoked
        verify(idempotentRepository).markFailed(eq("TX-FAIL-2"), eq("orders-group"), eq("checked error with rollback"), org.mockito.ArgumentMatchers.nullable(Instant.class));
        verify(idempotentRepository, never()).markCompleted(any(), any(), any());
        verify(idempotentRepository, never()).markCompleted(any(), any(), any(), any());
    }

    @Test
    @DisplayName("Senior Refinements v27: Local lease timeout does not skip markFailed; atomic CAS in DB protects overtakers")
    void shouldAlwaysAttemptMarkFailedEvenWhenExecutionExceedsLockTimeout() throws Throwable {
        Method method = TestService.class.getMethod("handleString", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getArgs()).thenReturn(new Object[]{"EXPIRE-1"});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0");
        when(idempotent.consumerGroup()).thenReturn("orders-group");
        when(idempotent.ttl()).thenReturn("7d");
        // Very short lockTimeout so it expires during sleep
        when(idempotent.lockTimeout()).thenReturn("10ms");

        Instant lockedUntil = Instant.now().plusSeconds(60).truncatedTo(java.time.temporal.ChronoUnit.MICROS);
        when(idempotentRepository.tryAcquireWithLease(any(), any(), any(), any()))
                .thenReturn(new io.github.alexlitvinov.idembox.core.model.AcquireOutcome(AcquireResult.ACQUIRED, lockedUntil));
        when(joinPoint.proceed()).thenAnswer(inv -> {
            Thread.sleep(50); // Sleep longer than 10ms lock timeout
            throw new RuntimeException("Slow failure after timeout");
        });

        assertThatThrownBy(() -> aspect.enforceIdempotency(joinPoint, idempotent))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("Slow failure after timeout");

        // Atomic CAS with effectiveLockedUntil protects against deleting overtaker lock, so markFailed is always attempted
        verify(idempotentRepository).markFailed(eq("EXPIRE-1"), eq("orders-group"), eq("Slow failure after timeout"), eq(lockedUntil));
    }

    @Test
    @DisplayName("Senior Refinements v12: CompletableFuture async method triggers markCompleted when future completes successfully")
    void shouldCompleteAsyncMethodWhenFutureSucceeds() throws Throwable {
        Method method = TestService.class.getMethod("asyncCompute", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getArgs()).thenReturn(new Object[]{"ASYNC-1"});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0");
        when(idempotent.consumerGroup()).thenReturn("orders-group");
        when(idempotent.ttl()).thenReturn("7d");
        when(idempotent.lockTimeout()).thenReturn("2m");

        when(idempotentRepository.tryAcquire(any(), any(), any(), any()))
                .thenReturn(AcquireResult.ACQUIRED);

        CompletableFuture<String> future = new CompletableFuture<>();
        when(joinPoint.proceed()).thenReturn(future);

        Object result = aspect.enforceIdempotency(joinPoint, idempotent);
        assertThat(result).isInstanceOf(CompletableFuture.class);

        // Before async completion, markCompleted MUST NOT be invoked!
        verify(idempotentRepository, never()).markCompleted(any(), any(), any());
        verify(idempotentRepository, never()).markCompleted(any(), any(), any(), any());

        // Now complete future asynchronously
        future.complete("done");

        verify(idempotentRepository).markCompleted(eq("ASYNC-1"), eq("orders-group"), any(), org.mockito.ArgumentMatchers.nullable(Instant.class));
        verify(idempotentRepository, never()).markFailed(any(), any(), any());
        verify(idempotentRepository, never()).markFailed(any(), any(), any(), any());
    }

    @Test
    @DisplayName("Senior Refinements v12: CompletableFuture async method triggers markFailed when future completes exceptionally")
    void shouldFailAsyncMethodWhenFutureCompletesExceptionally() throws Throwable {
        Method method = TestService.class.getMethod("asyncCompute", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getArgs()).thenReturn(new Object[]{"ASYNC-FAIL-1"});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0");
        when(idempotent.consumerGroup()).thenReturn("orders-group");
        when(idempotent.ttl()).thenReturn("7d");
        when(idempotent.lockTimeout()).thenReturn("2m");

        when(idempotentRepository.tryAcquire(any(), any(), any(), any()))
                .thenReturn(AcquireResult.ACQUIRED);

        CompletableFuture<String> future = new CompletableFuture<>();
        when(joinPoint.proceed()).thenReturn(future);

        Object result = aspect.enforceIdempotency(joinPoint, idempotent);
        assertThat(result).isInstanceOf(CompletableFuture.class);

        // Complete exceptionally
        future.completeExceptionally(new RuntimeException("async backend failure"));

        verify(idempotentRepository).markFailed(eq("ASYNC-FAIL-1"), eq("orders-group"), eq("async backend failure"), org.mockito.ArgumentMatchers.nullable(Instant.class));
        verify(idempotentRepository, never()).markCompleted(any(), any(), any());
        verify(idempotentRepository, never()).markCompleted(any(), any(), any(), any());
    }

    @Test
    @DisplayName("Senior Refinements v20: CompletableFuture async method with @Transactional marks COMPLETED on async failure because tx committed synchronously")
    void shouldMarkCompletedWhenAsyncTransactionalMethodFailsBecauseTxCommittedSynchronously() throws Throwable {
        org.springframework.transaction.support.TransactionSynchronizationManager.setActualTransactionActive(true);
        Method method = TestService.class.getMethod("asyncTransactionalMethod", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getArgs()).thenReturn(new Object[]{"ASYNC-TX-FAIL-1"});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0");
        when(idempotent.consumerGroup()).thenReturn("orders-group");
        when(idempotent.ttl()).thenReturn("7d");
        when(idempotent.lockTimeout()).thenReturn("2m");

        when(idempotentRepository.tryAcquire(any(), any(), any(), any()))
                .thenReturn(AcquireResult.ACQUIRED);

        CompletableFuture<String> future = new CompletableFuture<>();
        when(joinPoint.proceed()).thenReturn(future);

        Object result = aspect.enforceIdempotency(joinPoint, idempotent);
        assertThat(result).isInstanceOf(CompletableFuture.class);

        // Fail asynchronously with RuntimeException
        future.completeExceptionally(new RuntimeException("Async stage failure in downstream processing"));

        // Since the @Transactional method already committed synchronously when proceed() returned the future,
        // it must mark COMPLETED in idempotency store to prevent duplicate re-execution on retry!
        verify(idempotentRepository).markCompleted(eq("ASYNC-TX-FAIL-1"), eq("orders-group"), any(), org.mockito.ArgumentMatchers.nullable(Instant.class));
        verify(idempotentRepository, never()).markFailed(any(), any(), any());
        verify(idempotentRepository, never()).markFailed(any(), any(), any(), any());
    }

    @Test
    @DisplayName("Senior Refinements v26: Execution exceeding lockTimeout still attempts markCompleted relying on atomic CAS")
    void shouldAlwaysAttemptMarkCompletedEvenWhenExecutionExceedsLockTimeout() throws Throwable {
        Method method = TestService.class.getMethod("handleString", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getArgs()).thenReturn(new Object[]{"EXPIRE-COMP-1"});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0");
        when(idempotent.consumerGroup()).thenReturn("orders-group");
        when(idempotent.ttl()).thenReturn("7d");
        // Very short lockTimeout so it expires during execution
        when(idempotent.lockTimeout()).thenReturn("10ms");

        when(idempotentRepository.tryAcquire(any(), any(), any(), any()))
                .thenReturn(AcquireResult.ACQUIRED);
        when(joinPoint.proceed()).thenAnswer(inv -> {
            Thread.sleep(50); // Sleep longer than 10ms lock timeout
            return "ok";
        });

        Object result = aspect.enforceIdempotency(joinPoint, idempotent);
        assertThat(result).isEqualTo("ok");

        // Atomic CAS in repository protects overtaker, so markCompleted is always called
        verify(idempotentRepository).markCompleted(eq("EXPIRE-COMP-1"), eq("orders-group"), any(), any());
    }

    @Test
    @DisplayName("Senior Refinements v26: Condition evaluating to false bypasses deduplication")
    void shouldBypassDeduplicationWhenConditionEvaluatesToFalse() throws Throwable {
        Method method = TestService.class.getMethod("handleString", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getArgs()).thenReturn(new Object[]{null});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.condition()).thenReturn("#p0 != null");
        when(joinPoint.proceed()).thenReturn("bypassed");

        Object result = aspect.enforceIdempotency(joinPoint, idempotent);
        assertThat(result).isEqualTo("bypassed");

        verify(idempotentRepository, never()).tryAcquire(any(), any(), any(), any());
        verify(idempotentRepository, never()).tryAcquireWithLease(any(), any(), any(), any());
        verify(idempotentRepository, never()).markCompleted(any(), any(), any(), any());
    }

    @Test
    @DisplayName("Senior Refinements v14: normalizeConsumerGroup preserves groups <= 255 chars")
    void normalizeConsumerGroupShouldPreserveShortNames() {
        String normal = "my-service-order-consumer-group";
        assertThat(IdempotentConsumerAspect.normalizeConsumerGroup(normal)).isEqualTo(normal);
    }

    @Test
    @DisplayName("Senior Refinements v14: normalizeConsumerGroup hashes and truncates groups > 255 chars")
    void normalizeConsumerGroupShouldTruncateAndHashLongNames() {
        String prefix = "a".repeat(190);
        String longGroup1 = prefix + "suffix_one_" + "x".repeat(100);
        String longGroup2 = prefix + "suffix_two_" + "y".repeat(100);

        String normalized1 = IdempotentConsumerAspect.normalizeConsumerGroup(longGroup1);
        String normalized2 = IdempotentConsumerAspect.normalizeConsumerGroup(longGroup2);

        assertThat(normalized1.length()).isLessThanOrEqualTo(255);
        assertThat(normalized2.length()).isLessThanOrEqualTo(255);
        assertThat(normalized1).startsWith(prefix + "_");
        assertThat(normalized2).startsWith(prefix + "_");
        // Different suffixes must produce different hashes even with identical prefix
        assertThat(normalized1).isNotEqualTo(normalized2);
    }

    @Test
    @DisplayName("Senior Refinements v14: Aspect normalizes overlong consumerGroup before tryAcquire")
    void aspectShouldPassNormalizedConsumerGroupToRepository() throws Throwable {
        Method method = TestService.class.getMethod("handleString", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getArgs()).thenReturn(new Object[]{"LONG-GRP-1"});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        String longGroup = "very_long_group_name_".repeat(20); // 420 chars
        when(idempotent.key()).thenReturn("#p0");
        when(idempotent.consumerGroup()).thenReturn(longGroup);
        when(idempotent.ttl()).thenReturn("7d");
        when(idempotent.lockTimeout()).thenReturn("1m");

        when(idempotentRepository.tryAcquire(any(), any(), any(), any()))
                .thenReturn(AcquireResult.ACQUIRED);
        when(joinPoint.proceed()).thenReturn("ok");

        aspect.enforceIdempotency(joinPoint, idempotent);

        org.mockito.ArgumentCaptor<String> groupCaptor = org.mockito.ArgumentCaptor.forClass(String.class);
        verify(idempotentRepository).tryAcquire(eq("LONG-GRP-1"), groupCaptor.capture(), any(), any());
        assertThat(groupCaptor.getValue().length()).isLessThanOrEqualTo(255);
        assertThat(groupCaptor.getValue()).isEqualTo(IdempotentConsumerAspect.normalizeConsumerGroup(longGroup));
    }

    @Test
    @DisplayName("Senior Refinements v15: Swallowed duplicate acknowledges manual Kafka offset")
    void swallowedDuplicateShouldAcknowledgeManualKafkaOffset() throws Throwable {
        Method method = TestService.class.getMethod("processWithAcknowledgment", String.class, org.springframework.kafka.support.Acknowledgment.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);

        org.springframework.kafka.support.Acknowledgment ack = org.mockito.Mockito.mock(org.springframework.kafka.support.Acknowledgment.class);
        when(joinPoint.getArgs()).thenReturn(new Object[]{"DUP-ACK-1", ack});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0");
        when(idempotent.consumerGroup()).thenReturn("orders-group");
        when(idempotent.swallowDuplicate()).thenReturn(true);

        when(idempotentRepository.tryAcquire(any(), any(), any(), any()))
                .thenReturn(AcquireResult.DUPLICATE_COMPLETED);

        Object result = aspect.enforceIdempotency(joinPoint, idempotent);

        assertThat(result).isNull();
        verify(ack).acknowledge();
        verify(joinPoint, never()).proceed();
    }

    @Test
    @DisplayName("Senior Refinements v17 & Round 71: Substring in noRollbackForClassName matches per Spring RollbackRuleAttribute contract and marks COMPLETED")
    void substringInNoRollbackForClassNameShouldNotFalselyMatch() throws Throwable {
        org.springframework.transaction.support.TransactionSynchronizationManager.setActualTransactionActive(true);
        Method method = TestService.class.getMethod("transactionalMethodWithNoRollbackForValidationName", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getArgs()).thenReturn(new Object[]{"TX-FAIL-3"});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0");
        when(idempotent.consumerGroup()).thenReturn("orders-group");
        when(idempotent.ttl()).thenReturn("7d");
        when(idempotent.lockTimeout()).thenReturn("2m");

        when(idempotentRepository.tryAcquire(any(), any(), any(), any()))
                .thenReturn(AcquireResult.ACQUIRED);
        when(joinPoint.proceed()).thenThrow(new DatabaseValidationFailureException("DB validation fail"));

        // Spring's RollbackRuleAttribute uses exceptionType.getName().contains(pattern), so "Validation"
        // matches DatabaseValidationFailureException and commits the transaction -> aspect must mark COMPLETED
        assertThatThrownBy(() -> aspect.enforceIdempotency(joinPoint, idempotent))
                .isInstanceOf(DatabaseValidationFailureException.class);

        verify(idempotentRepository).markCompleted(eq("TX-FAIL-3"), eq("orders-group"), eq(Duration.ofDays(7)), org.mockito.ArgumentMatchers.nullable(Instant.class));
        verify(idempotentRepository, never()).markFailed(any(), any(), any(), any());
    }

    @Test
    @DisplayName("Senior Refinements v18: Swallowed duplicate acknowledges offset when Message<?> with KafkaHeaders.ACKNOWLEDGMENT is passed")
    void shouldAcknowledgeOffsetFromMessageHeaderOnSwallowedDuplicate() throws Throwable {
        Method method = TestService.class.getMethod("handleString", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);

        org.springframework.kafka.support.Acknowledgment ack = org.mockito.Mockito.mock(org.springframework.kafka.support.Acknowledgment.class);
        org.springframework.messaging.Message<String> springMessage = org.springframework.messaging.support.MessageBuilder
                .withPayload("test-payload")
                .setHeader(org.springframework.kafka.support.KafkaHeaders.ACKNOWLEDGMENT, ack)
                .build();

        when(joinPoint.getArgs()).thenReturn(new Object[]{"DUP-ACK-MSG", springMessage});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0");
        when(idempotent.consumerGroup()).thenReturn("orders-group");
        when(idempotent.swallowDuplicate()).thenReturn(true);

        when(idempotentRepository.tryAcquire(any(), any(), any(), any()))
                .thenReturn(AcquireResult.DUPLICATE_COMPLETED);

        Object result = aspect.enforceIdempotency(joinPoint, idempotent);

        assertThat(result).isNull();
        verify(ack).acknowledge();
        verify(joinPoint, never()).proceed();
    }

    @Test
    @DisplayName("Senior Refinements v19: Checked exception from non-transactional method invokes markFailed to release lock")
    void shouldReleaseLockWhenCheckedExceptionThrownFromNonTransactionalMethod() throws Throwable {
        Method method = TestService.class.getMethod("nonTransactionalMethodThrowingChecked", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getArgs()).thenReturn(new Object[]{"NON-TX-CHECKED"});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0");
        when(idempotent.consumerGroup()).thenReturn("orders-group");
        when(idempotent.ttl()).thenReturn("7d");
        when(idempotent.lockTimeout()).thenReturn("2m");

        when(idempotentRepository.tryAcquire(any(), any(), any(), any()))
                .thenReturn(AcquireResult.ACQUIRED);
        when(joinPoint.proceed()).thenThrow(new Exception("checked error outside tx"));

        assertThatThrownBy(() -> aspect.enforceIdempotency(joinPoint, idempotent))
                .isInstanceOf(Exception.class)
                .hasMessage("checked error outside tx");

        // Non-transactional method must release lock on checked exception
        verify(idempotentRepository).markFailed(eq("NON-TX-CHECKED"), eq("orders-group"), eq("checked error outside tx"), org.mockito.ArgumentMatchers.nullable(Instant.class));
        verify(idempotentRepository, never()).markCompleted(any(), any(), any());
    }

    @Test
    @DisplayName("Senior Refinements v19: In-flight wait resolving to DUPLICATE_COMPLETED acknowledges manual Kafka offset")
    void shouldAcknowledgeManualOffsetWhenInFlightPollResolvesToDuplicateCompleted() throws Throwable {
        properties.getIdempotency().setInFlightWaitTimeout(Duration.ofMillis(300));
        Method method = TestService.class.getMethod("processWithAcknowledgment", String.class, org.springframework.kafka.support.Acknowledgment.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);

        org.springframework.kafka.support.Acknowledgment ack = org.mockito.Mockito.mock(org.springframework.kafka.support.Acknowledgment.class);
        when(joinPoint.getArgs()).thenReturn(new Object[]{"POLL-ACK-1", ack});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0");
        when(idempotent.consumerGroup()).thenReturn("orders-group");
        when(idempotent.ttl()).thenReturn("7d");
        when(idempotent.lockTimeout()).thenReturn("2m");
        when(idempotent.swallowDuplicate()).thenReturn(true);

        when(idempotentRepository.tryAcquire(any(), any(), any(), any()))
                .thenReturn(AcquireResult.IN_FLIGHT_LOCKED);

        // Polling transitions to COMPLETED
        when(idempotentRepository.getProcessingState("POLL-ACK-1", "orders-group"))
                .thenReturn(new ProcessingState(ProcessingStatus.PROCESSING, Instant.now().plusSeconds(60), Instant.now().plusSeconds(3600)))
                .thenReturn(new ProcessingState(ProcessingStatus.COMPLETED, null, Instant.now().plusSeconds(3600)));

        Object result = aspect.enforceIdempotency(joinPoint, idempotent);

        assertThat(result).isNull();
        verify(ack).acknowledge();
        verify(joinPoint, never()).proceed();
    }

    @Test
    @DisplayName("Senior Refinements v19: In-flight wait timeout throws ConcurrentProcessingException")
    void shouldThrowConcurrentProcessingExceptionWhenInFlightWaitTimesOut() throws NoSuchMethodException {
        properties.getIdempotency().setInFlightWaitTimeout(Duration.ofMillis(80));
        Method method = TestService.class.getMethod("handleString", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getArgs()).thenReturn(new Object[]{"TIMEOUT-1"});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0");
        when(idempotent.consumerGroup()).thenReturn("orders-group");
        when(idempotent.ttl()).thenReturn("7d");
        when(idempotent.lockTimeout()).thenReturn("2m");

        when(idempotentRepository.tryAcquire(any(), any(), any(), any()))
                .thenReturn(AcquireResult.IN_FLIGHT_LOCKED);

        // Remains locked across polls
        when(idempotentRepository.getProcessingState("TIMEOUT-1", "orders-group"))
                .thenReturn(new ProcessingState(ProcessingStatus.PROCESSING, Instant.now().plusSeconds(60), Instant.now().plusSeconds(3600), true));

        assertThatThrownBy(() -> aspect.enforceIdempotency(joinPoint, idempotent))
                .isInstanceOf(ConcurrentProcessingException.class)
                .hasMessageContaining("currently being processed by another worker");
    }

    @Test
    @DisplayName("Senior Refinements v19: Thread interruption during in-flight wait restores interrupt flag and throws ConcurrentProcessingException")
    void shouldRestoreInterruptFlagAndThrowWhenThreadInterruptedDuringInFlightWait() throws NoSuchMethodException {
        properties.getIdempotency().setInFlightWaitTimeout(Duration.ofSeconds(2));
        Method method = TestService.class.getMethod("handleString", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getArgs()).thenReturn(new Object[]{"INTR-1"});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0");
        when(idempotent.consumerGroup()).thenReturn("orders-group");
        when(idempotent.ttl()).thenReturn("7d");
        when(idempotent.lockTimeout()).thenReturn("2m");

        when(idempotentRepository.tryAcquire(any(), any(), any(), any()))
                .thenReturn(AcquireResult.IN_FLIGHT_LOCKED);

        // Interrupt current thread before wait sleep executes
        Thread.currentThread().interrupt();

        try {
            assertThatThrownBy(() -> aspect.enforceIdempotency(joinPoint, idempotent))
                    .isInstanceOf(ConcurrentProcessingException.class);
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            // Clear interrupt flag so other tests are unaffected
            boolean clearedInterrupt = Thread.interrupted();
            assertThat(clearedInterrupt).isTrue();
        }
    }

    @Test
    @DisplayName("Senior Refinements v19: waitForInFlightCompletion returning AcquireOutcome preserves lockedUntil without extra getProcessingState")
    void shouldPreserveLockedUntilFromInFlightCompletionWithoutExtraGetProcessingState() throws Throwable {
        Method method = TestService.class.getMethod("handleString", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getArgs()).thenReturn(new Object[]{"LEASE-1"});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0");
        when(idempotent.consumerGroup()).thenReturn("orders-group");
        when(idempotent.ttl()).thenReturn("7d");
        when(idempotent.lockTimeout()).thenReturn("2m");

        Instant leaseDeadline = Instant.now().plusSeconds(120).truncatedTo(java.time.temporal.ChronoUnit.MICROS);

        when(idempotentRepository.tryAcquireWithLease(eq("LEASE-1"), eq("orders-group"), any(), any()))
                .thenReturn(new io.github.alexlitvinov.idembox.core.model.AcquireOutcome(AcquireResult.IN_FLIGHT_LOCKED, null))
                .thenReturn(new io.github.alexlitvinov.idembox.core.model.AcquireOutcome(AcquireResult.ACQUIRED, leaseDeadline));

        ProcessingState state = new ProcessingState(ProcessingStatus.PROCESSING, null, null, false);
        when(idempotentRepository.getProcessingState(eq("LEASE-1"), eq("orders-group")))
                .thenReturn(state);

        when(joinPoint.proceed()).thenReturn("success");

        Object result = aspect.enforceIdempotency(joinPoint, idempotent);

        assertThat(result).isEqualTo("success");
        verify(idempotentRepository, times(1)).getProcessingState(eq("LEASE-1"), eq("orders-group"));
        verify(idempotentRepository).markCompleted(eq("LEASE-1"), eq("orders-group"), eq(Duration.ofDays(7)), eq(leaseDeadline));
    }

    @Test
    @DisplayName("Senior Refinements v22: enforceIdempotency throws IllegalStateException when SpEL key evaluates to null")
    void shouldThrowIllegalStateExceptionWhenKeyExpressionEvaluatesToNull() throws NoSuchMethodException {
        Method method = TestService.class.getMethod("handleString", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getArgs()).thenReturn(new Object[]{null});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0");

        assertThatThrownBy(() -> aspect.enforceIdempotency(joinPoint, idempotent))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("returned null");

        verify(idempotentRepository, never()).tryAcquire(any(), any(), any(), any());
        verify(idempotentRepository, never()).tryAcquireWithLease(any(), any(), any(), any());
    }

    @Test
    @DisplayName("Senior Refinements v22: enforceIdempotency returns default primitive when duplicate is swallowed")
    void shouldReturnDefaultPrimitiveWhenDuplicateIsSwallowed() throws Throwable {
        Method intMethod = TestService.class.getMethod("computePrimitiveInt", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(intMethod);
        when(joinPoint.getArgs()).thenReturn(new Object[]{"INT-DUP-1"});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0");
        when(idempotent.consumerGroup()).thenReturn("orders-group");
        when(idempotent.ttl()).thenReturn("7d");
        when(idempotent.lockTimeout()).thenReturn("2m");
        when(idempotent.swallowDuplicate()).thenReturn(true);

        when(idempotentRepository.tryAcquireWithLease(any(), any(), any(), any()))
                .thenReturn(new io.github.alexlitvinov.idembox.core.model.AcquireOutcome(AcquireResult.DUPLICATE_COMPLETED, null));

        Object intResult = aspect.enforceIdempotency(joinPoint, idempotent);
        assertThat(intResult).isEqualTo(0);

        // Test boolean primitive
        Method boolMethod = TestService.class.getMethod("computePrimitiveBoolean", String.class);
        when(methodSignature.getMethod()).thenReturn(boolMethod);

        Object boolResult = aspect.enforceIdempotency(joinPoint, idempotent);
        assertThat(boolResult).isEqualTo(false);
    }

    @Test
    @DisplayName("Senior Refinements v24: normalizeConsumerGroup handles null, preserves <= 255 chars, and truncates/hashes > 255 chars")
    void shouldNormalizeConsumerGroup() {
        assertThat(IdempotentConsumerAspect.normalizeConsumerGroup(null)).isEqualTo("");
        assertThat(IdempotentConsumerAspect.normalizeConsumerGroup("orders-service-group")).isEqualTo("orders-service-group");

        String exactly255 = "a".repeat(255);
        assertThat(IdempotentConsumerAspect.normalizeConsumerGroup(exactly255)).isEqualTo(exactly255);

        String over255 = "b".repeat(300);
        String normalized = IdempotentConsumerAspect.normalizeConsumerGroup(over255);
        assertThat(normalized).isNotNull();
        assertThat(normalized.length()).isLessThanOrEqualTo(255);
        assertThat(normalized).startsWith("b".repeat(190) + "_");
        assertThat(normalized).hasSize(190 + 1 + 32);
    }

    @Test
    @DisplayName("Senior Refinements v36: Should derive consumerGroup from method descriptor when consumerGroup is empty or blank")
    void shouldDeriveConsumerGroupWhenDefaultOrBlank() throws Throwable {
        Method method = TestService.class.getMethod("handleString", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getArgs()).thenReturn(new Object[]{"ORD-DEFAULT-GRP"});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0");
        when(idempotent.consumerGroup()).thenReturn("");
        when(idempotent.ttl()).thenReturn("7d");
        when(idempotent.lockTimeout()).thenReturn("2m");

        when(idempotentRepository.tryAcquireWithLease(any(), any(), any(), any()))
                .thenReturn(new io.github.alexlitvinov.idembox.core.model.AcquireOutcome(AcquireResult.ACQUIRED, Instant.now().plusSeconds(60)));
        when(joinPoint.proceed()).thenReturn("ok");

        aspect.enforceIdempotency(joinPoint, idempotent);

        String expectedGroup = TestService.class.getName() + "#handleString";
        verify(idempotentRepository).tryAcquireWithLease(eq("ORD-DEFAULT-GRP"), eq(expectedGroup), any(), any());
    }

    @Test
    @DisplayName("Senior Refinements v36: Should preserve literal 'default' as explicit consumerGroup name")
    void shouldPreserveLiteralDefaultAsConsumerGroupName() throws Throwable {
        Method method = TestService.class.getMethod("handleString", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getArgs()).thenReturn(new Object[]{"ORD-LITERAL-DEFAULT"});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0");
        when(idempotent.consumerGroup()).thenReturn("default");
        when(idempotent.ttl()).thenReturn("7d");
        when(idempotent.lockTimeout()).thenReturn("2m");

        when(idempotentRepository.tryAcquireWithLease(any(), any(), any(), any()))
                .thenReturn(new io.github.alexlitvinov.idembox.core.model.AcquireOutcome(AcquireResult.ACQUIRED, Instant.now().plusSeconds(60)));
        when(joinPoint.proceed()).thenReturn("ok");

        aspect.enforceIdempotency(joinPoint, idempotent);

        verify(idempotentRepository).tryAcquireWithLease(eq("ORD-LITERAL-DEFAULT"), eq("default"), any(), any());
    }

    @Test
    @DisplayName("Senior Refinements v36: Should resolve consumerGroup from KafkaUtils.getConsumerGroupId() when consumerGroup is empty")
    void shouldResolveConsumerGroupFromKafkaUtilsWhenEmpty() throws Throwable {
        Method method = TestService.class.getMethod("handleString", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getArgs()).thenReturn(new Object[]{"ORD-KAFKA-UTILS"});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0");
        when(idempotent.consumerGroup()).thenReturn("");
        when(idempotent.ttl()).thenReturn("7d");
        when(idempotent.lockTimeout()).thenReturn("2m");

        when(idempotentRepository.tryAcquireWithLease(any(), any(), any(), any()))
                .thenReturn(new io.github.alexlitvinov.idembox.core.model.AcquireOutcome(AcquireResult.ACQUIRED, Instant.now().plusSeconds(60)));
        when(joinPoint.proceed()).thenReturn("ok");

        try {
            org.springframework.kafka.support.KafkaUtils.setConsumerGroupId("active-spring-kafka-group");
            aspect.enforceIdempotency(joinPoint, idempotent);

            verify(idempotentRepository).tryAcquireWithLease(eq("ORD-KAFKA-UTILS"), eq("active-spring-kafka-group"), any(), any());
        } finally {
            org.springframework.kafka.support.KafkaUtils.clearConsumerGroupId();
        }
    }

    @Test
    @DisplayName("Senior Refinements v36: Should resolve consumerGroup from Spring Message headers when consumerGroup is empty")
    void shouldResolveConsumerGroupFromMessageHeaderWhenEmpty() throws Throwable {
        Method method = TestService.class.getMethod("handleString", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);

        org.springframework.messaging.Message<String> springMsg = org.springframework.messaging.support.MessageBuilder
                .withPayload("ORD-HEADER-GRP")
                .setHeader(KafkaHeaders.GROUP_ID, "header-derived-group")
                .build();
        when(joinPoint.getArgs()).thenReturn(new Object[]{springMsg});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0.payload");
        when(idempotent.consumerGroup()).thenReturn("   "); // blank
        when(idempotent.ttl()).thenReturn("7d");
        when(idempotent.lockTimeout()).thenReturn("2m");

        when(idempotentRepository.tryAcquireWithLease(any(), any(), any(), any()))
                .thenReturn(new io.github.alexlitvinov.idembox.core.model.AcquireOutcome(AcquireResult.ACQUIRED, Instant.now().plusSeconds(60)));
        when(joinPoint.proceed()).thenReturn("ok");

        aspect.enforceIdempotency(joinPoint, idempotent);

        verify(idempotentRepository).tryAcquireWithLease(eq("ORD-HEADER-GRP"), eq("header-derived-group"), any(), any());
    }

    interface RootMarkerInterface {}
    interface ChildMarkerInterface extends RootMarkerInterface {}
    static class CustomCheckedException extends Exception implements ChildMarkerInterface {}

    @Test
    @DisplayName("Senior Refinements v23 & Round 71: TransactionalAnnotationUtils.getDepth checks superclasses, interfaces, substrings, and nested dot names")
    void shouldMatchClassNamesAndInterfaces() {
        // Direct class name, simple name, substring, and dot-normalized nested class name
        assertThat(TransactionalAnnotationUtils.getDepth(CustomCheckedException.class, "CustomCheckedException")).isEqualTo(0);
        assertThat(TransactionalAnnotationUtils.getDepth(CustomCheckedException.class, CustomCheckedException.class.getName())).isEqualTo(0);
        assertThat(TransactionalAnnotationUtils.getDepth(CustomCheckedException.class, "CustomChecked")).isEqualTo(0);
        assertThat(TransactionalAnnotationUtils.getDepth(CustomCheckedException.class, "IdempotentConsumerAspectTest.CustomCheckedException")).isEqualTo(0);

        // Superclass
        assertThat(TransactionalAnnotationUtils.getDepth(CustomCheckedException.class, "java.lang.Exception")).isEqualTo(1);

        // Direct interface
        assertThat(TransactionalAnnotationUtils.getDepth(CustomCheckedException.class, "ChildMarkerInterface")).isEqualTo(1);

        // Inherited interface
        assertThat(TransactionalAnnotationUtils.getDepth(CustomCheckedException.class, "RootMarkerInterface")).isEqualTo(2);

        // Unrelated interface/class
        assertThat(TransactionalAnnotationUtils.getDepth(CustomCheckedException.class, "java.util.List")).isEqualTo(-1);
        assertThat(TransactionalAnnotationUtils.getDepth(null, "Exception")).isEqualTo(-1);
        assertThat(TransactionalAnnotationUtils.getDepth(CustomCheckedException.class, "")).isEqualTo(-1);
    }

    @Test
    @DisplayName("Senior Refinements v24: enforceIdempotency throws IllegalArgumentException if first argument is Collection/Iterable")
    void shouldThrowIllegalArgumentExceptionOnBatchListenerMethod() throws Throwable {
        Method batchMethod = TestService.class.getMethod("handleBatch", java.util.List.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(batchMethod);
        when(joinPoint.getArgs()).thenReturn(new Object[]{java.util.List.of("item1", "item2")});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        assertThatThrownBy(() -> aspect.enforceIdempotency(joinPoint, idempotent))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("@Idempotent cannot be applied to batch listener method");
    }

    @Test
    @DisplayName("Senior Refinements v25: enforceIdempotency throws IllegalArgumentException if any argument is Collection/Iterable (batch listener with Ack first)")
    void shouldThrowIllegalArgumentExceptionOnBatchListenerMethodWithAckFirst() throws Throwable {
        Method batchMethod = TestService.class.getMethod("handleBatchWithAcknowledgment", org.springframework.kafka.support.Acknowledgment.class, java.util.List.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(batchMethod);
        org.springframework.kafka.support.Acknowledgment mockAck = org.mockito.Mockito.mock(org.springframework.kafka.support.Acknowledgment.class);
        when(joinPoint.getArgs()).thenReturn(new Object[]{mockAck, java.util.List.of("item1", "item2")});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        assertThatThrownBy(() -> aspect.enforceIdempotency(joinPoint, idempotent))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("@Idempotent cannot be applied to batch listener method");
    }

    @Test
    @DisplayName("Senior Refinements v54: enforceIdempotency throws IllegalArgumentException if argument is ConsumerRecord[]")
    void shouldThrowIllegalArgumentExceptionOnBatchRecordArray() throws Throwable {
        Method batchMethod = TestService.class.getMethod("handleBatchRecords", org.apache.kafka.clients.consumer.ConsumerRecord[].class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(batchMethod);
        org.apache.kafka.clients.consumer.ConsumerRecord<?, ?>[] records = new org.apache.kafka.clients.consumer.ConsumerRecord<?, ?>[]{
                new org.apache.kafka.clients.consumer.ConsumerRecord<>("topic", 0, 0L, "k1", "v1")
        };
        when(joinPoint.getArgs()).thenReturn(new Object[]{records});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        assertThatThrownBy(() -> aspect.enforceIdempotency(joinPoint, idempotent))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("@Idempotent cannot be applied to batch listener method");
    }

    @Test
    @DisplayName("Senior Refinements v55: enforceIdempotency throws IllegalArgumentException if argument is custom Iterable")
    void shouldThrowIllegalArgumentExceptionOnIterableBatch() throws Throwable {
        Method batchMethod = TestService.class.getMethod("handleIterable", Iterable.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(batchMethod);
        Iterable<String> iterable = () -> java.util.List.of("item1", "item2").iterator();
        when(joinPoint.getArgs()).thenReturn(new Object[]{iterable});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        assertThatThrownBy(() -> aspect.enforceIdempotency(joinPoint, idempotent))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("@Idempotent cannot be applied to batch listener method");
    }

    @Test
    @DisplayName("Senior Refinements Round 63: enforceIdempotency allows non-Kafka service methods accepting List/Collection")
    void shouldAllowNonKafkaMethodAcceptingCollection() throws Throwable {
        Method businessMethod = TestService.class.getMethod("handleBusinessList", String.class, java.util.List.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(businessMethod);
        when(joinPoint.getArgs()).thenReturn(new Object[]{"order-123", java.util.List.of("item1", "item2")});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0");
        when(idempotent.consumerGroup()).thenReturn("orders-group");
        when(idempotent.ttl()).thenReturn("7d");
        when(idempotent.lockTimeout()).thenReturn("2m");

        when(idempotentRepository.tryAcquire(org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any()))
                .thenReturn(AcquireResult.ACQUIRED);

        Object result = aspect.enforceIdempotency(joinPoint, idempotent);
        org.assertj.core.api.Assertions.assertThat(result).isNull();
        verify(idempotentRepository).tryAcquire(org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any());
    }

    @Test
    @DisplayName("Senior Refinements v26: Execution exceeding lockTimeout still attempts markCompleted relying on atomic CAS")
    void shouldAlwaysAttemptMarkCompletedEvenWhenExecutionExceedsLockTimeoutInAsyncOrSlowPath() throws Throwable {
        Method method = TestService.class.getMethod("handleString", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getArgs()).thenReturn(new Object[]{"ORD-SLOW-999"});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0");
        when(idempotent.consumerGroup()).thenReturn("orders-group");
        when(idempotent.ttl()).thenReturn("7d");
        when(idempotent.lockTimeout()).thenReturn("10ms"); // Very short timeout: 10ms

        when(idempotentRepository.tryAcquireWithLease(any(), any(), any(), any()))
                .thenReturn(new io.github.alexlitvinov.idembox.core.model.AcquireOutcome(AcquireResult.ACQUIRED, Instant.now().plusMillis(10)));
        when(joinPoint.proceed()).thenAnswer(inv -> {
            Thread.sleep(30); // Simulate execution taking 30ms > 10ms lockTimeout
            return "slow-ok";
        });

        Object result = aspect.enforceIdempotency(joinPoint, idempotent);

        assertThat(result).isEqualTo("slow-ok");
        // Atomic CAS in repository protects overtaker, so markCompleted is always called
        verify(idempotentRepository).markCompleted(eq("ORD-SLOW-999"), eq("orders-group"), any(), any());
    }

    @Test
    @DisplayName("Senior Refinements v26: Evaluating SpEL property on null event argument (Kafka Tombstone) throws EvaluationException")
    void shouldThrowEvaluationExceptionOnNullEventPayloadArgument() throws NoSuchMethodException {
        Method method = TestService.class.getMethod("handleString", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getArgs()).thenReturn(new Object[]{null}); // Tombstone value = null
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0.substring(0, 1)"); // Property access on null

        assertThatThrownBy(() -> aspect.enforceIdempotency(joinPoint, idempotent))
                .isInstanceOf(org.springframework.expression.EvaluationException.class);

        // Aspect failed before reaching idempotency store
        verify(idempotentRepository, never()).tryAcquireWithLease(any(), any(), any(), any());
        verify(idempotentRepository, never()).tryAcquire(any(), any(), any(), any());
    }

    @Test
    @DisplayName("Senior Refinements v27: SpEL property evaluation on null event falls back to ConsumerRecord.key when present")
    void shouldFallbackToConsumerRecordKeyWhenExpressionFailsOnTombstone() throws Throwable {
        Method method = TestService.class.getMethod("handleWithRecord", String.class, org.apache.kafka.clients.consumer.ConsumerRecord.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);

        org.apache.kafka.clients.consumer.ConsumerRecord<String, String> record =
                new org.apache.kafka.clients.consumer.ConsumerRecord<>("my-topic", 0, 100L, "TOMBSTONE-KEY-456", null);
        when(joinPoint.getArgs()).thenReturn(new Object[]{null, record}); // payload is null, record is present
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0.substring(0, 1)"); // Throws EvaluationException on null #p0
        when(idempotent.consumerGroup()).thenReturn("orders-group");
        when(idempotent.ttl()).thenReturn("7d");
        when(idempotent.lockTimeout()).thenReturn("2m");

        when(idempotentRepository.tryAcquireWithLease(eq("TOMBSTONE-KEY-456"), eq("orders-group"), any(), any()))
                .thenReturn(new io.github.alexlitvinov.idembox.core.model.AcquireOutcome(AcquireResult.ACQUIRED, Instant.now().plusSeconds(120)));
        when(joinPoint.proceed()).thenReturn("tombstone-handled");

        Object result = aspect.enforceIdempotency(joinPoint, idempotent);

        assertThat(result).isEqualTo("tombstone-handled");
        verify(idempotentRepository).tryAcquireWithLease(eq("TOMBSTONE-KEY-456"), eq("orders-group"), any(), any());
        verify(idempotentRepository).markCompleted(eq("TOMBSTONE-KEY-456"), eq("orders-group"), any(), any());
    }

    @Test
    @DisplayName("Senior Refinements v29: Reactive return type Mono throws IllegalStateException and releases lock")
    void reactiveReturnTypeShouldThrowIllegalStateExceptionAndMarkFailed() throws Throwable {
        Method method = TestService.class.getMethod("reactiveMonoMethod", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getArgs()).thenReturn(new Object[]{"REACTIVE-1"});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0");
        when(idempotent.consumerGroup()).thenReturn("orders-group");
        when(idempotent.ttl()).thenReturn("7d");
        when(idempotent.lockTimeout()).thenReturn("2m");

        when(idempotentRepository.tryAcquire(any(), any(), any(), any()))
                .thenReturn(AcquireResult.ACQUIRED);

        when(joinPoint.proceed()).thenReturn(reactor.core.publisher.Mono.just("result"));

        assertThatThrownBy(() -> aspect.enforceIdempotency(joinPoint, idempotent))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Reactive streams (Mono/Flux/Publisher) are not supported with @Idempotent because execution is deferred");

        verify(idempotentRepository).markTerminalFailed(eq("REACTIVE-1"), eq("orders-group"), org.mockito.ArgumentMatchers.contains("Reactive streams (Mono/Flux/Publisher) are not supported"), any(), any());
        verify(idempotentRepository, never()).markCompleted(any(), any(), any());
        verify(idempotentRepository, never()).markCompleted(any(), any(), any(), any());
    }

    static class D0 extends Exception {}
    static class D1 extends D0 {}
    static class D2 extends D1 {}
    static class D3 extends D2 {}
    static class D4 extends D3 {}
    static class D5 extends D4 {}
    static class D6 extends D5 {}
    static class D7 extends D6 {}
    static class D8 extends D7 {}
    static class D9 extends D8 {}
    static class D10 extends D9 {}
    static class D11 extends D10 {}
    static class D12 extends D11 {}
    static class D13 extends D12 {}
    static class D14 extends D13 {}
    static class D15 extends D14 {}
    static class D16 extends D15 {}
    static class D17 extends D16 {}
    static class D18 extends D17 {}
    static class D19 extends D18 {}
    static class D20 extends D19 {}
    static class D21 extends D20 {}
    static class D22 extends D21 {}
    static class D23 extends D22 {}
    static class D24 extends D23 {}
    static class D25 extends D24 {}
    static class D26 extends D25 {}
    static class D27 extends D26 {}
    static class D28 extends D27 {}
    static class D29 extends D28 {}
    static class D30 extends D29 {}
    static class D31 extends D30 {}
    static class D32 extends D31 {}
    static class D33 extends D32 {}
    static class D34 extends D33 {}
    static class D35 extends D34 {}

    @Test
    @DisplayName("Senior Refinements v31 & Round 71: TransactionalAnnotationUtils.getDepth respects maxDepth limit of 32")
    void shouldRespectMaxDepthLimitInMatchesClassName() {
        // D31 depth to D0 is 31 (within 32)
        assertThat(TransactionalAnnotationUtils.getDepth(D31.class, "D0")).isEqualTo(31);
        // D32 depth to D0 is 32 (exceeds maxDepth < 32 limit)
        assertThat(TransactionalAnnotationUtils.getDepth(D32.class, "D0")).isEqualTo(-1);
        // D35 depth to D0 is 35 (exceeds maxDepth < 32 limit)
        assertThat(TransactionalAnnotationUtils.getDepth(D35.class, "D0")).isEqualTo(-1);
    }

    @Test
    @DisplayName("Senior Refinements v31: in-flight lock retry records concurrent_retry metric, not failed")
    void shouldRecordConcurrentRetryMetricAndNotFailedOnInFlightLock() throws Throwable {
        IdemboxMetrics mockMetrics = mock(IdemboxMetrics.class);
        IdempotentConsumerAspect customAspect = new IdempotentConsumerAspect(idempotentRepository, spelKeyExtractor, properties, mockMetrics);

        properties.getIdempotency().setInFlightWaitTimeout(Duration.ofMillis(80));
        Method method = TestService.class.getMethod("handleString", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getArgs()).thenReturn(new Object[]{"CONCURRENT-RETRY-1"});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0");
        when(idempotent.consumerGroup()).thenReturn("orders-group");
        when(idempotent.ttl()).thenReturn("7d");
        when(idempotent.lockTimeout()).thenReturn("2m");

        when(idempotentRepository.tryAcquire(any(), any(), any(), any()))
                .thenReturn(AcquireResult.IN_FLIGHT_LOCKED);
        when(idempotentRepository.getProcessingState("CONCURRENT-RETRY-1", "orders-group"))
                .thenReturn(new ProcessingState(ProcessingStatus.PROCESSING, Instant.now().plusSeconds(60), Instant.now().plusSeconds(3600), true));

        assertThatThrownBy(() -> customAspect.enforceIdempotency(joinPoint, idempotent))
                .isInstanceOf(ConcurrentProcessingException.class);

        verify(mockMetrics).recordIdempotentConcurrentRetry("orders-group");
        verify(mockMetrics, never()).recordIdempotentFailed(any());
    }

    @Test
    @DisplayName("Senior Refinements v37: should preserve original business exception when markFailed throws DataAccessException (e.g. Postgres 25P02)")
    void shouldPreserveOriginalBusinessExceptionWhenMarkFailedThrowsDataAccessException() throws Throwable {
        Method method = TestService.class.getMethod("handleString", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getArgs()).thenReturn(new Object[]{"TX-ABORTED-KEY"});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0");
        when(idempotent.consumerGroup()).thenReturn("orders-group");
        when(idempotent.ttl()).thenReturn("7d");
        when(idempotent.lockTimeout()).thenReturn("2m");

        when(idempotentRepository.tryAcquire(any(), any(), any(), any()))
                .thenReturn(AcquireResult.ACQUIRED);

        RuntimeException businessException = new RuntimeException("Business unique violation");
        when(joinPoint.proceed()).thenThrow(businessException);

        org.mockito.Mockito.doThrow(new org.springframework.dao.DataAccessException("25P02: current transaction is aborted, commands ignored") {})
                .when(idempotentRepository).markFailed(any(), any(), any(), any());

        assertThatThrownBy(() -> aspect.enforceIdempotency(joinPoint, idempotent))
                .isSameAs(businessException);

        verify(idempotentRepository).markFailed(eq("TX-ABORTED-KEY"), eq("orders-group"), eq("Business unique violation"), any());
    }

    @Test
    @DisplayName("Senior Refinements v40: recordIdempotentLeaseStolen when markCompleted returns false")
    void shouldRecordLeaseStolenMetricWhenMarkCompletedReturnsFalse() throws Throwable {
        IdemboxMetrics mockMetrics = mock(IdemboxMetrics.class);
        IdempotentConsumerAspect customAspect = new IdempotentConsumerAspect(idempotentRepository, spelKeyExtractor, properties, mockMetrics);

        Method method = TestService.class.getMethod("handleString", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getArgs()).thenReturn(new Object[]{"STOLEN-LEASE-KEY"});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0");
        when(idempotent.consumerGroup()).thenReturn("orders-group");
        when(idempotent.ttl()).thenReturn("7d");
        when(idempotent.lockTimeout()).thenReturn("2m");

        when(idempotentRepository.tryAcquireWithLease(any(), any(), any(), any()))
                .thenReturn(new io.github.alexlitvinov.idembox.core.model.AcquireOutcome(AcquireResult.ACQUIRED, Instant.now().plusSeconds(120)));
        when(joinPoint.proceed()).thenReturn("done");
        when(idempotentRepository.markCompleted(eq("STOLEN-LEASE-KEY"), eq("orders-group"), any(), any()))
                .thenReturn(false);

        assertThatThrownBy(() -> customAspect.enforceIdempotency(joinPoint, idempotent))
                .isInstanceOf(IdempotencyException.class)
                .hasMessageContaining("Lease was stolen or lock expired before completion");

        verify(mockMetrics).recordIdempotentLeaseStolen("orders-group");
    }

    @Test
    @DisplayName("Senior Refinements v40: DUPLICATE_FAILED allows retry when allowFailedRetry is true")
    void shouldAllowRetryOnDuplicateFailedWhenAllowFailedRetryIsTrue() throws Throwable {
        Method method = TestService.class.getMethod("handleString", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getArgs()).thenReturn(new Object[]{"RETRY-FAILED-KEY"});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0");
        when(idempotent.consumerGroup()).thenReturn("orders-group");
        when(idempotent.ttl()).thenReturn("7d");
        when(idempotent.lockTimeout()).thenReturn("2m");
        when(idempotent.allowFailedRetry()).thenReturn(true);

        when(idempotentRepository.tryAcquireWithLease("RETRY-FAILED-KEY", "orders-group", Duration.ofMinutes(2), Duration.ofDays(7)))
                .thenReturn(new io.github.alexlitvinov.idembox.core.model.AcquireOutcome(AcquireResult.DUPLICATE_FAILED, null))
                .thenReturn(new io.github.alexlitvinov.idembox.core.model.AcquireOutcome(AcquireResult.ACQUIRED, Instant.now().plusSeconds(120)));
        when(idempotentRepository.resetFailed("RETRY-FAILED-KEY", "orders-group")).thenReturn(true);
        when(joinPoint.proceed()).thenReturn("retried-ok");
        when(idempotentRepository.markCompleted(eq("RETRY-FAILED-KEY"), eq("orders-group"), any(), any())).thenReturn(true);

        Object res = aspect.enforceIdempotency(joinPoint, idempotent);
        assertThat(res).isEqualTo("retried-ok");

        verify(idempotentRepository).resetFailed("RETRY-FAILED-KEY", "orders-group");
    }

    @Test
    @DisplayName("Senior Refinements v40: DUPLICATE_FAILED allows retry when X-Idempotent-Replay header is present")
    void shouldAllowRetryOnDuplicateFailedWhenReplayHeaderPresent() throws Throwable {
        Method method = TestService.class.getMethod("handleString", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);

        org.apache.kafka.clients.consumer.ConsumerRecord<String, String> record =
                new org.apache.kafka.clients.consumer.ConsumerRecord<>("orders", 0, 100L, "k", "v");
        record.headers().add("X-Idempotent-Replay", "true".getBytes(java.nio.charset.StandardCharsets.UTF_8));

        when(joinPoint.getArgs()).thenReturn(new Object[]{"REPLAY-HEADER-KEY", record});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0");
        when(idempotent.consumerGroup()).thenReturn("orders-group");
        when(idempotent.ttl()).thenReturn("7d");
        when(idempotent.lockTimeout()).thenReturn("2m");
        when(idempotent.allowFailedRetry()).thenReturn(false);

        when(idempotentRepository.tryAcquireWithLease("REPLAY-HEADER-KEY", "orders-group", Duration.ofMinutes(2), Duration.ofDays(7)))
                .thenReturn(new io.github.alexlitvinov.idembox.core.model.AcquireOutcome(AcquireResult.DUPLICATE_FAILED, null))
                .thenReturn(new io.github.alexlitvinov.idembox.core.model.AcquireOutcome(AcquireResult.ACQUIRED, Instant.now().plusSeconds(120)));
        when(idempotentRepository.resetFailed("REPLAY-HEADER-KEY", "orders-group")).thenReturn(true);
        when(joinPoint.proceed()).thenReturn("replayed-ok");
        when(idempotentRepository.markCompleted(eq("REPLAY-HEADER-KEY"), eq("orders-group"), any(), any())).thenReturn(true);

        Object res = aspect.enforceIdempotency(joinPoint, idempotent);
        assertThat(res).isEqualTo("replayed-ok");

        verify(idempotentRepository).resetFailed("REPLAY-HEADER-KEY", "orders-group");
    }

    @Test
    @DisplayName("Senior Refinements v40: DUPLICATE_FAILED is treated as duplicate when retry/replay is not permitted")
    void shouldTreatDuplicateFailedAsDuplicateWhenNoReplayPermitted() throws Throwable {
        Method method = TestService.class.getMethod("handleString", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getArgs()).thenReturn(new Object[]{"FAILED-NO-RETRY-KEY"});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0");
        when(idempotent.consumerGroup()).thenReturn("orders-group");
        when(idempotent.ttl()).thenReturn("7d");
        when(idempotent.lockTimeout()).thenReturn("2m");
        when(idempotent.allowFailedRetry()).thenReturn(false);
        when(idempotent.swallowDuplicate()).thenReturn(false);

        when(idempotentRepository.tryAcquireWithLease("FAILED-NO-RETRY-KEY", "orders-group", Duration.ofMinutes(2), Duration.ofDays(7)))
                .thenReturn(new io.github.alexlitvinov.idembox.core.model.AcquireOutcome(AcquireResult.DUPLICATE_FAILED, null));

        assertThatThrownBy(() -> aspect.enforceIdempotency(joinPoint, idempotent))
                .isInstanceOf(DuplicateMessageException.class);

        verify(idempotentRepository, never()).resetFailed(any(), any());
    }

    @Test
    @DisplayName("Senior Refinements v44: waitForInFlightCompletion returns DUPLICATE_FAILED when state is FAILED")
    void waitForInFlightCompletionShouldReturnDuplicateFailedWhenStateIsFailed() throws Throwable {
        Method method = TestService.class.getMethod("handleString", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getArgs()).thenReturn(new Object[]{"CONCURRENT-FAIL-KEY"});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0");
        when(idempotent.consumerGroup()).thenReturn("orders-group");
        when(idempotent.ttl()).thenReturn("7d");
        when(idempotent.lockTimeout()).thenReturn("2m");
        when(idempotent.allowFailedRetry()).thenReturn(false);
        when(idempotent.swallowDuplicate()).thenReturn(false);

        // First attempt gets IN_FLIGHT_LOCKED, entering waitForInFlightCompletion
        when(idempotentRepository.tryAcquireWithLease("CONCURRENT-FAIL-KEY", "orders-group", Duration.ofMinutes(2), Duration.ofDays(7)))
                .thenReturn(new io.github.alexlitvinov.idembox.core.model.AcquireOutcome(AcquireResult.IN_FLIGHT_LOCKED, null));

        // In wait loop, worker 1 has terminally failed and status is FAILED
        ProcessingState failedState = new ProcessingState(ProcessingStatus.FAILED, null, Instant.now().plusSeconds(3600), false);
        when(idempotentRepository.getProcessingState("CONCURRENT-FAIL-KEY", "orders-group"))
                .thenReturn(failedState);

        // Must throw DuplicateMessageException for failed message (since allowFailedRetry=false), NOT swallow as completed!
        assertThatThrownBy(() -> aspect.enforceIdempotency(joinPoint, idempotent))
                .isInstanceOf(DuplicateMessageException.class);
    }

    @Test
    @DisplayName("Senior Refinements v44: getDefaultReturnValue returns completed future for CompletionStage interface")
    void shouldReturnCompletedFutureForCompletionStageReturnTypeWhenDuplicateSwallowed() throws Throwable {
        Method method = TestService.class.getMethod("asyncStage", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getArgs()).thenReturn(new Object[]{"STAGE-KEY"});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0");
        when(idempotent.consumerGroup()).thenReturn("orders-group");
        when(idempotent.ttl()).thenReturn("7d");
        when(idempotent.lockTimeout()).thenReturn("2m");
        when(idempotent.swallowDuplicate()).thenReturn(true);

        when(idempotentRepository.tryAcquireWithLease("STAGE-KEY", "orders-group", Duration.ofMinutes(2), Duration.ofDays(7)))
                .thenReturn(new io.github.alexlitvinov.idembox.core.model.AcquireOutcome(AcquireResult.DUPLICATE_COMPLETED, null));

        Object res = aspect.enforceIdempotency(joinPoint, idempotent);

        assertThat(res).isNotNull();
        assertThat(res).isInstanceOf(java.util.concurrent.CompletionStage.class);
        @SuppressWarnings("unchecked")
        java.util.concurrent.CompletionStage<String> stage = (java.util.concurrent.CompletionStage<String>) res;
        assertThat(stage.toCompletableFuture()).isCompletedWithValue(null);
    }

    @Test
    @DisplayName("Senior Refinements Round 65 & Round 72: findFallbackKafkaGroupId accepts UUID groups from trusted Message KafkaHeaders.GROUP_ID and ignores untrusted ConsumerRecord headers")
    void shouldValidateFallbackConsumerGroupHeadersAndAcceptUuid() throws Throwable {
        Method method = TestService.class.getMethod("handleString", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0");
        when(idempotent.consumerGroup()).thenReturn(""); // empty triggers fallback
        when(idempotent.ttl()).thenReturn("7d");
        when(idempotent.lockTimeout()).thenReturn("2m");
        when(idempotentRepository.tryAcquire(any(), any(), any(), any())).thenReturn(AcquireResult.ACQUIRED);
        when(joinPoint.proceed()).thenReturn("ok");

        // 1. Untrusted header "consumerGroup" with UUID on ConsumerRecord should be ignored -> fallback to method name
        ConsumerRecord<String, String> recordWithUntrustedHeader = new ConsumerRecord<>("topic", 0, 0L, "key", "payload");
        recordWithUntrustedHeader.headers().add("consumerGroup", java.util.UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8));
        when(joinPoint.getArgs()).thenReturn(new Object[]{"KEY-1", recordWithUntrustedHeader});

        aspect.enforceIdempotency(joinPoint, idempotent);
        String expectedMethodGroup = TestService.class.getName() + "#handleString";
        verify(idempotentRepository).tryAcquire(eq("KEY-1"), eq(expectedMethodGroup), any(), any());

        // 2. Trusted Spring Message header KafkaHeaders.GROUP_ID with UUID should be accepted as consumer group
        org.mockito.Mockito.clearInvocations(idempotentRepository);
        String uuidStr = "5f0d48c4-7892-453e-9926-9f6f09fb8b94";
        org.springframework.messaging.Message<String> msgWithUuidGroup = org.springframework.messaging.support.MessageBuilder
                .withPayload("payload")
                .setHeader(KafkaHeaders.GROUP_ID, uuidStr)
                .build();
        when(joinPoint.getArgs()).thenReturn(new Object[]{"KEY-2", msgWithUuidGroup});

        aspect.enforceIdempotency(joinPoint, idempotent);
        verify(idempotentRepository).tryAcquire(eq("KEY-2"), eq(uuidStr), any(), any());

        // 3. Trusted Spring Message header KafkaHeaders.GROUP_ID with valid static group should be accepted
        org.mockito.Mockito.clearInvocations(idempotentRepository);
        org.springframework.messaging.Message<String> msgWithValidGroup = org.springframework.messaging.support.MessageBuilder
                .withPayload("payload")
                .setHeader(KafkaHeaders.GROUP_ID, "payment-processors")
                .build();
        when(joinPoint.getArgs()).thenReturn(new Object[]{"KEY-3", msgWithValidGroup});

        aspect.enforceIdempotency(joinPoint, idempotent);
        verify(idempotentRepository).tryAcquire(eq("KEY-3"), eq("payment-processors"), any(), any());
    }

    @Test
    @DisplayName("Senior Refinements v46: enforceIdempotency swallows duplicate on @SendTo method and returns null")
    void shouldSwallowDuplicateOnSendToMethod() throws Throwable {
        Method method = TestService.class.getMethod("replyMethod", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getArgs()).thenReturn(new Object[]{"SEND-TO-KEY"});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0");
        when(idempotent.consumerGroup()).thenReturn("orders-group");
        when(idempotent.ttl()).thenReturn("7d");
        when(idempotent.lockTimeout()).thenReturn("2m");
        when(idempotent.swallowDuplicate()).thenReturn(true);

        when(idempotentRepository.tryAcquireWithLease("SEND-TO-KEY", "orders-group", Duration.ofMinutes(2), Duration.ofDays(7)))
                .thenReturn(new io.github.alexlitvinov.idembox.core.model.AcquireOutcome(AcquireResult.DUPLICATE_COMPLETED, null));

        Object result = aspect.enforceIdempotency(joinPoint, idempotent);
        assertThat(result).isNull();
    }

    @Test
    @DisplayName("Senior Refinements v47: Binary byte[] Kafka key is deterministically formatted as hex, avoiding [B@ identity hash code")
    void shouldFormatBinaryByteArrayKafkaKeyDeterministicallyInFallback() throws Throwable {
        Method method = TestService.class.getMethod("handleWithRecord", String.class, org.apache.kafka.clients.consumer.ConsumerRecord.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);

        byte[] binaryKey1 = new byte[]{(byte) 0x12, (byte) 0xab, (byte) 0x34, (byte) 0xcd};
        byte[] binaryKey2 = new byte[]{(byte) 0x12, (byte) 0xab, (byte) 0x34, (byte) 0xcd}; // Different instance, same bytes

        org.apache.kafka.clients.consumer.ConsumerRecord<byte[], String> record1 =
                new org.apache.kafka.clients.consumer.ConsumerRecord<>("my-topic", 0, 100L, binaryKey1, null);
        org.apache.kafka.clients.consumer.ConsumerRecord<byte[], String> record2 =
                new org.apache.kafka.clients.consumer.ConsumerRecord<>("my-topic", 0, 101L, binaryKey2, null);

        when(joinPoint.getArgs()).thenReturn(new Object[]{null, record1});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0.substring(0, 1)"); // Fails on null, triggering fallback to ConsumerRecord.key
        when(idempotent.consumerGroup()).thenReturn("orders-group");
        when(idempotent.ttl()).thenReturn("7d");
        when(idempotent.lockTimeout()).thenReturn("2m");

        when(idempotentRepository.tryAcquireWithLease(eq("12ab34cd"), eq("orders-group"), any(), any()))
                .thenReturn(new io.github.alexlitvinov.idembox.core.model.AcquireOutcome(AcquireResult.ACQUIRED, Instant.now().plusSeconds(120)));
        when(joinPoint.proceed()).thenReturn("processed");

        // First delivery
        Object result1 = aspect.enforceIdempotency(joinPoint, idempotent);
        assertThat(result1).isEqualTo("processed");
        verify(idempotentRepository).tryAcquireWithLease(eq("12ab34cd"), eq("orders-group"), any(), any());

        // Second delivery with separate byte[] instance in separate record
        when(joinPoint.getArgs()).thenReturn(new Object[]{null, record2});
        when(idempotentRepository.tryAcquireWithLease(eq("12ab34cd"), eq("orders-group"), any(), any()))
                .thenReturn(new io.github.alexlitvinov.idembox.core.model.AcquireOutcome(AcquireResult.DUPLICATE_COMPLETED, null));

        assertThatThrownBy(() -> aspect.enforceIdempotency(joinPoint, idempotent))
                .isInstanceOf(DuplicateMessageException.class)
                .hasMessageContaining("12ab34cd");
    }

    @Test
    @DisplayName("Senior Refinements Round 52: String array Kafka key is deterministically formatted avoiding identity hash code")
    void shouldFormatStringArrayKafkaKeyDeterministicallyInFallback() throws Throwable {
        Method method = TestService.class.getMethod("handleWithRecord", String.class, org.apache.kafka.clients.consumer.ConsumerRecord.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);

        String[] key1 = new String[]{"partA", "partB"};
        String[] key2 = new String[]{"partA", "partB"}; // Separate array instance

        org.apache.kafka.clients.consumer.ConsumerRecord<String[], String> record1 =
                new org.apache.kafka.clients.consumer.ConsumerRecord<>("my-topic", 0, 100L, key1, null);
        org.apache.kafka.clients.consumer.ConsumerRecord<String[], String> record2 =
                new org.apache.kafka.clients.consumer.ConsumerRecord<>("my-topic", 0, 101L, key2, null);

        when(joinPoint.getArgs()).thenReturn(new Object[]{null, record1});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0.substring(0, 1)"); // Fails on null, triggering fallback to ConsumerRecord.key
        when(idempotent.consumerGroup()).thenReturn("orders-group");
        when(idempotent.ttl()).thenReturn("7d");
        when(idempotent.lockTimeout()).thenReturn("2m");

        when(idempotentRepository.tryAcquireWithLease(eq("[partA, partB]"), eq("orders-group"), any(), any()))
                .thenReturn(new io.github.alexlitvinov.idembox.core.model.AcquireOutcome(AcquireResult.ACQUIRED, Instant.now().plusSeconds(120)));
        when(joinPoint.proceed()).thenReturn("processed");

        // First delivery
        Object result1 = aspect.enforceIdempotency(joinPoint, idempotent);
        assertThat(result1).isEqualTo("processed");
        verify(idempotentRepository).tryAcquireWithLease(eq("[partA, partB]"), eq("orders-group"), any(), any());

        // Second delivery with separate String[] instance
        when(joinPoint.getArgs()).thenReturn(new Object[]{null, record2});
        when(idempotentRepository.tryAcquireWithLease(eq("[partA, partB]"), eq("orders-group"), any(), any()))
                .thenReturn(new io.github.alexlitvinov.idembox.core.model.AcquireOutcome(AcquireResult.DUPLICATE_COMPLETED, null));

        assertThatThrownBy(() -> aspect.enforceIdempotency(joinPoint, idempotent))
                .isInstanceOf(DuplicateMessageException.class)
                .hasMessageContaining("[partA, partB]");
    }

    @Test
    @DisplayName("Senior Refinements v47: isEventLoopOrNonBlockingThread detects Netty and event loop thread names")
    void shouldDetectEventLoopAndNonBlockingThreads() {
        Thread nettyThread = new Thread(() -> {}, "default-eventLoopGroup-2-1");
        Thread epollThread = new Thread(() -> {}, "epollEventLoopGroup-3-2");
        Thread reactorThread = new Thread(() -> {}, "reactor-http-epoll-4");
        Thread normalThread = new Thread(() -> {}, "custom-worker-thread-1");

        assertThat(IdempotentConsumerAspect.isEventLoopOrNonBlockingThread(nettyThread)).isTrue();
        assertThat(IdempotentConsumerAspect.isEventLoopOrNonBlockingThread(epollThread)).isTrue();
        assertThat(IdempotentConsumerAspect.isEventLoopOrNonBlockingThread(reactorThread)).isTrue();
        assertThat(IdempotentConsumerAspect.isEventLoopOrNonBlockingThread(normalThread)).isFalse();
        assertThat(IdempotentConsumerAspect.isEventLoopOrNonBlockingThread(null)).isFalse();
    }

    @Test
    @DisplayName("Senior Refinements v49: DLT Replay race condition with IN_FLIGHT_LOCKED throws ConcurrentProcessingException and never executes business logic")
    void shouldThrowConcurrentProcessingExceptionWhenReplayAcquireReturnsInFlightLocked() throws Throwable {
        Method method = TestService.class.getMethod("handleString", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getArgs()).thenReturn(new Object[]{"REPLAY-RACE-KEY"});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0");
        when(idempotent.consumerGroup()).thenReturn("orders-group");
        when(idempotent.ttl()).thenReturn("7d");
        when(idempotent.lockTimeout()).thenReturn("2m");
        when(idempotent.allowFailedRetry()).thenReturn(true);

        when(idempotentRepository.tryAcquireWithLease("REPLAY-RACE-KEY", "orders-group", Duration.ofMinutes(2), Duration.ofDays(7)))
                .thenReturn(new io.github.alexlitvinov.idembox.core.model.AcquireOutcome(AcquireResult.DUPLICATE_FAILED, null))
                .thenReturn(new io.github.alexlitvinov.idembox.core.model.AcquireOutcome(AcquireResult.IN_FLIGHT_LOCKED, null));
        when(idempotentRepository.resetFailed("REPLAY-RACE-KEY", "orders-group")).thenReturn(true);

        // waitForInFlightCompletion probe also sees locked
        when(idempotentRepository.getProcessingState("REPLAY-RACE-KEY", "orders-group"))
                .thenReturn(new io.github.alexlitvinov.idembox.core.model.ProcessingState(
                        io.github.alexlitvinov.idembox.core.model.ProcessingStatus.PROCESSING,
                        Instant.now().plusSeconds(60),
                        Instant.now().plusSeconds(3600),
                        true
                ));

        assertThatThrownBy(() -> aspect.enforceIdempotency(joinPoint, idempotent))
                .isInstanceOf(ConcurrentProcessingException.class)
                .hasMessageContaining("REPLAY-RACE-KEY");

        verify(joinPoint, never()).proceed();
    }

    @Test
    @DisplayName("Senior Refinements Round 73: Synchronous @Idempotent method executes handleFailure synchronously on caller thread to preserve ThreadLocal transaction status and prevent retry races")
    void shouldExecuteHandleFailureSynchronouslyOnCallerThreadForSyncMethod() throws Throwable {
        Method method = TestService.class.getMethod("handleString", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getArgs()).thenReturn(new Object[]{"EVENTLOOP-FAIL-KEY"});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0");
        when(idempotent.consumerGroup()).thenReturn("orders-group");
        when(idempotent.ttl()).thenReturn("7d");
        when(idempotent.lockTimeout()).thenReturn("2m");

        when(idempotentRepository.tryAcquireWithLease("EVENTLOOP-FAIL-KEY", "orders-group", Duration.ofMinutes(2), Duration.ofDays(7)))
                .thenReturn(new io.github.alexlitvinov.idembox.core.model.AcquireOutcome(AcquireResult.ACQUIRED, Instant.now().plusSeconds(120)));
        when(joinPoint.proceed()).thenThrow(new RuntimeException("Simulated sync failure"));

        Thread[] markFailedThread = new Thread[1];
        org.mockito.Mockito.doAnswer(inv -> {
            markFailedThread[0] = Thread.currentThread();
            return null;
        }).when(idempotentRepository).markFailed(eq("EVENTLOOP-FAIL-KEY"), eq("orders-group"), eq("Simulated sync failure"), any(Instant.class));

        Throwable[] thrown = new Throwable[1];

        Thread nettyThread = new Thread(() -> {
            try {
                aspect.enforceIdempotency(joinPoint, idempotent);
            } catch (Throwable t) {
                thrown[0] = t;
            }
        }, "default-eventLoopGroup-4-1");

        nettyThread.start();
        nettyThread.join();

        assertThat(thrown[0]).isInstanceOf(RuntimeException.class).hasMessage("Simulated sync failure");
        // markFailed MUST have completed synchronously on the caller thread before enforceIdempotency returned
        verify(idempotentRepository).markFailed(eq("EVENTLOOP-FAIL-KEY"), eq("orders-group"), eq("Simulated sync failure"), any(Instant.class));
        assertThat(markFailedThread[0]).isSameAs(nettyThread);
    }

    @Test
    @DisplayName("Senior Refinements v50: enforceIdempotency allows JsonNode argument without throwing IllegalArgumentException")
    void shouldAllowJsonNodeArgumentInIdempotentConsumer() throws Throwable {
        Method method = TestService.class.getMethod("handleJson", com.fasterxml.jackson.databind.JsonNode.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);

        com.fasterxml.jackson.databind.node.ObjectNode node = com.fasterxml.jackson.databind.node.JsonNodeFactory.instance.objectNode();
        node.put("orderId", "NODE-123");

        when(joinPoint.getArgs()).thenReturn(new Object[]{node});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0.get('orderId').asText()");
        when(idempotent.consumerGroup()).thenReturn("orders-group");
        when(idempotent.ttl()).thenReturn("7d");
        when(idempotent.lockTimeout()).thenReturn("2m");

        when(idempotentRepository.tryAcquireWithLease(eq("NODE-123"), eq("orders-group"), any(), any()))
                .thenReturn(new io.github.alexlitvinov.idembox.core.model.AcquireOutcome(AcquireResult.ACQUIRED, Instant.now().plusSeconds(120)));
        when(joinPoint.proceed()).thenReturn("done");

        Object result = aspect.enforceIdempotency(joinPoint, idempotent);

        assertThat(result).isEqualTo("done");
        verify(joinPoint).proceed();
    }

    @Test
    @DisplayName("Senior Refinements v50: SpEL condition evaluation exception fails fast by default")
    void shouldFailFastWhenConditionThrowsEvaluationExceptionByDefault() throws Throwable {
        Method method = TestService.class.getMethod("handleString", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getArgs()).thenReturn(new Object[]{"TEST-ID"});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        // Condition references non-existent property without safe navigation, throwing EvaluationException
        when(idempotent.condition()).thenReturn("#p0.nonExistentProperty == 'FOO'");

        org.assertj.core.api.Assertions.assertThatThrownBy(() -> aspect.enforceIdempotency(joinPoint, idempotent))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Failed to evaluate @Idempotent condition");

        verify(joinPoint, never()).proceed();
        verify(idempotentRepository, never()).tryAcquireWithLease(any(), any(), any(), any());
    }

    @Test
    @DisplayName("Senior Refinements v50: SpEL condition evaluation exception safely bypasses deduplication when failOnConditionError=false")
    void shouldBypassDeduplicationWhenConditionThrowsEvaluationExceptionAndFailFastDisabled() throws Throwable {
        properties.getIdempotency().setFailOnConditionError(false);
        Method method = TestService.class.getMethod("handleString", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getArgs()).thenReturn(new Object[]{"TEST-ID"});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.condition()).thenReturn("#p0.nonExistentProperty == 'FOO'");
        when(joinPoint.proceed()).thenReturn("bypassed");

        Object result = aspect.enforceIdempotency(joinPoint, idempotent);

        assertThat(result).isEqualTo("bypassed");
        verify(joinPoint).proceed();
        verify(idempotentRepository, never()).tryAcquireWithLease(any(), any(), any(), any());
        verify(idempotentRepository, never()).tryAcquire(any(), any(), any(), any());
    }

    @Test
    @DisplayName("Senior Refinements Round 51: enforceIdempotency logs warning and returns null when swallowing duplicate for non-container return type")
    void shouldReturnNullAndWarnWhenSwallowingDuplicateForNonContainerReturnType() throws Throwable {
        Method method = TestService.class.getMethod("handleString", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getArgs()).thenReturn(new Object[]{"KEY-NON-CONTAINER"});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0");
        when(idempotent.consumerGroup()).thenReturn("orders-group");
        when(idempotent.ttl()).thenReturn("7d");
        when(idempotent.lockTimeout()).thenReturn("2m");
        when(idempotent.swallowDuplicate()).thenReturn(true);

        when(idempotentRepository.tryAcquireWithLease("KEY-NON-CONTAINER", "orders-group", Duration.ofMinutes(2), Duration.ofDays(7)))
                .thenReturn(new io.github.alexlitvinov.idembox.core.model.AcquireOutcome(AcquireResult.DUPLICATE_COMPLETED, null));

        Object result = aspect.enforceIdempotency(joinPoint, idempotent);
        assertThat(result).isNull();
    }

    @Test
    @DisplayName("Senior Refinements Round 69: in-flight wait timeout is clamped to MAX_IN_FLIGHT_WAIT_TIMEOUT (5s) to prevent consumer stall and group rebalance")
    void shouldClampInFlightWaitTimeoutToFiveSeconds() {
        properties.getIdempotency().setInFlightWaitTimeout(Duration.ofSeconds(15));
        assertThat(properties.getIdempotency().getInFlightWaitTimeout())
                .isEqualTo(IdemboxProperties.IdempotencyProperties.MAX_IN_FLIGHT_WAIT_TIMEOUT);

        // Test with null/negative duration falling back safely
        properties.getIdempotency().setInFlightWaitTimeout(null);
        assertThat(properties.getIdempotency().getInFlightWaitTimeout()).isEqualTo(Duration.ofSeconds(3));

        properties.getIdempotency().setInFlightWaitTimeout(Duration.ofSeconds(-5));
        assertThat(properties.getIdempotency().getInFlightWaitTimeout()).isEqualTo(Duration.ofSeconds(3));
    }

    @Test
    @DisplayName("Round 56: @Transactional(noRollbackFor = RuntimeException.class) marks COMPLETED and does not mark rollback-only")
    void transactionalMethodWithNoRollbackForRuntimeExceptionShouldMarkCompleted() throws Throwable {
        org.springframework.transaction.support.TransactionSynchronizationManager.setActualTransactionActive(true);
        Method method = TestService.class.getMethod("transactionalMethodWithNoRollbackForRuntimeException", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getArgs()).thenReturn(new Object[]{"TX-NO-ROLLBACK-RT"});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0");
        when(idempotent.consumerGroup()).thenReturn("orders-group");
        when(idempotent.ttl()).thenReturn("7d");
        when(idempotent.lockTimeout()).thenReturn("2m");

        when(idempotentRepository.tryAcquireWithLease(any(), any(), any(), any()))
                .thenReturn(new io.github.alexlitvinov.idembox.core.model.AcquireOutcome(AcquireResult.ACQUIRED, null));
        when(joinPoint.proceed()).thenThrow(new CustomNoRollbackException("Expected business error that commits"));

        assertThatThrownBy(() -> aspect.enforceIdempotency(joinPoint, idempotent))
                .isInstanceOf(CustomNoRollbackException.class);

        // Must mark COMPLETED because Spring commits the transaction on noRollbackFor
        verify(idempotentRepository).markCompleted(eq("TX-NO-ROLLBACK-RT"), eq("orders-group"), any(), any());
        verify(idempotentRepository, never()).markFailed(any(), any(), any(), any());
    }

    @Test
    @DisplayName("Round 56 & Round 72: CompletionStage completion offloads to provided asyncExecutor on event-loop thread instead of ForkJoinPool.commonPool")
    void shouldOffloadToProvidedAsyncExecutorOnEventLoopThread() throws Throwable {
        java.util.concurrent.atomic.AtomicBoolean executedOnCustomExecutor = new java.util.concurrent.atomic.AtomicBoolean(false);
        java.util.concurrent.Executor customExecutor = r -> {
            executedOnCustomExecutor.set(true);
            r.run();
        };
        IdempotentConsumerAspect customAspect = new IdempotentConsumerAspect(
                idempotentRepository, spelKeyExtractor, properties, IdemboxMetrics.noOp(), customExecutor);

        Method method = TestService.class.getMethod("asyncCompute", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getArgs()).thenReturn(new Object[]{"KEY-ASYNC-OFFLOAD"});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0");
        when(idempotent.consumerGroup()).thenReturn("orders-group");
        when(idempotent.ttl()).thenReturn("7d");
        when(idempotent.lockTimeout()).thenReturn("2m");

        when(idempotentRepository.tryAcquireWithLease(any(), any(), any(), any()))
                .thenReturn(new io.github.alexlitvinov.idembox.core.model.AcquireOutcome(AcquireResult.ACQUIRED, null));
        when(idempotentRepository.markCompleted(eq("KEY-ASYNC-OFFLOAD"), eq("orders-group"), any(), any()))
                .thenReturn(true);
        when(joinPoint.proceed()).thenReturn(CompletableFuture.completedFuture("success"));

        java.util.concurrent.atomic.AtomicReference<Throwable> threadEx = new java.util.concurrent.atomic.AtomicReference<>();
        Thread eventLoopThread = new Thread(() -> {
            try {
                CompletableFuture<?> res = (CompletableFuture<?>) customAspect.enforceIdempotency(joinPoint, idempotent);
                res.join();
            } catch (Throwable e) {
                threadEx.set(e);
            }
        }, "epollEventLoopGroup-3-1");

        eventLoopThread.start();
        eventLoopThread.join();

        assertThat(threadEx.get()).isNull();
        assertThat(executedOnCustomExecutor.get()).isTrue();
        verify(idempotentRepository).markCompleted(eq("KEY-ASYNC-OFFLOAD"), eq("orders-group"), any(), any());
    }

    @Test
    @DisplayName("Round 57 & Round 72: executeAsyncOffload safely falls back to ForkJoinPool when asyncExecutor throws RejectedExecutionException")
    void shouldFallbackToForkJoinPoolWhenAsyncExecutorRejects() throws Throwable {
        Executor rejectingExecutor = r -> {
            throw new RejectedExecutionException("Queue full and web thread rejected");
        };
        IdempotentConsumerAspect customAspect = new IdempotentConsumerAspect(
                idempotentRepository, spelKeyExtractor, properties, IdemboxMetrics.noOp(), rejectingExecutor);

        Method method = TestService.class.getMethod("asyncCompute", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getArgs()).thenReturn(new Object[]{"KEY-REJECT-FALLBACK"});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0");
        when(idempotent.consumerGroup()).thenReturn("orders-group");
        when(idempotent.ttl()).thenReturn("7d");
        when(idempotent.lockTimeout()).thenReturn("2m");

        when(idempotentRepository.tryAcquireWithLease(any(), any(), any(), any()))
                .thenReturn(new io.github.alexlitvinov.idembox.core.model.AcquireOutcome(AcquireResult.ACQUIRED, null));
        when(idempotentRepository.markCompleted(eq("KEY-REJECT-FALLBACK"), eq("orders-group"), any(), any()))
                .thenReturn(true);
        when(joinPoint.proceed()).thenReturn(CompletableFuture.completedFuture("success"));

        java.util.concurrent.atomic.AtomicReference<Throwable> threadEx = new java.util.concurrent.atomic.AtomicReference<>();
        Thread eventLoopThread = new Thread(() -> {
            try {
                customAspect.enforceIdempotency(joinPoint, idempotent);
            } catch (Throwable e) {
                threadEx.set(e);
            }
        }, "reactor-http-epoll-4");

        eventLoopThread.start();
        eventLoopThread.join();

        assertThat(threadEx.get()).isNull();
        org.awaitility.Awaitility.await()
                .atMost(java.time.Duration.ofSeconds(3))
                .untilAsserted(() -> verify(idempotentRepository).markCompleted(eq("KEY-REJECT-FALLBACK"), eq("orders-group"), any(), any()));
    }

    @Test
    @DisplayName("Senior Refinements Round 58: Aspect rejects runtime return of JDK 9+ Flow.Publisher with IllegalStateException")
    void shouldRejectFlowPublisherInvocationResult() throws Throwable {
        Method method = TestService.class.getMethod("handleString", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getArgs()).thenReturn(new Object[]{"KEY-FLOW"});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0");
        when(idempotent.consumerGroup()).thenReturn("orders-group");
        when(idempotent.ttl()).thenReturn("7d");
        when(idempotent.lockTimeout()).thenReturn("2m");

        when(idempotentRepository.tryAcquireWithLease(any(), any(), any(), any()))
                .thenReturn(new io.github.alexlitvinov.idembox.core.model.AcquireOutcome(AcquireResult.ACQUIRED, null));

        try (java.util.concurrent.SubmissionPublisher<String> publisher = new java.util.concurrent.SubmissionPublisher<>()) {
            when(joinPoint.proceed()).thenReturn(publisher);

            assertThatThrownBy(() -> aspect.enforceIdempotency(joinPoint, idempotent))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Reactive return type detected");

            verify(idempotentRepository, never()).markCompleted(any(), any(), any(), any());
            verify(idempotentRepository).markTerminalFailed(eq("KEY-FLOW"), eq("orders-group"), any(), any(), any());
        }
    }

    @Test
    @DisplayName("Senior Refinements Round 60 & Round 74: Checked exception from @jakarta.transaction.Transactional without rollbackOn calls markCompleted")
    void shouldMarkCompletedWhenCheckedExceptionThrownFromJakartaTransactionalWithoutRollbackOn() throws Throwable {
        org.springframework.transaction.support.TransactionSynchronizationManager.setActualTransactionActive(true);
        Method method = TestService.class.getMethod("jakartaTransactionalMethodWithoutRollback", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getArgs()).thenReturn(new Object[]{"JAKARTA-TX-1"});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0");
        when(idempotent.consumerGroup()).thenReturn("orders-group");
        when(idempotent.ttl()).thenReturn("7d");
        when(idempotent.lockTimeout()).thenReturn("2m");

        when(idempotentRepository.tryAcquire(any(), any(), any(), any()))
                .thenReturn(AcquireResult.ACQUIRED);
        when(joinPoint.proceed()).thenThrow(new Exception("checked error in JTA tx"));

        assertThatThrownBy(() -> aspect.enforceIdempotency(joinPoint, idempotent))
                .isInstanceOf(Exception.class)
                .hasMessage("checked error in JTA tx");

        verify(idempotentRepository).markCompleted(eq("JAKARTA-TX-1"), eq("orders-group"), eq(Duration.ofDays(7)), org.mockito.ArgumentMatchers.nullable(Instant.class));
        verify(idempotentRepository, never()).markFailed(any(), any(), any());
        verify(idempotentRepository, never()).markFailed(any(), any(), any(), any());
    }

    @Test
    @DisplayName("Senior Refinements Round 60: Checked exception from @jakarta.transaction.Transactional WITH rollbackOn releases lock for retry")
    void shouldReleaseLockWhenCheckedExceptionThrownFromJakartaTransactionalWithRollbackOn() throws Throwable {
        org.springframework.transaction.support.TransactionSynchronizationManager.setActualTransactionActive(true);
        Method method = TestService.class.getMethod("jakartaTransactionalMethodWithRollback", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getArgs()).thenReturn(new Object[]{"JAKARTA-TX-2"});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0");
        when(idempotent.consumerGroup()).thenReturn("orders-group");
        when(idempotent.ttl()).thenReturn("7d");
        when(idempotent.lockTimeout()).thenReturn("2m");

        when(idempotentRepository.tryAcquire(any(), any(), any(), any()))
                .thenReturn(AcquireResult.ACQUIRED);
        when(joinPoint.proceed()).thenThrow(new Exception("checked error with JTA rollback"));

        assertThatThrownBy(() -> aspect.enforceIdempotency(joinPoint, idempotent))
                .isInstanceOf(Exception.class)
                .hasMessage("checked error with JTA rollback");

        verify(idempotentRepository).markFailed(eq("JAKARTA-TX-2"), eq("orders-group"), eq("checked error with JTA rollback"), org.mockito.ArgumentMatchers.nullable(Instant.class));
        verify(idempotentRepository, never()).markCompleted(any(), any(), any());
        verify(idempotentRepository, never()).markCompleted(any(), any(), any(), any());
    }

    @Test
    @DisplayName("Senior Refinements Round 60: Exception matching no-rollback rule from Jakarta Transactional marks COMPLETED")
    void shouldMarkCompletedWhenNoRollbackRuleExceptionThrownFromJakartaTransactional() throws Throwable {
        org.springframework.transaction.support.TransactionSynchronizationManager.setActualTransactionActive(true);
        Method method = TestService.class.getMethod("jakartaTransactionalMethodWithNoRollbackOn", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getArgs()).thenReturn(new Object[]{"JAKARTA-TX-3"});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0");
        when(idempotent.consumerGroup()).thenReturn("orders-group");
        when(idempotent.ttl()).thenReturn("7d");
        when(idempotent.lockTimeout()).thenReturn("2m");

        when(idempotentRepository.tryAcquire(any(), any(), any(), any()))
                .thenReturn(AcquireResult.ACQUIRED);
        when(joinPoint.proceed()).thenThrow(new CustomNoRollbackException("Expected business error in JTA tx that commits"));

        assertThatThrownBy(() -> aspect.enforceIdempotency(joinPoint, idempotent))
                .isInstanceOf(CustomNoRollbackException.class)
                .hasMessage("Expected business error in JTA tx that commits");

        verify(idempotentRepository, never()).markFailed(any(), any(), any());
        verify(idempotentRepository, never()).markFailed(any(), any(), any(), any());
        verify(idempotentRepository).markCompleted(eq("JAKARTA-TX-3"), eq("orders-group"), eq(Duration.ofDays(7)), org.mockito.ArgumentMatchers.nullable(Instant.class));
    }

    @Test
    @DisplayName("Senior Refinements Round 60: CompletableFuture async method with @jakarta.transaction.Transactional marks COMPLETED on async failure")
    void shouldMarkCompletedWhenAsyncJakartaTransactionalMethodFails() throws Throwable {
        org.springframework.transaction.support.TransactionSynchronizationManager.setActualTransactionActive(true);
        Method method = TestService.class.getMethod("jakartaAsyncTransactionalMethod", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getArgs()).thenReturn(new Object[]{"JAKARTA-TX-4"});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0");
        when(idempotent.consumerGroup()).thenReturn("orders-group");
        when(idempotent.ttl()).thenReturn("7d");
        when(idempotent.lockTimeout()).thenReturn("2m");

        when(idempotentRepository.tryAcquire(any(), any(), any(), any()))
                .thenReturn(AcquireResult.ACQUIRED);

        CompletableFuture<String> future = new CompletableFuture<>();
        when(joinPoint.proceed()).thenReturn(future);

        Object result = aspect.enforceIdempotency(joinPoint, idempotent);
        assertThat(result).isInstanceOf(CompletableFuture.class);

        // Fail async stage
        future.completeExceptionally(new RuntimeException("async error after tx commit"));

        verify(idempotentRepository, never()).markFailed(any(), any(), any());
        verify(idempotentRepository, never()).markFailed(any(), any(), any(), any());
        verify(idempotentRepository).markCompleted(eq("JAKARTA-TX-4"), eq("orders-group"), eq(Duration.ofDays(7)), org.mockito.ArgumentMatchers.nullable(Instant.class));
    }

    @Test
    @DisplayName("Senior Refinements Round 61: Specific rollbackFor wins over general noRollbackFor based on hierarchy depth in Spring @Transactional")
    void specificRollbackShouldWinOverGeneralNoRollbackInSpringTransactional() throws Throwable {
        org.springframework.transaction.support.TransactionSynchronizationManager.setActualTransactionActive(true);
        Method method = TestService.class.getMethod("transactionalMethodWithSpecificRollbackAndGeneralNoRollback", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getArgs()).thenReturn(new Object[]{"TX-DEPTH-1"});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0");
        when(idempotent.consumerGroup()).thenReturn("orders-group");
        when(idempotent.ttl()).thenReturn("7d");
        when(idempotent.lockTimeout()).thenReturn("2m");

        when(idempotentRepository.tryAcquire(any(), any(), any(), any()))
                .thenReturn(AcquireResult.ACQUIRED);
        when(joinPoint.proceed()).thenThrow(new SpecificBusinessException("specific fail"));

        assertThatThrownBy(() -> aspect.enforceIdempotency(joinPoint, idempotent))
                .isInstanceOf(SpecificBusinessException.class)
                .hasMessage("specific fail");

        // Specific rollbackFor (depth 0) beats general noRollbackFor (depth 1) -> rollback -> markFailed
        verify(idempotentRepository).markFailed(eq("TX-DEPTH-1"), eq("orders-group"), eq("specific fail"), org.mockito.ArgumentMatchers.nullable(Instant.class));
        verify(idempotentRepository, never()).markCompleted(any(), any(), any(), any());
    }

    @Test
    @DisplayName("Senior Refinements Round 61: General noRollbackFor wins when specific rollbackFor does not match in Spring @Transactional")
    void generalNoRollbackShouldWinWhenSpecificRollbackDoesNotMatchInSpringTransactional() throws Throwable {
        org.springframework.transaction.support.TransactionSynchronizationManager.setActualTransactionActive(true);
        Method method = TestService.class.getMethod("transactionalMethodWithSpecificRollbackAndGeneralNoRollback", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getArgs()).thenReturn(new Object[]{"TX-DEPTH-2"});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0");
        when(idempotent.consumerGroup()).thenReturn("orders-group");
        when(idempotent.ttl()).thenReturn("7d");
        when(idempotent.lockTimeout()).thenReturn("2m");

        when(idempotentRepository.tryAcquire(any(), any(), any(), any()))
                .thenReturn(AcquireResult.ACQUIRED);
        when(joinPoint.proceed()).thenThrow(new OtherBusinessException("other fail"));

        assertThatThrownBy(() -> aspect.enforceIdempotency(joinPoint, idempotent))
                .isInstanceOf(OtherBusinessException.class)
                .hasMessage("other fail");

        // rollbackFor doesn't match, general noRollbackFor (depth 1) wins -> commit -> markCompleted
        verify(idempotentRepository, never()).markFailed(any(), any(), any());
        verify(idempotentRepository, never()).markFailed(any(), any(), any(), any());
        verify(idempotentRepository).markCompleted(eq("TX-DEPTH-2"), eq("orders-group"), any(), org.mockito.ArgumentMatchers.nullable(Instant.class));
    }

    @Test
    @DisplayName("Senior Refinements Round 61: Specific rollbackOn wins over general no-rollback rule based on hierarchy depth in Jakarta @Transactional")
    void specificRollbackShouldWinOverGeneralNoRollbackInJakartaTransactional() throws Throwable {
        org.springframework.transaction.support.TransactionSynchronizationManager.setActualTransactionActive(true);
        Method method = TestService.class.getMethod("jakartaMethodWithSpecificRollbackAndGeneralNoRollback", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getArgs()).thenReturn(new Object[]{"JAKARTA-DEPTH-1"});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0");
        when(idempotent.consumerGroup()).thenReturn("orders-group");
        when(idempotent.ttl()).thenReturn("7d");
        when(idempotent.lockTimeout()).thenReturn("2m");

        when(idempotentRepository.tryAcquire(any(), any(), any(), any()))
                .thenReturn(AcquireResult.ACQUIRED);
        when(joinPoint.proceed()).thenThrow(new SpecificBusinessException("JTA specific fail"));

        assertThatThrownBy(() -> aspect.enforceIdempotency(joinPoint, idempotent))
                .isInstanceOf(SpecificBusinessException.class)
                .hasMessage("JTA specific fail");

        // Specific rollbackOn beats general dontRollbackOn -> rollback -> markFailed
        verify(idempotentRepository).markFailed(eq("JAKARTA-DEPTH-1"), eq("orders-group"), eq("JTA specific fail"), org.mockito.ArgumentMatchers.nullable(Instant.class));
        verify(idempotentRepository, never()).markCompleted(any(), any(), any(), any());
    }

    @Test
    @DisplayName("Senior Refinements Round 61: General no-rollback rule wins when specific rollbackOn does not match in Jakarta @Transactional")
    void generalNoRollbackShouldWinWhenSpecificRollbackDoesNotMatchInJakartaTransactional() throws Throwable {
        org.springframework.transaction.support.TransactionSynchronizationManager.setActualTransactionActive(true);
        Method method = TestService.class.getMethod("jakartaMethodWithSpecificRollbackAndGeneralNoRollback", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getArgs()).thenReturn(new Object[]{"JAKARTA-DEPTH-2"});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0");
        when(idempotent.consumerGroup()).thenReturn("orders-group");
        when(idempotent.ttl()).thenReturn("7d");
        when(idempotent.lockTimeout()).thenReturn("2m");

        when(idempotentRepository.tryAcquire(any(), any(), any(), any()))
                .thenReturn(AcquireResult.ACQUIRED);
        when(joinPoint.proceed()).thenThrow(new OtherBusinessException("JTA other fail"));

        assertThatThrownBy(() -> aspect.enforceIdempotency(joinPoint, idempotent))
                .isInstanceOf(OtherBusinessException.class)
                .hasMessage("JTA other fail");

        // rollbackOn doesn't match, general dontRollbackOn wins -> commit -> markCompleted
        verify(idempotentRepository, never()).markFailed(any(), any(), any());
        verify(idempotentRepository, never()).markFailed(any(), any(), any(), any());
        verify(idempotentRepository).markCompleted(eq("JAKARTA-DEPTH-2"), eq("orders-group"), any(), org.mockito.ArgumentMatchers.nullable(Instant.class));
    }

    @Test
    @DisplayName("Senior Refinements Round 61: IN_FLIGHT_LOCKED on non-blocking Netty / Reactor thread immediately throws ConcurrentProcessingException without sleeping")
    void inFlightLockOnNonBlockingEventLoopThreadShouldImmediatelyThrowConcurrentProcessingExceptionWithoutSleeping() throws Throwable {
        when(idempotent.key()).thenReturn("#p0");
        when(idempotent.consumerGroup()).thenReturn("orders-group");
        when(idempotent.ttl()).thenReturn("7d");
        when(idempotent.lockTimeout()).thenReturn("2m");

        when(joinPoint.getArgs()).thenReturn(new Object[]{"NON-BLOCKING-KEY"});
        when(joinPoint.getTarget()).thenReturn(new TestService());
        Method method = TestService.class.getMethod("handleString", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);

        when(idempotentRepository.tryAcquireWithLease(eq("NON-BLOCKING-KEY"), eq("orders-group"), any(), any()))
                .thenReturn(new io.github.alexlitvinov.idembox.core.model.AcquireOutcome(AcquireResult.IN_FLIGHT_LOCKED, null));

        // Execute on a thread whose name matches Netty / reactor non-blocking conventions
        java.util.concurrent.atomic.AtomicReference<Throwable> thrown = new java.util.concurrent.atomic.AtomicReference<>();
        long start = System.currentTimeMillis();
        Thread thread = new Thread(() -> {
            try {
                aspect.enforceIdempotency(joinPoint, idempotent);
            } catch (Throwable t) {
                thrown.set(t);
            }
        }, "reactor-http-epoll-4");

        thread.start();
        thread.join(2000);
        long duration = System.currentTimeMillis() - start;

        assertThat(thrown.get())
                .isNotNull()
                .isInstanceOf(ConcurrentProcessingException.class);
        // Ensure no blocking sleep (default inFlightWait is 5000ms)
        assertThat(duration).isLessThan(1500);
        verify(joinPoint, never()).proceed();
    }

    @Test
    @DisplayName("Senior Refinements Round 62: normalizeConsumerGroup avoids splitting surrogate pair at boundary")
    void normalizeConsumerGroupShouldNotSplitSurrogatePairAtBoundary() {
        String prefix189 = "a".repeat(189);
        String emoji = "\uD83D\uDE00";
        String rest = "b".repeat(100);
        String group = prefix189 + emoji + rest;

        String normalized = IdempotentConsumerAspect.normalizeConsumerGroup(group);

        assertThat(normalized).startsWith(prefix189 + "_");
        assertThat(normalized.length()).isEqualTo(189 + 1 + 32);

        byte[] bytes = normalized.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        String redecoded = new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
        assertThat(redecoded).isEqualTo(normalized);
    }

    @Test
    @DisplayName("Senior Refinements Round 62: normalizeConsumerGroup truncates BMP characters at 190")
    void normalizeConsumerGroupShouldTruncateAt190WhenNoSurrogateSplit() {
        String group = "a".repeat(300);
        String normalized = IdempotentConsumerAspect.normalizeConsumerGroup(group);

        assertThat(normalized).startsWith("a".repeat(190) + "_");
        assertThat(normalized.length()).isEqualTo(190 + 1 + 32);
    }

    @Test
    @DisplayName("IdempotentConsumerAspect should be ordered with HIGHEST_PRECEDENCE before @Transactional to prevent HikariCP connection pool starvation")
    void shouldBeOrderedWithHighestPrecedence() {
        org.springframework.core.annotation.Order order =
                IdempotentConsumerAspect.class.getAnnotation(org.springframework.core.annotation.Order.class);
        assertThat(order).isNotNull();
        assertThat(order.value()).isEqualTo(IdempotentConsumerAspect.DEFAULT_ORDER);
        assertThat(order.value()).isEqualTo(org.springframework.core.Ordered.HIGHEST_PRECEDENCE);
        assertThat(aspect.getOrder()).isEqualTo(IdempotentConsumerAspect.DEFAULT_ORDER);

        IdemboxProperties customProps = new IdemboxProperties();
        customProps.getIdempotency().setOrder(100);
        IdempotentConsumerAspect customAspect = new IdempotentConsumerAspect(
                idempotentRepository, spelKeyExtractor, customProps, IdemboxMetrics.noOp());
        assertThat(customAspect.getOrder()).isEqualTo(100);
        customAspect.setOrder(200);
        assertThat(customAspect.getOrder()).isEqualTo(200);
    }

    @Test
    @DisplayName("Should release lock via TransactionSynchronization when surrounding transaction rolls back after completion")
    void shouldReleaseLockWhenSurroundingTransactionRollsBackAfterCompletion() throws Throwable {
        org.springframework.transaction.support.TransactionSynchronizationManager.initSynchronization();
        try {
            Method method = TestService.class.getMethod("handleString", String.class);
            when(joinPoint.getSignature()).thenReturn(methodSignature);
            when(methodSignature.getMethod()).thenReturn(method);
            when(joinPoint.getArgs()).thenReturn(new Object[]{"KEY-SURROUNDING-ROLLBACK"});
            when(joinPoint.getTarget()).thenReturn(new TestService());

            when(idempotent.key()).thenReturn("#p0");
            when(idempotent.consumerGroup()).thenReturn("orders-group");
            when(idempotent.ttl()).thenReturn("1m");
            when(idempotent.lockTimeout()).thenReturn("2m");

            Instant lease = Instant.now().plusSeconds(120).truncatedTo(java.time.temporal.ChronoUnit.MICROS);
            when(idempotentRepository.tryAcquireWithLease(eq("KEY-SURROUNDING-ROLLBACK"), eq("orders-group"), any(), any()))
                    .thenReturn(new io.github.alexlitvinov.idembox.core.model.AcquireOutcome(AcquireResult.ACQUIRED, lease));
            when(joinPoint.proceed()).thenReturn("ok");
            when(idempotentRepository.markCompleted(eq("KEY-SURROUNDING-ROLLBACK"), eq("orders-group"), any(), eq(lease)))
                    .thenReturn(true);

            Object result = aspect.enforceIdempotency(joinPoint, idempotent);
            assertThat(result).isEqualTo("ok");

            java.util.List<org.springframework.transaction.support.TransactionSynchronization> syncs =
                    org.springframework.transaction.support.TransactionSynchronizationManager.getSynchronizations();
            assertThat(syncs).hasSize(1);

            // Simulate surrounding transaction rollback
            syncs.get(0).afterCompletion(org.springframework.transaction.support.TransactionSynchronization.STATUS_ROLLED_BACK);

            verify(idempotentRepository).markFailed(
                    eq("KEY-SURROUNDING-ROLLBACK"),
                    eq("orders-group"),
                    eq("Surrounding transaction rolled back after completion"),
                    eq(lease)
            );
        } finally {
            org.springframework.transaction.support.TransactionSynchronizationManager.clearSynchronization();
        }
    }

    @Test
    @DisplayName("Senior Refinements Round 64: Should throw IdempotencyException when lease is stolen before completion")
    void shouldThrowIdempotencyExceptionWhenLeaseIsStolenBeforeCompletion() throws Throwable {
        Method method = TestService.class.getMethod("handleString", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getArgs()).thenReturn(new Object[]{"KEY-STOLEN"});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0");
        when(idempotent.consumerGroup()).thenReturn("orders-group");
        when(idempotent.ttl()).thenReturn("1m");
        when(idempotent.lockTimeout()).thenReturn("2m");

        when(idempotentRepository.tryAcquire(eq("KEY-STOLEN"), eq("orders-group"), any(), any()))
                .thenReturn(AcquireResult.ACQUIRED);
        when(joinPoint.proceed()).thenReturn("ok");

        // markCompleted returns false indicating lock was overtaken/stolen
        when(idempotentRepository.markCompleted(eq("KEY-STOLEN"), eq("orders-group"), any(), any()))
                .thenReturn(false);

        assertThatThrownBy(() -> aspect.enforceIdempotency(joinPoint, idempotent))
                .isInstanceOf(IdempotencyException.class)
                .hasMessageContaining("Lease was stolen or lock expired before completion");
    }

    @Test
    @DisplayName("Senior Refinements Round 64: Should throw IdempotencyException when markCompleted fails all retry attempts")
    void shouldThrowIdempotencyExceptionWhenMarkCompletedFailsAllRetries() throws Throwable {
        Method method = TestService.class.getMethod("handleString", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getArgs()).thenReturn(new Object[]{"KEY-RETRY-FAIL"});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0");
        when(idempotent.consumerGroup()).thenReturn("orders-group");
        when(idempotent.ttl()).thenReturn("1m");
        when(idempotent.lockTimeout()).thenReturn("2m");

        when(idempotentRepository.tryAcquire(eq("KEY-RETRY-FAIL"), eq("orders-group"), any(), any()))
                .thenReturn(AcquireResult.ACQUIRED);
        when(joinPoint.proceed()).thenReturn("ok");

        // markCompleted throws exception every time
        when(idempotentRepository.markCompleted(eq("KEY-RETRY-FAIL"), eq("orders-group"), any(), any()))
                .thenThrow(new org.springframework.dao.DataAccessResourceFailureException("DB connection dropped"));

        assertThatThrownBy(() -> aspect.enforceIdempotency(joinPoint, idempotent))
                .isInstanceOf(IdempotencyException.class)
                .hasMessageContaining("Failed to set COMPLETED status in idempotency store")
                .hasRootCauseInstanceOf(org.springframework.dao.DataAccessResourceFailureException.class);

        verify(idempotentRepository, times(3)).markCompleted(eq("KEY-RETRY-FAIL"), eq("orders-group"), any(), any());
    }

    @Test
    @DisplayName("Senior Refinements Round 65: Should call markTerminalFailed when exception matches terminalExceptions")
    void shouldCallMarkTerminalFailedWhenExceptionMatchesTerminalExceptions() throws Throwable {
        Method method = TestService.class.getMethod("handleString", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getArgs()).thenReturn(new Object[]{"KEY-TERM"});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0");
        when(idempotent.consumerGroup()).thenReturn("orders-group");
        when(idempotent.ttl()).thenReturn("1m");
        when(idempotent.lockTimeout()).thenReturn("2m");
        when(idempotent.terminalExceptions()).thenReturn(exceptionTypes(IllegalArgumentException.class));
        when(idempotent.retryableExceptions()).thenReturn(exceptionTypes());

        when(idempotentRepository.tryAcquire(eq("KEY-TERM"), eq("orders-group"), any(), any()))
                .thenReturn(AcquireResult.ACQUIRED);
        when(joinPoint.proceed()).thenThrow(new IllegalArgumentException("Poison pill invalid payload"));

        assertThatThrownBy(() -> aspect.enforceIdempotency(joinPoint, idempotent))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Poison pill invalid payload");

        verify(idempotentRepository).markTerminalFailed(eq("KEY-TERM"), eq("orders-group"), eq("Poison pill invalid payload"), any(), any());
        verify(idempotentRepository, never()).markFailed(any(), any(), any(), any());
    }

    @Test
    @DisplayName("Senior Refinements Round 65: Should call markTerminalFailed when exception is not in retryableExceptions whitelist")
    void shouldCallMarkTerminalFailedWhenExceptionNotInRetryableWhitelist() throws Throwable {
        Method method = TestService.class.getMethod("handleString", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getArgs()).thenReturn(new Object[]{"KEY-NON-RETRYABLE"});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0");
        when(idempotent.consumerGroup()).thenReturn("orders-group");
        when(idempotent.ttl()).thenReturn("1m");
        when(idempotent.lockTimeout()).thenReturn("2m");
        when(idempotent.terminalExceptions()).thenReturn(exceptionTypes());
        when(idempotent.retryableExceptions()).thenReturn(exceptionTypes(IllegalStateException.class));

        when(idempotentRepository.tryAcquire(eq("KEY-NON-RETRYABLE"), eq("orders-group"), any(), any()))
                .thenReturn(AcquireResult.ACQUIRED);
        when(joinPoint.proceed()).thenThrow(new IllegalArgumentException("Non-whitelisted exception"));

        assertThatThrownBy(() -> aspect.enforceIdempotency(joinPoint, idempotent))
                .isInstanceOf(IllegalArgumentException.class);

        verify(idempotentRepository).markTerminalFailed(eq("KEY-NON-RETRYABLE"), eq("orders-group"), eq("Non-whitelisted exception"), any(), any());
        verify(idempotentRepository, never()).markFailed(any(), any(), any(), any());
    }

    @Test
    @DisplayName("Senior Refinements Round 65: Should call markFailed when exception matches retryableExceptions")
    void shouldCallMarkFailedWhenExceptionMatchesRetryableExceptions() throws Throwable {
        Method method = TestService.class.getMethod("handleString", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getArgs()).thenReturn(new Object[]{"KEY-RETRY"});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0");
        when(idempotent.consumerGroup()).thenReturn("orders-group");
        when(idempotent.ttl()).thenReturn("1m");
        when(idempotent.lockTimeout()).thenReturn("2m");
        when(idempotent.terminalExceptions()).thenReturn(exceptionTypes());
        when(idempotent.retryableExceptions()).thenReturn(exceptionTypes(IllegalStateException.class));

        when(idempotentRepository.tryAcquire(eq("KEY-RETRY"), eq("orders-group"), any(), any()))
                .thenReturn(AcquireResult.ACQUIRED);
        when(joinPoint.proceed()).thenThrow(new IllegalStateException("Transient lock failure"));

        assertThatThrownBy(() -> aspect.enforceIdempotency(joinPoint, idempotent))
                .isInstanceOf(IllegalStateException.class);

        verify(idempotentRepository).markFailed(eq("KEY-RETRY"), eq("orders-group"), eq("Transient lock failure"), any());
        verify(idempotentRepository, never()).markTerminalFailed(any(), any(), any(), any(), any());
    }

    @Test
    @DisplayName("Senior Refinements Round 65: CompletionStage returned future only completes after markCompleted finishes")
    void shouldCompleteReturnedFutureOnlyAfterMarkCompletedFinishes() throws Throwable {
        Method method = TestService.class.getMethod("asyncCompute", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getArgs()).thenReturn(new Object[]{"KEY-ASYNC"});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0");
        when(idempotent.consumerGroup()).thenReturn("orders-group");
        when(idempotent.ttl()).thenReturn("1m");
        when(idempotent.lockTimeout()).thenReturn("2m");

        when(idempotentRepository.tryAcquire(eq("KEY-ASYNC"), eq("orders-group"), any(), any()))
                .thenReturn(AcquireResult.ACQUIRED);

        CompletableFuture<String> stageFuture = new CompletableFuture<>();
        when(joinPoint.proceed()).thenReturn(stageFuture);

        Object result = aspect.enforceIdempotency(joinPoint, idempotent);
        assertThat(result).isInstanceOf(CompletableFuture.class);
        @SuppressWarnings("unchecked")
        CompletableFuture<String> returnedFuture = (CompletableFuture<String>) result;

        // Stage has not completed yet, returnedFuture must not be done
        assertThat(returnedFuture.isDone()).isFalse();
        verify(idempotentRepository, never()).markCompleted(any(), any(), any(), any());

        // Now complete the stage
        stageFuture.complete("async-result");

        // Returned future must now be completed and result matched
        assertThat(returnedFuture.isDone()).isTrue();
        assertThat(returnedFuture.join()).isEqualTo("async-result");
        verify(idempotentRepository).markCompleted(eq("KEY-ASYNC"), eq("orders-group"), any(), any());
    }

    @Test
    @DisplayName("Senior Refinements Round 65: CompletionStage returned future propagates failure when stage fails")
    void shouldCompleteExceptionallyWhenAsyncStageFails() throws Throwable {
        Method method = TestService.class.getMethod("asyncCompute", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getArgs()).thenReturn(new Object[]{"KEY-ASYNC-FAIL"});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0");
        when(idempotent.consumerGroup()).thenReturn("orders-group");
        when(idempotent.ttl()).thenReturn("1m");
        when(idempotent.lockTimeout()).thenReturn("2m");

        when(idempotentRepository.tryAcquire(eq("KEY-ASYNC-FAIL"), eq("orders-group"), any(), any()))
                .thenReturn(AcquireResult.ACQUIRED);

        CompletableFuture<String> stageFuture = new CompletableFuture<>();
        when(joinPoint.proceed()).thenReturn(stageFuture);

        Object result = aspect.enforceIdempotency(joinPoint, idempotent);
        assertThat(result).isInstanceOf(CompletableFuture.class);
        @SuppressWarnings("unchecked")
        CompletableFuture<String> returnedFuture = (CompletableFuture<String>) result;

        // Stage fails
        stageFuture.completeExceptionally(new RuntimeException("Async compute error"));

        assertThat(returnedFuture.isCompletedExceptionally()).isTrue();
        assertThatThrownBy(returnedFuture::join)
                .hasCauseInstanceOf(RuntimeException.class)
                .hasRootCauseMessage("Async compute error");
        verify(idempotentRepository).markFailed(eq("KEY-ASYNC-FAIL"), eq("orders-group"), eq("Async compute error"), any());
    }

    @Test
    @DisplayName("Senior Refinements Round 65 & Round 72: Should recognize legitimate UUID consumer group from Spring Message KafkaHeaders.GROUP_ID")
    void shouldRecognizeLegitimateUuidConsumerGroup() throws Throwable {
        Method method = TestService.class.getMethod("handleString", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);

        String uuidGroup = "4a2f8c12-3b4e-4c7a-9f12-0a1b2c3d4e5f";
        org.springframework.messaging.Message<String> springMsg = org.springframework.messaging.support.MessageBuilder
                .withPayload("payload")
                .setHeader(KafkaHeaders.GROUP_ID, uuidGroup)
                .build();

        when(joinPoint.getArgs()).thenReturn(new Object[]{springMsg});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0.payload");
        when(idempotent.consumerGroup()).thenReturn(""); // empty -> fallback to KafkaHeaders.GROUP_ID
        when(idempotent.ttl()).thenReturn("1m");
        when(idempotent.lockTimeout()).thenReturn("2m");

        when(idempotentRepository.tryAcquire(eq("payload"), eq(uuidGroup), any(), any()))
                .thenReturn(AcquireResult.ACQUIRED);
        when(idempotentRepository.markCompleted(eq("payload"), eq(uuidGroup), any(), any()))
                .thenReturn(true);
        when(joinPoint.proceed()).thenReturn("ok");

        aspect.enforceIdempotency(joinPoint, idempotent);

        verify(idempotentRepository).tryAcquire(eq("payload"), eq(uuidGroup), any(), any());
        verify(idempotentRepository).markCompleted(eq("payload"), eq(uuidGroup), any(), any());
    }

    @Test
    @DisplayName("Senior Refinements Round 66: Checked exception from @Transactional(NOT_SUPPORTED) calls markFailed and NOT markCompleted")
    void shouldNotMarkCompletedWhenCheckedExceptionThrownFromSpringTransactionalWithNotSupported() throws Throwable {
        Method method = TestService.class.getMethod("transactionalMethodWithNotSupported", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getArgs()).thenReturn(new Object[]{"TX-NOT-SUP"});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0");
        when(idempotent.consumerGroup()).thenReturn("orders-group");
        when(idempotent.ttl()).thenReturn("7d");
        when(idempotent.lockTimeout()).thenReturn("2m");

        when(idempotentRepository.tryAcquire(any(), any(), any(), any()))
                .thenReturn(AcquireResult.ACQUIRED);
        when(joinPoint.proceed()).thenThrow(new Exception("checked error in not supported tx"));

        assertThatThrownBy(() -> aspect.enforceIdempotency(joinPoint, idempotent))
                .isInstanceOf(Exception.class)
                .hasMessage("checked error in not supported tx");

        verify(idempotentRepository, never()).markCompleted(any(), any(), any(), any());
        verify(idempotentRepository).markFailed(eq("TX-NOT-SUP"), eq("orders-group"), eq("checked error in not supported tx"), any());
    }

    @Test
    @DisplayName("Senior Refinements Round 66: Checked exception from @Transactional(NEVER) calls markFailed and NOT markCompleted")
    void shouldNotMarkCompletedWhenCheckedExceptionThrownFromSpringTransactionalWithNever() throws Throwable {
        Method method = TestService.class.getMethod("transactionalMethodWithNever", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getArgs()).thenReturn(new Object[]{"TX-NEVER"});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0");
        when(idempotent.consumerGroup()).thenReturn("orders-group");
        when(idempotent.ttl()).thenReturn("7d");
        when(idempotent.lockTimeout()).thenReturn("2m");

        when(idempotentRepository.tryAcquire(any(), any(), any(), any()))
                .thenReturn(AcquireResult.ACQUIRED);
        when(joinPoint.proceed()).thenThrow(new Exception("checked error in never tx"));

        assertThatThrownBy(() -> aspect.enforceIdempotency(joinPoint, idempotent))
                .isInstanceOf(Exception.class)
                .hasMessage("checked error in never tx");

        verify(idempotentRepository, never()).markCompleted(any(), any(), any(), any());
        verify(idempotentRepository).markFailed(eq("TX-NEVER"), eq("orders-group"), eq("checked error in never tx"), any());
    }

    @Test
    @DisplayName("Senior Refinements Round 66: Checked exception from Jakarta @Transactional(NOT_SUPPORTED) calls markFailed and NOT markCompleted")
    void shouldNotMarkCompletedWhenCheckedExceptionThrownFromJakartaTransactionalWithNotSupported() throws Throwable {
        Method method = TestService.class.getMethod("jakartaTransactionalMethodWithNotSupported", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getArgs()).thenReturn(new Object[]{"JAKARTA-NOT-SUP"});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0");
        when(idempotent.consumerGroup()).thenReturn("orders-group");
        when(idempotent.ttl()).thenReturn("7d");
        when(idempotent.lockTimeout()).thenReturn("2m");

        when(idempotentRepository.tryAcquire(any(), any(), any(), any()))
                .thenReturn(AcquireResult.ACQUIRED);
        when(joinPoint.proceed()).thenThrow(new Exception("checked error in JTA not supported tx"));

        assertThatThrownBy(() -> aspect.enforceIdempotency(joinPoint, idempotent))
                .isInstanceOf(Exception.class)
                .hasMessage("checked error in JTA not supported tx");

        verify(idempotentRepository, never()).markCompleted(any(), any(), any(), any());
        verify(idempotentRepository).markFailed(eq("JAKARTA-NOT-SUP"), eq("orders-group"), eq("checked error in JTA not supported tx"), any());
    }

    @Test
    @DisplayName("Senior Refinements Round 66: Checked exception from Jakarta @Transactional(NEVER) calls markFailed and NOT markCompleted")
    void shouldNotMarkCompletedWhenCheckedExceptionThrownFromJakartaTransactionalWithNever() throws Throwable {
        Method method = TestService.class.getMethod("jakartaTransactionalMethodWithNever", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getArgs()).thenReturn(new Object[]{"JAKARTA-NEVER"});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0");
        when(idempotent.consumerGroup()).thenReturn("orders-group");
        when(idempotent.ttl()).thenReturn("7d");
        when(idempotent.lockTimeout()).thenReturn("2m");

        when(idempotentRepository.tryAcquire(any(), any(), any(), any()))
                .thenReturn(AcquireResult.ACQUIRED);
        when(joinPoint.proceed()).thenThrow(new Exception("checked error in JTA never tx"));

        assertThatThrownBy(() -> aspect.enforceIdempotency(joinPoint, idempotent))
                .isInstanceOf(Exception.class)
                .hasMessage("checked error in JTA never tx");

        verify(idempotentRepository, never()).markCompleted(any(), any(), any(), any());
        verify(idempotentRepository).markFailed(eq("JAKARTA-NEVER"), eq("orders-group"), eq("checked error in JTA never tx"), any());
    }

    @Test
    @DisplayName("Senior Refinements Round 66: Terminal checked exception from @Transactional method marks FAILED and NOT COMPLETED")
    void shouldMarkTerminalFailedWhenTerminalCheckedExceptionThrownFromTransactionalMethod() throws Throwable {
        Method method = TestService.class.getMethod("transactionalMethodWithTerminalCheckedException", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getArgs()).thenReturn(new Object[]{"TX-TERM-CHECKED"});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0");
        when(idempotent.consumerGroup()).thenReturn("orders-group");
        when(idempotent.ttl()).thenReturn("7d");
        when(idempotent.terminalExceptions()).thenReturn(exceptionTypes(CustomTerminalCheckedException.class));

        when(idempotentRepository.tryAcquire(any(), any(), any(), any()))
                .thenReturn(AcquireResult.ACQUIRED);
        when(joinPoint.proceed()).thenThrow(new CustomTerminalCheckedException("terminal checked error"));

        assertThatThrownBy(() -> aspect.enforceIdempotency(joinPoint, idempotent))
                .isInstanceOf(CustomTerminalCheckedException.class)
                .hasMessage("terminal checked error");

        // Must NOT mark completed even though method has @Transactional and exception is checked!
        verify(idempotentRepository, never()).markCompleted(any(), any(), any(), any());
        verify(idempotentRepository).markTerminalFailed(eq("TX-TERM-CHECKED"), eq("orders-group"), eq("terminal checked error"), eq(Duration.ofDays(7)), any());
    }

    @Test
    @DisplayName("Senior Refinements Round 66: Interface in terminalExceptions is properly recognized using BFS")
    void shouldMatchInterfaceInTerminalExceptionsWithExactBFS() {
        org.mockito.Mockito.doReturn(new Class<?>[]{BusinessMarkerInterface.class}).when(idempotent).terminalExceptions();
        ExceptionImplementingMarkerInterface ex = new ExceptionImplementingMarkerInterface("marker error");

        assertThat(aspect.isTerminalException(idempotent, ex)).isTrue();
    }

    @Test
    @DisplayName("Senior Refinements Round 67: @KafkaListener with single-message List payload is NOT blocked as batch")
    void shouldAllowSingleKafkaMessageWithListPayload() throws Throwable {
        Method method = TestService.class.getMethod("handleSingleMessageWithList", java.util.List.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getArgs()).thenReturn(new Object[]{java.util.List.of("itemA", "itemB")});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0[0]");
        when(idempotent.consumerGroup()).thenReturn("orders-group");
        when(idempotent.ttl()).thenReturn("7d");
        when(idempotent.lockTimeout()).thenReturn("2m");

        when(idempotentRepository.tryAcquire(any(), any(), any(), any()))
                .thenReturn(AcquireResult.ACQUIRED);
        when(joinPoint.proceed()).thenReturn(null);

        aspect.enforceIdempotency(joinPoint, idempotent);

        verify(joinPoint).proceed();
        verify(idempotentRepository).tryAcquire(eq("itemA"), eq("orders-group"), any(), any());
        verify(idempotentRepository).markCompleted(eq("itemA"), eq("orders-group"), any(), any());
    }

    @Test
    @DisplayName("Senior Refinements Round 67: @KafkaListener with single-message String[] payload is NOT blocked as batch")
    void shouldAllowSingleKafkaMessageWithArrayPayload() throws Throwable {
        Method method = TestService.class.getMethod("handleSingleMessageWithArray", String[].class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getArgs()).thenReturn(new Object[]{new String[]{"itemA", "itemB"}});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0[0]");
        when(idempotent.consumerGroup()).thenReturn("orders-group");
        when(idempotent.ttl()).thenReturn("7d");
        when(idempotent.lockTimeout()).thenReturn("2m");

        when(idempotentRepository.tryAcquire(any(), any(), any(), any()))
                .thenReturn(AcquireResult.ACQUIRED);
        when(joinPoint.proceed()).thenReturn(null);

        aspect.enforceIdempotency(joinPoint, idempotent);

        verify(joinPoint).proceed();
        verify(idempotentRepository).tryAcquire(eq("itemA"), eq("orders-group"), any(), any());
        verify(idempotentRepository).markCompleted(eq("itemA"), eq("orders-group"), any(), any());
    }

    @Test
    @DisplayName("Senior Refinements Round 67: @KafkaListener with List<ConsumerRecord> IS rejected as batch listener")
    void shouldRejectKafkaListenerWithBatchRecordsGeneric() throws Throwable {
        Method method = TestService.class.getMethod("handleBatchRecordsGeneric", java.util.List.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        ConsumerRecord<String, String> cr = new ConsumerRecord<>("topic", 0, 0L, "k", "v");
        when(joinPoint.getArgs()).thenReturn(new Object[]{java.util.List.of(cr)});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        assertThatThrownBy(() -> aspect.enforceIdempotency(joinPoint, idempotent))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("@Idempotent cannot be applied to batch listener method");
    }

    @Test
    @DisplayName("Senior Refinements Round 69: getDefaultReturnValue returns empty collection for Collection and Iterable, and empty array for arrays")
    void shouldReturnEmptyContainersForCollectionIterableAndArray() {
        Object colResult = aspect.getDefaultReturnValue(java.util.Collection.class);
        assertThat(colResult).isInstanceOf(java.util.Collection.class);
        assertThat((java.util.Collection<?>) colResult).isEmpty();

        Object iterableResult = aspect.getDefaultReturnValue(Iterable.class);
        assertThat(iterableResult).isInstanceOf(Iterable.class);
        assertThat((Iterable<?>) iterableResult).isEmpty();

        Object stringArrayResult = aspect.getDefaultReturnValue(String[].class);
        assertThat(stringArrayResult).isInstanceOf(String[].class);
        assertThat((String[]) stringArrayResult).isEmpty();

        Object intArrayResult = aspect.getDefaultReturnValue(int[].class);
        assertThat(intArrayResult).isInstanceOf(int[].class);
        assertThat((int[]) intArrayResult).isEmpty();
    }

    @Test
    @DisplayName("Senior Refinements Round 69: inFlightWaitTimeout clamped to MAX_IN_FLIGHT_WAIT_TIMEOUT")
    void shouldClampInFlightWaitTimeoutToMaximum() {
        properties.getIdempotency().setInFlightWaitTimeout(Duration.ofSeconds(60));
        assertThat(properties.getIdempotency().getInFlightWaitTimeout())
                .isEqualTo(IdemboxProperties.IdempotencyProperties.MAX_IN_FLIGHT_WAIT_TIMEOUT);
    }

    @Test
    @DisplayName("Senior Refinements Round 70: doHandleSuccess does not retry markCompleted when inside an active transaction")
    void shouldNotRetryMarkCompletedWhenInsideActiveTransaction() throws Throwable {
        Method method = TestService.class.getMethod("handleString", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getArgs()).thenReturn(new Object[]{"KEY-ACTIVE-TX-FAIL"});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0");
        when(idempotent.consumerGroup()).thenReturn("orders-group");
        when(idempotent.ttl()).thenReturn("1m");
        when(idempotent.lockTimeout()).thenReturn("2m");

        when(idempotentRepository.tryAcquire(eq("KEY-ACTIVE-TX-FAIL"), eq("orders-group"), any(), any()))
                .thenReturn(AcquireResult.ACQUIRED);
        when(joinPoint.proceed()).thenReturn("ok");

        when(idempotentRepository.markCompleted(eq("KEY-ACTIVE-TX-FAIL"), eq("orders-group"), any(), any()))
                .thenThrow(new org.springframework.dao.DataIntegrityViolationException("Statement failed in active tx"));

        org.springframework.transaction.support.TransactionSynchronizationManager.setActualTransactionActive(true);
        try {
            assertThatThrownBy(() -> aspect.enforceIdempotency(joinPoint, idempotent))
                    .isInstanceOf(IdempotencyException.class)
                    .hasMessageContaining("inside active/aborted transaction");

            // Must fail immediately after 1 attempt without futile retries on poisoned transaction
            verify(idempotentRepository, times(1)).markCompleted(eq("KEY-ACTIVE-TX-FAIL"), eq("orders-group"), any(), any());
        } finally {
            org.springframework.transaction.support.TransactionSynchronizationManager.setActualTransactionActive(false);
        }
    }

    @Test
    @DisplayName("Senior Refinements Round 70: doHandleSuccess does not retry markCompleted on PostgreSQL 25P02 aborted transaction error")
    void shouldNotRetryMarkCompletedOnPostgres25P02Error() throws Throwable {
        Method method = TestService.class.getMethod("handleString", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getArgs()).thenReturn(new Object[]{"KEY-25P02-FAIL"});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0");
        when(idempotent.consumerGroup()).thenReturn("orders-group");
        when(idempotent.ttl()).thenReturn("1m");
        when(idempotent.lockTimeout()).thenReturn("2m");

        when(idempotentRepository.tryAcquire(eq("KEY-25P02-FAIL"), eq("orders-group"), any(), any()))
                .thenReturn(AcquireResult.ACQUIRED);
        when(joinPoint.proceed()).thenReturn("ok");

        java.sql.SQLException pgAborted = new java.sql.SQLException(
                "ERROR: current transaction is aborted, commands ignored until end of transaction block", "25P02");
        when(idempotentRepository.markCompleted(eq("KEY-25P02-FAIL"), eq("orders-group"), any(), any()))
                .thenThrow(new org.springframework.dao.UncategorizedDataAccessException("SQL error", pgAborted) {});

        assertThatThrownBy(() -> aspect.enforceIdempotency(joinPoint, idempotent))
                .isInstanceOf(IdempotencyException.class)
                .hasMessageContaining("inside active/aborted transaction");

        verify(idempotentRepository, times(1)).markCompleted(eq("KEY-25P02-FAIL"), eq("orders-group"), any(), any());
    }

    static class SubstringNoRollbackService {
        @org.springframework.transaction.annotation.Transactional(noRollbackForClassName = {"IdempotentConsumerAspectTest.CustomCheckedException"})
        public void processWithNestedDotName(String id) throws CustomCheckedException {
            java.util.Objects.requireNonNull(id);
            throw new CustomCheckedException();
        }
    }

    @Test
    @DisplayName("Senior Refinements Round 71: noRollbackForClassName with nested dot-notation matches and marks COMPLETED")
    void shouldMatchNestedDotNameInNoRollbackForClassNameAndMarkCompleted() throws Throwable {
        org.springframework.transaction.support.TransactionSynchronizationManager.setActualTransactionActive(true);
        Method method = SubstringNoRollbackService.class.getMethod("processWithNestedDotName", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getArgs()).thenReturn(new Object[]{"KEY-SUBSTRING-1"});
        when(joinPoint.getTarget()).thenReturn(new SubstringNoRollbackService());

        when(idempotent.key()).thenReturn("#p0");
        when(idempotent.consumerGroup()).thenReturn("orders-group");
        when(idempotent.ttl()).thenReturn("7d");
        when(idempotent.lockTimeout()).thenReturn("2m");

        when(idempotentRepository.tryAcquire(eq("KEY-SUBSTRING-1"), eq("orders-group"), any(), any()))
                .thenReturn(AcquireResult.ACQUIRED);
        when(joinPoint.proceed()).thenThrow(new CustomCheckedException());

        assertThatThrownBy(() -> aspect.enforceIdempotency(joinPoint, idempotent))
                .isInstanceOf(CustomCheckedException.class);

        verify(idempotentRepository).markCompleted(eq("KEY-SUBSTRING-1"), eq("orders-group"), eq(Duration.ofDays(7)), org.mockito.ArgumentMatchers.nullable(Instant.class));
        verify(idempotentRepository, never()).markFailed(any(), any(), any(), any());
    }

    @Test
    @DisplayName("Senior Refinements Round 71: Falls back to global default-ttl and default-lock-timeout when @Idempotent attributes are blank")
    void shouldFallbackToGlobalDefaultTtlAndLockTimeoutWhenAnnotationAttributesAreBlank() throws Throwable {
        properties.getIdempotency().setDefaultTtl("14d");
        properties.getIdempotency().setDefaultLockTimeout("5m");

        Method method = TestService.class.getMethod("handleString", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getArgs()).thenReturn(new Object[]{"KEY-GLOBAL-DEFAULTS"});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0");
        when(idempotent.consumerGroup()).thenReturn("orders-group");
        when(idempotent.ttl()).thenReturn("");
        when(idempotent.lockTimeout()).thenReturn("");

        when(idempotentRepository.tryAcquire(eq("KEY-GLOBAL-DEFAULTS"), eq("orders-group"), eq(Duration.ofMinutes(5)), eq(Duration.ofDays(14))))
                .thenReturn(AcquireResult.ACQUIRED);
        when(joinPoint.proceed()).thenReturn("ok");

        Object result = aspect.enforceIdempotency(joinPoint, idempotent);
        assertThat(result).isEqualTo("ok");

        verify(idempotentRepository).tryAcquire(eq("KEY-GLOBAL-DEFAULTS"), eq("orders-group"), eq(Duration.ofMinutes(5)), eq(Duration.ofDays(14)));
        verify(idempotentRepository).markCompleted(eq("KEY-GLOBAL-DEFAULTS"), eq("orders-group"), eq(Duration.ofDays(14)), org.mockito.ArgumentMatchers.nullable(Instant.class));
    }

    @Test
    @DisplayName("Senior Refinements Round 72: SpEL evaluation failure on non-tombstone ConsumerRecord rethrows exception instead of falling back to partition key")
    void shouldNotFallbackToConsumerRecordKeyWhenPayloadIsNonNull() throws Throwable {
        Method method = TestService.class.getMethod("handleWithRecord", String.class, ConsumerRecord.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);

        ConsumerRecord<String, String> record =
                new ConsumerRecord<>("orders-topic", 0, 100L, "CUSTOMER-PARTITION-KEY-42", "{\"orderId\":null}");
        when(joinPoint.getArgs()).thenReturn(new Object[]{"{\"orderId\":null}", record});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        // Typo in SpEL property name on non-null record value
        when(idempotent.key()).thenReturn("#p1.value().nonExistentOrderField");

        assertThatThrownBy(() -> aspect.enforceIdempotency(joinPoint, idempotent))
                .isInstanceOf(org.springframework.expression.EvaluationException.class);

        verify(idempotentRepository, never()).tryAcquireWithLease(any(), any(), any(), any());
        verify(idempotentRepository, never()).tryAcquire(any(), any(), any(), any());
        verify(idempotentRepository, never()).markCompleted(any(), any(), any(), any());
    }

    @Test
    @DisplayName("Senior Refinements Round 72: Untrusted kafka_groupId and X-Consumer-Group network headers on ConsumerRecord and Message are ignored")
    void shouldIgnoreUntrustedNetworkConsumerGroupHeaders() throws Throwable {
        Method method = TestService.class.getMethod("handleWithRecord", String.class, ConsumerRecord.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);

        ConsumerRecord<String, String> cr = new ConsumerRecord<>("topic", 0, 0, "k", "ORD-UNTRUSTED-HDR");
        cr.headers().add("kafka_groupId", "spoofed-victim-group".getBytes(StandardCharsets.UTF_8));
        cr.headers().add("X-Consumer-Group", "spoofed-bypass-group".getBytes(StandardCharsets.UTF_8));

        when(joinPoint.getArgs()).thenReturn(new Object[]{"ORD-UNTRUSTED-HDR", cr});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0");
        when(idempotent.consumerGroup()).thenReturn("");
        when(idempotent.ttl()).thenReturn("7d");
        when(idempotent.lockTimeout()).thenReturn("2m");

        String expectedFallbackGroup = TestService.class.getName() + "#handleWithRecord";
        when(idempotentRepository.tryAcquire(eq("ORD-UNTRUSTED-HDR"), eq(expectedFallbackGroup), any(), any()))
                .thenReturn(AcquireResult.ACQUIRED);
        when(idempotentRepository.markCompleted(eq("ORD-UNTRUSTED-HDR"), eq(expectedFallbackGroup), any(), any()))
                .thenReturn(true);
        when(joinPoint.proceed()).thenReturn("ok");

        aspect.enforceIdempotency(joinPoint, idempotent);

        verify(idempotentRepository).tryAcquire(eq("ORD-UNTRUSTED-HDR"), eq(expectedFallbackGroup), any(), any());
        verify(idempotentRepository, never()).tryAcquire(any(), eq("spoofed-victim-group"), any(), any());
        verify(idempotentRepository, never()).tryAcquire(any(), eq("spoofed-bypass-group"), any(), any());
    }

    @Test
    @DisplayName("Senior Refinements Round 72: Synchronous method on event-loop thread executes handleSuccess synchronously and propagates IdempotencyException on stolen lease")
    void shouldPropagateIdempotencyExceptionSynchronouslyOnEventLoopThreadWhenLeaseStolen() throws Throwable {
        Method method = TestService.class.getMethod("handleString", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getArgs()).thenReturn(new Object[]{"KEY-SYNC-EVENT-LOOP-STOLEN"});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0");
        when(idempotent.consumerGroup()).thenReturn("orders-group");
        when(idempotent.ttl()).thenReturn("7d");
        when(idempotent.lockTimeout()).thenReturn("2m");

        Instant lease = Instant.now().plusSeconds(120).truncatedTo(java.time.temporal.ChronoUnit.MICROS);
        when(idempotentRepository.tryAcquireWithLease(eq("KEY-SYNC-EVENT-LOOP-STOLEN"), eq("orders-group"), any(), any()))
                .thenReturn(new io.github.alexlitvinov.idembox.core.model.AcquireOutcome(AcquireResult.ACQUIRED, lease));
        when(joinPoint.proceed()).thenReturn("success");
        // Simulate stolen lease: markCompleted returns false
        when(idempotentRepository.markCompleted(eq("KEY-SYNC-EVENT-LOOP-STOLEN"), eq("orders-group"), any(), eq(lease)))
                .thenReturn(false);

        java.util.concurrent.atomic.AtomicReference<Throwable> threadEx = new java.util.concurrent.atomic.AtomicReference<>();
        Thread eventLoopThread = new Thread(() -> {
            try {
                aspect.enforceIdempotency(joinPoint, idempotent);
            } catch (Throwable e) {
                threadEx.set(e);
            }
        }, "reactor-http-epoll-4");

        eventLoopThread.start();
        eventLoopThread.join();

        assertThat(threadEx.get())
                .isInstanceOf(IdempotencyException.class)
                .hasMessageContaining("Lease was stolen or lock expired before completion");
        verify(idempotentRepository).markCompleted(eq("KEY-SYNC-EVENT-LOOP-STOLEN"), eq("orders-group"), any(), eq(lease));
    }

    static class SupportsTxService {
        @org.springframework.transaction.annotation.Transactional(propagation = org.springframework.transaction.annotation.Propagation.SUPPORTS)
        public void processWithSupports(String id) throws CustomCheckedException {
            java.util.Objects.requireNonNull(id);
            throw new CustomCheckedException();
        }

        @org.springframework.transaction.annotation.Transactional(propagation = org.springframework.transaction.annotation.Propagation.SUPPORTS)
        public CompletableFuture<String> asyncWithSupports(String id) {
            return CompletableFuture.completedFuture(id);
        }
    }

    @Test
    @DisplayName("Senior Refinements Round 75: @Transactional(Propagation.SUPPORTS) without active transaction calls markFailed instead of markCompleted on checked exception")
    void shouldCallMarkFailedWhenSupportsPropagationCalledWithoutActiveTransaction() throws Throwable {
        Method method = SupportsTxService.class.getMethod("processWithSupports", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getArgs()).thenReturn(new Object[]{"KEY-SUPPORTS-NO-TX"});
        when(joinPoint.getTarget()).thenReturn(new SupportsTxService());

        when(idempotent.key()).thenReturn("#p0");
        when(idempotent.consumerGroup()).thenReturn("orders-group");
        when(idempotent.ttl()).thenReturn("7d");
        when(idempotent.lockTimeout()).thenReturn("2m");

        when(idempotentRepository.tryAcquire(eq("KEY-SUPPORTS-NO-TX"), eq("orders-group"), any(), any()))
                .thenReturn(AcquireResult.ACQUIRED);
        when(joinPoint.proceed()).thenThrow(new CustomCheckedException());

        org.springframework.transaction.support.TransactionSynchronizationManager.setActualTransactionActive(false);
        assertThatThrownBy(() -> aspect.enforceIdempotency(joinPoint, idempotent))
                .isInstanceOf(CustomCheckedException.class);

        verify(idempotentRepository, never()).markCompleted(any(), any(), any(), any());
        verify(idempotentRepository).markFailed(eq("KEY-SUPPORTS-NO-TX"), eq("orders-group"), any(), any());
    }

    @Test
    @DisplayName("Senior Refinements Round 75 & 76: Stolen lease (markCompleted == false) on non-rollback @Transactional exception throws IdempotencyException with original exception attached as suppressed")
    void shouldThrowIdempotencyExceptionWhenLeaseStolenOnNonRollbackTransactionalException() throws Throwable {
        org.springframework.transaction.support.TransactionSynchronizationManager.setActualTransactionActive(true);
        Method method = TestService.class.getMethod("transactionalMethodWithoutRollback", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getArgs()).thenReturn(new Object[]{"KEY-NO-ROLLBACK-STOLEN"});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0");
        when(idempotent.consumerGroup()).thenReturn("orders-group");
        when(idempotent.ttl()).thenReturn("7d");
        when(idempotent.lockTimeout()).thenReturn("2m");

        Instant lease = Instant.now().plusSeconds(120).truncatedTo(java.time.temporal.ChronoUnit.MICROS);
        when(idempotentRepository.tryAcquireWithLease(eq("KEY-NO-ROLLBACK-STOLEN"), eq("orders-group"), any(), any()))
                .thenReturn(new io.github.alexlitvinov.idembox.core.model.AcquireOutcome(AcquireResult.ACQUIRED, lease));
        Exception originalCheckedException = new Exception("checked business exception that would normally commit");
        when(joinPoint.proceed()).thenThrow(originalCheckedException);
        when(idempotentRepository.markCompleted(eq("KEY-NO-ROLLBACK-STOLEN"), eq("orders-group"), any(), eq(lease)))
                .thenReturn(false);

        assertThatThrownBy(() -> aspect.enforceIdempotency(joinPoint, idempotent))
                .isInstanceOf(IdempotencyException.class)
                .hasMessageContaining("Lease was stolen or lock expired before completion")
                .satisfies(ex -> assertThat(ex.getSuppressed()).contains(originalCheckedException));
    }

    @Test
    @DisplayName("Senior Refinements Round 76: @Transactional (REQUIRED) without actual active transaction calls markFailed instead of markCompleted")
    void shouldCallMarkFailedWhenTransactionalRequiredCalledWithoutActualActiveTransaction() throws Throwable {
        org.springframework.transaction.support.TransactionSynchronizationManager.setActualTransactionActive(false);
        Method method = TestService.class.getMethod("transactionalMethodWithoutRollback", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getArgs()).thenReturn(new Object[]{"KEY-REQUIRED-NO-TX"});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0");
        when(idempotent.consumerGroup()).thenReturn("orders-group");
        when(idempotent.ttl()).thenReturn("7d");
        when(idempotent.lockTimeout()).thenReturn("2m");

        when(idempotentRepository.tryAcquire(eq("KEY-REQUIRED-NO-TX"), eq("orders-group"), any(), any()))
                .thenReturn(AcquireResult.ACQUIRED);
        when(joinPoint.proceed()).thenThrow(new Exception("checked error without actual tx"));

        assertThatThrownBy(() -> aspect.enforceIdempotency(joinPoint, idempotent))
                .isInstanceOf(Exception.class)
                .hasMessage("checked error without actual tx");

        verify(idempotentRepository, never()).markCompleted(any(), any(), any(), any());
        verify(idempotentRepository).markFailed(eq("KEY-REQUIRED-NO-TX"), eq("orders-group"), eq("checked error without actual tx"), any());
    }

    @Test
    @DisplayName("Senior Refinements Round 76: CompletionStage with @Transactional(Propagation.SUPPORTS) preserves active transaction state when failing on worker thread")
    void shouldPreserveSupportsActiveTransactionWhenCompletionStageFailsOnWorkerThread() throws Throwable {
        org.springframework.transaction.support.TransactionSynchronizationManager.setActualTransactionActive(true);
        Method method = SupportsTxService.class.getMethod("asyncWithSupports", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getArgs()).thenReturn(new Object[]{"KEY-ASYNC-SUPPORTS-TX"});
        when(joinPoint.getTarget()).thenReturn(new SupportsTxService());

        when(idempotent.key()).thenReturn("#p0");
        when(idempotent.consumerGroup()).thenReturn("orders-group");
        when(idempotent.ttl()).thenReturn("7d");
        when(idempotent.lockTimeout()).thenReturn("2m");

        when(idempotentRepository.tryAcquire(eq("KEY-ASYNC-SUPPORTS-TX"), eq("orders-group"), any(), any()))
                .thenReturn(AcquireResult.ACQUIRED);

        CompletableFuture<String> stage = new CompletableFuture<>();
        when(joinPoint.proceed()).thenReturn(stage);

        // Invoke while outer transaction is active on caller thread
        Object res = aspect.enforceIdempotency(joinPoint, idempotent);
        assertThat(res).isInstanceOf(CompletableFuture.class);

        // Simulate outer transaction already committed and stage failing on an async worker thread where isActualTransactionActive() is false
        org.springframework.transaction.support.TransactionSynchronizationManager.setActualTransactionActive(false);
        Thread worker = new Thread(() -> stage.completeExceptionally(new RuntimeException("async worker failure")));
        worker.start();
        worker.join();

        verify(idempotentRepository).markCompleted(eq("KEY-ASYNC-SUPPORTS-TX"), eq("orders-group"), any(), any());
        verify(idempotentRepository, never()).markFailed(any(), any(), any(), any());
    }

    @Test
    @DisplayName("Senior Refinements Round 76: TransactionalAnnotationUtils.getDepth ignores package names, Object.class, and Serializable.class")
    void shouldIgnorePackageSubstringsObjectAndSerializableInGetDepth() {
        // Package substrings in "io.github.alexlitvinov.idembox.starter.aop.IdempotentConsumerAspectTest$CustomCheckedException" must NOT match
        assertThat(TransactionalAnnotationUtils.getDepth(CustomCheckedException.class, "github")).isEqualTo(-1);
        assertThat(TransactionalAnnotationUtils.getDepth(CustomCheckedException.class, "alexlitvinov")).isEqualTo(-1);
        assertThat(TransactionalAnnotationUtils.getDepth(CustomCheckedException.class, "starter")).isEqualTo(-1);

        // Object and Serializable must NOT match
        assertThat(TransactionalAnnotationUtils.getDepth(CustomCheckedException.class, "Object")).isEqualTo(-1);
        assertThat(TransactionalAnnotationUtils.getDepth(CustomCheckedException.class, "java.lang.Object")).isEqualTo(-1);
        assertThat(TransactionalAnnotationUtils.getDepth(CustomCheckedException.class, "Serializable")).isEqualTo(-1);
        assertThat(TransactionalAnnotationUtils.getDepth(CustomCheckedException.class, "java.io.Serializable")).isEqualTo(-1);

        // Legitimate class name substrings and interfaces still match
        assertThat(TransactionalAnnotationUtils.getDepth(CustomCheckedException.class, "CustomChecked")).isEqualTo(0);
        assertThat(TransactionalAnnotationUtils.getDepth(CustomCheckedException.class, "ChildMarkerInterface")).isEqualTo(1);
    }

    static class NonAnnotatedAsyncService {
        public CompletableFuture<String> handleAsyncWithoutTxAnnotation(String id) {
            return CompletableFuture.completedFuture(id);
        }
    }

    @Test
    @DisplayName("Senior Refinements Round 77: Non-@Transactional @Idempotent method invoked inside active outer transaction marks COMPLETED when CompletionStage fails asynchronously")
    void shouldMarkCompletedWhenNonAnnotatedMethodInsideActiveOuterTransactionFailsAsyncStage() throws Throwable {
        org.springframework.transaction.support.TransactionSynchronizationManager.setActualTransactionActive(true);
        Method method = NonAnnotatedAsyncService.class.getMethod("handleAsyncWithoutTxAnnotation", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getArgs()).thenReturn(new Object[]{"KEY-OUTER-TX-ASYNC"});
        when(joinPoint.getTarget()).thenReturn(new NonAnnotatedAsyncService());

        when(idempotent.key()).thenReturn("#p0");
        when(idempotent.consumerGroup()).thenReturn("orders-group");
        when(idempotent.ttl()).thenReturn("7d");
        when(idempotent.lockTimeout()).thenReturn("2m");

        when(idempotentRepository.tryAcquire(eq("KEY-OUTER-TX-ASYNC"), eq("orders-group"), any(), any()))
                .thenReturn(AcquireResult.ACQUIRED);

        CompletableFuture<String> stage = new CompletableFuture<>();
        when(joinPoint.proceed()).thenReturn(stage);

        Object res = aspect.enforceIdempotency(joinPoint, idempotent);
        assertThat(res).isInstanceOf(CompletableFuture.class);

        // Outer transaction commits, then stage fails on worker thread
        org.springframework.transaction.support.TransactionSynchronizationManager.setActualTransactionActive(false);
        Thread worker = new Thread(() -> stage.completeExceptionally(new RuntimeException("async failure after outer tx commit")));
        worker.start();
        worker.join();

        verify(idempotentRepository).markCompleted(eq("KEY-OUTER-TX-ASYNC"), eq("orders-group"), any(), any());
        verify(idempotentRepository, never()).markFailed(any(), any(), any(), any());
    }

    @Test
    @DisplayName("Senior Refinements Round 77: TransactionalAnnotationUtils.getDepth matches full outer class name prefix for nested exceptions and resolveEffectiveLockedUntil does not query DB")
    void shouldMatchFullClassNameOuterClassPrefixForNestedExceptionAndNeverQueryDbForLeaseTokenOnFailure() throws Throwable {
        // 1. Outer class name prefix matches nested CustomCheckedException
        assertThat(TransactionalAnnotationUtils.getDepth(
                CustomCheckedException.class,
                IdempotentConsumerAspectTest.class.getName())).isEqualTo(0);

        // 2. When tryAcquireWithLease returns null lockedUntil (e.g. legacy custom repository),
        //    and another worker acquires a new lease during joinPoint.proceed(),
        //    doHandleFailure must NOT call getProcessingState after failure to steal Worker B's lease token!
        org.springframework.transaction.support.TransactionSynchronizationManager.setActualTransactionActive(false);
        Method method = TestService.class.getMethod("handleString", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getArgs()).thenReturn(new Object[]{"KEY-NO-DB-LEASE-STEAL"});
        when(joinPoint.getTarget()).thenReturn(new TestService());

        when(idempotent.key()).thenReturn("#p0");
        when(idempotent.consumerGroup()).thenReturn("orders-group");
        when(idempotent.ttl()).thenReturn("7d");
        when(idempotent.lockTimeout()).thenReturn("2m");

        when(idempotentRepository.tryAcquireWithLease(eq("KEY-NO-DB-LEASE-STEAL"), eq("orders-group"), any(), any()))
                .thenReturn(new io.github.alexlitvinov.idembox.core.model.AcquireOutcome(AcquireResult.ACQUIRED, null));
        when(joinPoint.proceed()).thenThrow(new RuntimeException("worker A failed"));

        assertThatThrownBy(() -> aspect.enforceIdempotency(joinPoint, idempotent))
                .isInstanceOf(RuntimeException.class);

        verify(idempotentRepository, never()).getProcessingState(eq("KEY-NO-DB-LEASE-STEAL"), eq("orders-group"));
        verify(idempotentRepository).markFailed(eq("KEY-NO-DB-LEASE-STEAL"), eq("orders-group"), eq("worker A failed"), org.mockito.ArgumentMatchers.isNull());
    }

    static class CustomParentException extends Exception {
        CustomParentException(String message) {
            super(message);
        }
    }

    static class CustomBusinessException extends CustomParentException {
        CustomBusinessException(String message) {
            super(message);
        }
    }

    static class SupportsTxTestService {
        @org.springframework.transaction.annotation.Transactional(propagation = org.springframework.transaction.annotation.Propagation.SUPPORTS)
        public void processSupports(String id) throws java.io.IOException {
            throw new java.io.IOException(id);
        }

        @org.springframework.transaction.annotation.Transactional(
                rollbackFor = CustomParentException.class,
                noRollbackForClassName = "Exception"
        )
        public void processWithParentRollbackAndBaseExceptionNoRollback(String id) throws CustomBusinessException {
            throw new CustomBusinessException(id);
        }
    }

    @Test
    @DisplayName("Senior Refinements Round 78: Checked exception in active outer transaction or SUPPORTS does not falsely mark COMPLETED, and getDepth does not invert hierarchy distance on 'Exception'")
    void shouldNotMarkCompletedOnCheckedExceptionInOuterTxAndShouldNotInvertGetDepthForExceptionRule() throws Throwable {
        // 1. Verify getDepth(CustomBusinessException.class, "Exception") returns actual depth (2), not 0!
        assertThat(TransactionalAnnotationUtils.getDepth(CustomBusinessException.class, CustomParentException.class)).isEqualTo(1);
        assertThat(TransactionalAnnotationUtils.getDepth(CustomBusinessException.class, "Exception")).isEqualTo(2);

        Method ruleMethod = SupportsTxTestService.class.getMethod("processWithParentRollbackAndBaseExceptionNoRollback", String.class);
        org.springframework.transaction.annotation.Transactional ruleTxAnn =
                org.springframework.core.annotation.AnnotatedElementUtils.findMergedAnnotation(ruleMethod, org.springframework.transaction.annotation.Transactional.class);
        // CustomParentException (depth 1) must win over "Exception" (depth 2) -> rollback is true!
        assertThat(TransactionalAnnotationUtils.isSpringRollbackFor(ruleTxAnn, new CustomBusinessException("boom"))).isTrue();

        // 2. Unannotated @Idempotent method running inside active outer transaction throwing IOException -> must markFailed, NEVER markCompleted
        try {
            org.springframework.transaction.support.TransactionSynchronizationManager.setActualTransactionActive(true);
            Method unannotatedMethod = TestService.class.getMethod("handleString", String.class);
            when(joinPoint.getSignature()).thenReturn(methodSignature);
            when(methodSignature.getMethod()).thenReturn(unannotatedMethod);
            when(joinPoint.getArgs()).thenReturn(new Object[]{"KEY-OUTER-CHECKED"});
            when(joinPoint.getTarget()).thenReturn(new TestService());

            when(idempotent.key()).thenReturn("#p0");
            when(idempotent.consumerGroup()).thenReturn("orders-group");
            when(idempotent.ttl()).thenReturn("7d");
            when(idempotent.lockTimeout()).thenReturn("2m");

            Instant lease = Instant.now().plusSeconds(120).truncatedTo(java.time.temporal.ChronoUnit.MICROS);
            when(idempotentRepository.tryAcquireWithLease(eq("KEY-OUTER-CHECKED"), eq("orders-group"), any(), any()))
                    .thenReturn(new io.github.alexlitvinov.idembox.core.model.AcquireOutcome(AcquireResult.ACQUIRED, lease));
            when(joinPoint.proceed()).thenThrow(new java.io.IOException("I/O failure in outer tx"));

            assertThatThrownBy(() -> aspect.enforceIdempotency(joinPoint, idempotent))
                    .isInstanceOf(java.io.IOException.class);

            verify(idempotentRepository, never()).markCompleted(eq("KEY-OUTER-CHECKED"), any(), any(), any());
            verify(idempotentRepository).markFailed(eq("KEY-OUTER-CHECKED"), eq("orders-group"), eq("I/O failure in outer tx"), eq(lease));

            // 3. @Transactional(SUPPORTS) method running inside active outer transaction throwing IOException -> must markFailed, NEVER markCompleted
            Method supportsMethod = SupportsTxTestService.class.getMethod("processSupports", String.class);
            when(methodSignature.getMethod()).thenReturn(supportsMethod);
            when(joinPoint.getArgs()).thenReturn(new Object[]{"KEY-SUPPORTS-CHECKED"});
            when(joinPoint.getTarget()).thenReturn(new SupportsTxTestService());
            when(idempotentRepository.tryAcquireWithLease(eq("KEY-SUPPORTS-CHECKED"), eq("orders-group"), any(), any()))
                    .thenReturn(new io.github.alexlitvinov.idembox.core.model.AcquireOutcome(AcquireResult.ACQUIRED, lease));

            assertThatThrownBy(() -> aspect.enforceIdempotency(joinPoint, idempotent))
                    .isInstanceOf(java.io.IOException.class);

            verify(idempotentRepository, never()).markCompleted(eq("KEY-SUPPORTS-CHECKED"), any(), any(), any());
            verify(idempotentRepository).markFailed(eq("KEY-SUPPORTS-CHECKED"), eq("orders-group"), eq("I/O failure in outer tx"), eq(lease));
        } finally {
            org.springframework.transaction.support.TransactionSynchronizationManager.setActualTransactionActive(false);
        }
    }

    @Test
    @DisplayName("Should throw IllegalStateException when outer transaction is active and fail-fast is enabled")
    void shouldThrowIllegalStateExceptionWhenOuterTransactionIsActiveAndFailFastIsEnabled() throws Throwable {
        properties.getIdempotency().setFailOnExistingTransaction(true);
        org.springframework.transaction.support.TransactionSynchronizationManager.setActualTransactionActive(true);
        try {
            Method method = TestService.class.getMethod("handleString", String.class);
            when(joinPoint.getSignature()).thenReturn(methodSignature);
            when(methodSignature.getMethod()).thenReturn(method);
            when(joinPoint.getTarget()).thenReturn(new TestService());
            when(joinPoint.getArgs()).thenReturn(new Object[]{"msg-1"});

            when(idempotent.key()).thenReturn("#root.args[0]");
            when(idempotent.consumerGroup()).thenReturn("orders-group");

            assertThatThrownBy(() -> aspect.enforceIdempotency(joinPoint, idempotent))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Active transaction detected upon entering @Idempotent method");
            org.mockito.Mockito.verifyNoInteractions(idempotentRepository);
        } finally {
            org.springframework.transaction.support.TransactionSynchronizationManager.setActualTransactionActive(false);
        }
    }

    @Test
    @DisplayName("Should defer markFailed to afterCompletion when transaction synchronization is active")
    void shouldDeferMarkFailedToAfterCompletionWhenSynchronizationIsActive() throws Throwable {
        properties.getIdempotency().setFailOnExistingTransaction(false);
        org.springframework.transaction.support.TransactionSynchronizationManager.initSynchronization();
        try {
            Method method = TestService.class.getMethod("handleString", String.class);
            when(joinPoint.getSignature()).thenReturn(methodSignature);
            when(methodSignature.getMethod()).thenReturn(method);
            when(joinPoint.getTarget()).thenReturn(new TestService());
            when(joinPoint.getArgs()).thenReturn(new Object[]{"msg-sync"});

            when(idempotent.key()).thenReturn("#root.args[0]");
            when(idempotent.consumerGroup()).thenReturn("orders-group");
            when(idempotent.ttl()).thenReturn("7d");
            when(idempotent.lockTimeout()).thenReturn("2m");

            Instant lease = Instant.now().plusSeconds(60);
            when(idempotentRepository.tryAcquireWithLease(eq("msg-sync"), eq("orders-group"), any(), any()))
                    .thenReturn(new io.github.alexlitvinov.idembox.core.model.AcquireOutcome(AcquireResult.ACQUIRED, lease));
            when(joinPoint.proceed()).thenThrow(new RuntimeException("database timeout"));

            assertThatThrownBy(() -> aspect.enforceIdempotency(joinPoint, idempotent))
                    .isInstanceOf(RuntimeException.class)
                    .hasMessage("database timeout");

            // markFailed must NOT be called yet while transaction is active
            verify(idempotentRepository, never()).markFailed(any(), any(), any(), any());

            // After completion of surrounding transaction, markFailed must be called
            java.util.List<org.springframework.transaction.support.TransactionSynchronization> synchronizations =
                    org.springframework.transaction.support.TransactionSynchronizationManager.getSynchronizations();
            assertThat(synchronizations).isNotEmpty();
            for (org.springframework.transaction.support.TransactionSynchronization sync : synchronizations) {
                sync.afterCompletion(org.springframework.transaction.support.TransactionSynchronization.STATUS_ROLLED_BACK);
            }

            verify(idempotentRepository).markFailed(eq("msg-sync"), eq("orders-group"), eq("database timeout"), eq(lease.truncatedTo(java.time.temporal.ChronoUnit.MICROS)));
        } finally {
            org.springframework.transaction.support.TransactionSynchronizationManager.clear();
        }
    }

    @Test
    @DisplayName("Dual-Commit prevention: marks completed in beforeCommit when surrounding transaction is active")
    void shouldAtomicallyMarkCompletedInBeforeCommitWhenSurroundingTransactionIsActive() throws Throwable {
        org.springframework.transaction.support.TransactionSynchronizationManager.initSynchronization();
        try {
            Method method = TestService.class.getMethod("handleString", String.class);
            when(methodSignature.getMethod()).thenReturn(method);
            when(joinPoint.getSignature()).thenReturn(methodSignature);
            when(joinPoint.getArgs()).thenReturn(new Object[]{"msg-atomic"});
            when(joinPoint.getTarget()).thenReturn(new TestService());
            when(idempotent.key()).thenReturn("#root.args[0]");
            when(idempotent.consumerGroup()).thenReturn("orders-group");
            when(idempotent.ttl()).thenReturn("7d");
            when(idempotent.lockTimeout()).thenReturn("2m");

            Instant lease = Instant.now().plusSeconds(60);
            when(idempotentRepository.tryAcquireWithLease(eq("msg-atomic"), eq("orders-group"), any(), any()))
                    .thenReturn(new io.github.alexlitvinov.idembox.core.model.AcquireOutcome(AcquireResult.ACQUIRED, lease));
            when(idempotentRepository.markCompleted(eq("msg-atomic"), eq("orders-group"), any(), any()))
                    .thenReturn(true);
            when(joinPoint.proceed()).thenAnswer(inv -> {
                // Inside transaction execution: verify synchronization is registered
                java.util.List<org.springframework.transaction.support.TransactionSynchronization> syncs =
                        org.springframework.transaction.support.TransactionSynchronizationManager.getSynchronizations();
                assertThat(syncs).isNotEmpty();
                // Simulate Spring TransactionManager triggering beforeCommit before DB commit
                for (org.springframework.transaction.support.TransactionSynchronization sync : syncs) {
                    sync.beforeCommit(false);
                }
                return "processed";
            });

            Object result = aspect.enforceIdempotency(joinPoint, idempotent);
            assertThat(result).isEqualTo("processed");

            // markCompleted was called ONCE inside beforeCommit, and separate markCompleted in doHandleSuccess was skipped!
            verify(idempotentRepository, times(1)).markCompleted(eq("msg-atomic"), eq("orders-group"), any(), any());
        } finally {
            org.springframework.transaction.support.TransactionSynchronizationManager.clear();
        }
    }

    @Test
    @DisplayName("Dual-Commit prevention: throws IdempotencyException and rolls back transaction when lease is stolen during beforeCommit")
    void shouldThrowIdempotencyExceptionWhenLeaseIsStolenDuringBeforeCommit() throws Throwable {
        org.springframework.transaction.support.TransactionSynchronizationManager.initSynchronization();
        try {
            Method method = TestService.class.getMethod("handleString", String.class);
            when(methodSignature.getMethod()).thenReturn(method);
            when(joinPoint.getSignature()).thenReturn(methodSignature);
            when(joinPoint.getArgs()).thenReturn(new Object[]{"msg-stolen"});
            when(joinPoint.getTarget()).thenReturn(new TestService());
            when(idempotent.key()).thenReturn("#root.args[0]");
            when(idempotent.consumerGroup()).thenReturn("orders-group");
            when(idempotent.ttl()).thenReturn("7d");
            when(idempotent.lockTimeout()).thenReturn("2m");

            Instant lease = Instant.now().plusSeconds(60);
            when(idempotentRepository.tryAcquireWithLease(eq("msg-stolen"), eq("orders-group"), any(), any()))
                    .thenReturn(new io.github.alexlitvinov.idembox.core.model.AcquireOutcome(AcquireResult.ACQUIRED, lease));
            when(idempotentRepository.markCompleted(eq("msg-stolen"), eq("orders-group"), any(), any()))
                    .thenReturn(false); // Lease stolen!
            when(joinPoint.proceed()).thenAnswer(inv -> {
                java.util.List<org.springframework.transaction.support.TransactionSynchronization> syncs =
                        org.springframework.transaction.support.TransactionSynchronizationManager.getSynchronizations();
                assertThat(syncs).isNotEmpty();
                for (org.springframework.transaction.support.TransactionSynchronization sync : syncs) {
                    sync.beforeCommit(false);
                }
                return "processed";
            });

            assertThatThrownBy(() -> aspect.enforceIdempotency(joinPoint, idempotent))
                    .isInstanceOf(IdempotencyException.class)
                    .hasMessageContaining("Lease was stolen or lock expired before transaction commit");
        } finally {
            org.springframework.transaction.support.TransactionSynchronizationManager.clear();
        }
    }

    @Test
    @DisplayName("Issue 2: CURRENT_SYNC_CONTEXT is removed from ThreadLocalMap after enforceIdempotency")
    void shouldCleanUpCurrentSyncContextViaRemove() throws Throwable {
        Method method = TestService.class.getMethod("handleString", String.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);
        when(joinPoint.getArgs()).thenReturn(new Object[]{"sync-key"});
        when(joinPoint.getTarget()).thenReturn(new TestService());
        when(idempotent.key()).thenReturn("#root.args[0]");
        when(idempotent.consumerGroup()).thenReturn("orders-group");
        when(idempotent.ttl()).thenReturn("7d");
        when(idempotent.lockTimeout()).thenReturn("2m");

        when(idempotentRepository.tryAcquireWithLease(eq("sync-key"), eq("orders-group"), any(), any()))
                .thenReturn(new io.github.alexlitvinov.idembox.core.model.AcquireOutcome(AcquireResult.ACQUIRED, Instant.now().plusSeconds(60)));
        when(joinPoint.proceed()).thenReturn("done");

        aspect.enforceIdempotency(joinPoint, idempotent);

        assertThat(IdempotentConsumerAspect.getCurrentSyncContext()).isNull();
    }

    @Test
    @DisplayName("Issue 5: non-batch method with custom one-pass Iterable does not lose first element")
    void shouldNotConsumeFirstElementOfOnePassIterableInNonBatchMethod() throws Throwable {
        Method method = TestService.class.getMethod("handleBusinessList", String.class, java.util.List.class);
        when(joinPoint.getSignature()).thenReturn(methodSignature);
        when(methodSignature.getMethod()).thenReturn(method);

        java.util.concurrent.atomic.AtomicInteger nextCalls = new java.util.concurrent.atomic.AtomicInteger(0);
        Iterable<String> onePassIterable = () -> new java.util.Iterator<>() {
            private int index = 0;
            @Override
            public boolean hasNext() {
                return index < 2;
            }
            @Override
            public String next() {
                nextCalls.incrementAndGet();
                return "item-" + index++;
            }
        };

        when(joinPoint.getArgs()).thenReturn(new Object[]{"business-key", onePassIterable});
        when(joinPoint.getTarget()).thenReturn(new TestService());
        when(idempotent.key()).thenReturn("#root.args[0]");
        when(idempotent.consumerGroup()).thenReturn("orders-group");
        when(idempotent.ttl()).thenReturn("7d");
        when(idempotent.lockTimeout()).thenReturn("2m");

        when(idempotentRepository.tryAcquireWithLease(eq("business-key"), eq("orders-group"), any(), any()))
                .thenReturn(new io.github.alexlitvinov.idembox.core.model.AcquireOutcome(AcquireResult.ACQUIRED, Instant.now().plusSeconds(60)));
        when(joinPoint.proceed()).thenReturn("processed");

        aspect.enforceIdempotency(joinPoint, idempotent);

        // Aspect inspection must NOT have called next() on the iterable!
        assertThat(nextCalls.get()).isZero();
    }
}
