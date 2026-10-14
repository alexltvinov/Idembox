package io.github.alexlitvinov.idembox.starter.aop;

import io.github.alexlitvinov.idembox.core.annotation.Idempotent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.BeanInitializationException;
import org.springframework.context.support.GenericApplicationContext;

import java.lang.reflect.Method;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IdempotentStartupValidatorTest {

    private IdempotentStartupValidator validator;

    @BeforeEach
    void setUp() {
        validator = new IdempotentStartupValidator();
    }

    static class ValidSampleService {
        @Idempotent(key = "#event.orderId", consumerGroup = "orders-group", ttl = "7d", lockTimeout = "2m")
        public void process(Object event) {
            java.util.Objects.requireNonNull(event);
        }

        @Idempotent(key = "#p0", ttl = "10s", lockTimeout = "500ms")
        public void processWithParamIndex(String id) {
            java.util.Objects.requireNonNull(id);
        }
    }

    static class BlankKeyService {
        @Idempotent(key = "   ", consumerGroup = "orders-group")
        public void process(Object event) {
            java.util.Objects.requireNonNull(event);
        }
    }

    static class InvalidSpelKeyService {
        @Idempotent(key = "#event.orderId +", consumerGroup = "orders-group")
        public void process(Object event) {
            java.util.Objects.requireNonNull(event);
        }
    }

    static class DisallowedSpelTypeService {
        @Idempotent(key = "T(java.lang.System).currentTimeMillis()", consumerGroup = "orders-group")
        public void process(Object event) {
            java.util.Objects.requireNonNull(event);
        }
    }

    static class InvalidTtlService {
        @Idempotent(key = "#event.orderId", consumerGroup = "orders-group", ttl = "7 days")
        public void process(Object event) {
            java.util.Objects.requireNonNull(event);
        }
    }

    static class InvalidLockTimeoutService {
        @Idempotent(key = "#event.orderId", consumerGroup = "orders-group", lockTimeout = "unparsable")
        public void process(Object event) {
            java.util.Objects.requireNonNull(event);
        }
    }

    static class PrivateMethodService {
        @Idempotent(key = "#event.orderId", consumerGroup = "orders-group")
        private void process(Object event) {
            java.util.Objects.requireNonNull(event);
        }
    }

    static class FinalMethodService {
        @Idempotent(key = "#event.orderId", consumerGroup = "orders-group")
        public final void process(Object event) {
            java.util.Objects.requireNonNull(event);
        }
    }

    static class StaticMethodService {
        @Idempotent(key = "#event.orderId", consumerGroup = "orders-group")
        public static void process(Object event) {
            java.util.Objects.requireNonNull(event);
        }
    }

    @Test
    @DisplayName("Senior Refinements v21: Valid @Idempotent annotations pass startup validation")
    void validAnnotationsShouldPassValidation() throws Exception {
        Method method = ValidSampleService.class.getMethod("process", Object.class);
        Idempotent idempotent = method.getAnnotation(Idempotent.class);

        assertThatCode(() -> validator.validateIdempotentAnnotation(ValidSampleService.class, method, idempotent))
                .doesNotThrowAnyException();

        Method method2 = ValidSampleService.class.getMethod("processWithParamIndex", String.class);
        Idempotent idempotent2 = method2.getAnnotation(Idempotent.class);
        assertThatCode(() -> validator.validateIdempotentAnnotation(ValidSampleService.class, method2, idempotent2))
                .doesNotThrowAnyException();
    }

    @Test
    @DisplayName("Senior Refinements v21: Blank key expression throws BeanInitializationException")
    void blankKeyShouldThrowBeanInitializationException() throws Exception {
        Method method = BlankKeyService.class.getMethod("process", Object.class);
        Idempotent idempotent = method.getAnnotation(Idempotent.class);

        assertThatThrownBy(() -> validator.validateIdempotentAnnotation(BlankKeyService.class, method, idempotent))
                .isInstanceOf(BeanInitializationException.class)
                .hasMessageContaining("key expression cannot be blank");
    }

    @Test
    @DisplayName("Senior Refinements v21: Invalid SpEL syntax in key throws BeanInitializationException")
    void invalidSpelKeyShouldThrowBeanInitializationException() throws Exception {
        Method method = InvalidSpelKeyService.class.getMethod("process", Object.class);
        Idempotent idempotent = method.getAnnotation(Idempotent.class);

        assertThatThrownBy(() -> validator.validateIdempotentAnnotation(InvalidSpelKeyService.class, method, idempotent))
                .isInstanceOf(BeanInitializationException.class)
                .hasMessageContaining("Invalid SpEL expression");
    }

    @Test
    @DisplayName("Senior Refinements v21: Disallowed SpEL type reference T(...) throws BeanInitializationException")
    void disallowedSpelTypeReferenceShouldThrowBeanInitializationException() throws Exception {
        Method method = DisallowedSpelTypeService.class.getMethod("process", Object.class);
        Idempotent idempotent = method.getAnnotation(Idempotent.class);

        assertThatThrownBy(() -> validator.validateIdempotentAnnotation(DisallowedSpelTypeService.class, method, idempotent))
                .isInstanceOf(BeanInitializationException.class)
                .hasMessageContaining("Type references like T(...) are not permitted");
    }

    @Test
    @DisplayName("Senior Refinements v21: Typo in duration string (e.g. '7 days') throws BeanInitializationException")
    void invalidTtlDurationShouldThrowBeanInitializationException() throws Exception {
        Method method = InvalidTtlService.class.getMethod("process", Object.class);
        Idempotent idempotent = method.getAnnotation(Idempotent.class);

        assertThatThrownBy(() -> validator.validateIdempotentAnnotation(InvalidTtlService.class, method, idempotent))
                .isInstanceOf(BeanInitializationException.class)
                .hasMessageContaining("Invalid ttl duration in @Idempotent(ttl = \"7 days\")");
    }

    @Test
    @DisplayName("Senior Refinements v21: Unparsable lockTimeout throws BeanInitializationException")
    void invalidLockTimeoutDurationShouldThrowBeanInitializationException() throws Exception {
        Method method = InvalidLockTimeoutService.class.getMethod("process", Object.class);
        Idempotent idempotent = method.getAnnotation(Idempotent.class);

        assertThatThrownBy(() -> validator.validateIdempotentAnnotation(InvalidLockTimeoutService.class, method, idempotent))
                .isInstanceOf(BeanInitializationException.class)
                .hasMessageContaining("Invalid lockTimeout duration in @Idempotent(lockTimeout = \"unparsable\")");
    }

    @Test
    @DisplayName("Senior Refinements v21: ApplicationContext bean scanning detects invalid @Idempotent methods on startup")
    void applicationContextScanningShouldDetectInvalidBeans() {
        GenericApplicationContext ctx = new GenericApplicationContext();
        ctx.registerBean("validService", ValidSampleService.class);
        ctx.registerBean("invalidService", InvalidTtlService.class);
        ctx.refresh();

        validator.setApplicationContext(ctx);

        assertThatThrownBy(() -> validator.afterSingletonsInstantiated())
                .isInstanceOf(BeanInitializationException.class)
                .hasMessageContaining("Invalid ttl duration");

        ctx.close();
    }

    @Test
    @DisplayName("Senior Refinements v22: Private method with @Idempotent throws BeanInitializationException")
    void privateMethodShouldThrowBeanInitializationException() throws Exception {
        Method method = PrivateMethodService.class.getDeclaredMethod("process", Object.class);
        Idempotent idempotent = method.getAnnotation(Idempotent.class);

        assertThatThrownBy(() -> validator.validateIdempotentAnnotation(PrivateMethodService.class, method, idempotent))
                .isInstanceOf(BeanInitializationException.class)
                .hasMessageContaining("@Idempotent cannot be applied to private method");
    }

    @Test
    @DisplayName("Senior Refinements v22: Final method with @Idempotent throws BeanInitializationException")
    void finalMethodShouldThrowBeanInitializationException() throws Exception {
        Method method = FinalMethodService.class.getMethod("process", Object.class);
        Idempotent idempotent = method.getAnnotation(Idempotent.class);

        assertThatThrownBy(() -> validator.validateIdempotentAnnotation(FinalMethodService.class, method, idempotent))
                .isInstanceOf(BeanInitializationException.class)
                .hasMessageContaining("@Idempotent cannot be applied to final method");
    }

    @Test
    @DisplayName("Senior Refinements v22: Static method with @Idempotent throws BeanInitializationException")
    void staticMethodShouldThrowBeanInitializationException() throws Exception {
        Method method = StaticMethodService.class.getMethod("process", Object.class);
        Idempotent idempotent = method.getAnnotation(Idempotent.class);

        assertThatThrownBy(() -> validator.validateIdempotentAnnotation(StaticMethodService.class, method, idempotent))
                .isInstanceOf(BeanInitializationException.class)
                .hasMessageContaining("@Idempotent cannot be applied to static method");
    }

    public interface SampleServiceInterface {
        void process(String id);
    }

    public static class SampleServiceImpl implements SampleServiceInterface {
        @Override
        @Idempotent(key = "#p0", ttl = "7 days") // invalid duration
        public void process(String id) {
            java.util.Objects.requireNonNull(id);
        }
    }

    static class BatchListenerService {
        @org.springframework.kafka.annotation.KafkaListener(topics = "orders")
        @Idempotent(key = "#p0[0].id")
        public void onBatch(java.util.List<Object> batch) {
            java.util.Objects.requireNonNull(batch);
        }
    }

    static class BatchListenerWithAckService {
        @org.springframework.kafka.annotation.KafkaListener(topics = "orders")
        @Idempotent(key = "#p1[0].id")
        public void onBatchWithAcknowledgment(org.springframework.kafka.support.Acknowledgment ack, java.util.List<Object> batch) {
            java.util.Objects.requireNonNull(ack);
            java.util.Objects.requireNonNull(batch);
        }
    }

    @Test
    @DisplayName("Senior Refinements v24: ApplicationContext scanning unwraps JDK Dynamic Proxies to validate concrete class annotations")
    void jdkDynamicProxyScanningShouldDetectInvalidBeans() throws Exception {
        assertThat(SampleServiceInterface.class.getMethod("process", String.class)).isNotNull();
        GenericApplicationContext ctx = new GenericApplicationContext();
        ctx.registerBean("jdkProxiedService", SampleServiceInterface.class, () -> {
            org.springframework.aop.framework.ProxyFactory pf = new org.springframework.aop.framework.ProxyFactory(new SampleServiceImpl());
            pf.setInterfaces(SampleServiceInterface.class);
            pf.setProxyTargetClass(false); // force JDK Dynamic Proxy
            return (SampleServiceInterface) pf.getProxy();
        });
        ctx.refresh();

        validator.setApplicationContext(ctx);

        assertThatThrownBy(() -> validator.afterSingletonsInstantiated())
                .isInstanceOf(BeanInitializationException.class)
                .hasMessageContaining("Invalid ttl duration");

        ctx.close();
    }

    @Test
    @DisplayName("Senior Refinements v24: Method accepting Collection or Iterable throws BeanInitializationException")
    void batchListenerShouldThrowBeanInitializationException() throws Exception {
        Method method = BatchListenerService.class.getMethod("onBatch", java.util.List.class);
        Idempotent idempotent = method.getAnnotation(Idempotent.class);

        assertThatThrownBy(() -> validator.validateIdempotentAnnotation(BatchListenerService.class, method, idempotent))
                .isInstanceOf(BeanInitializationException.class)
                .hasMessageContaining("@Idempotent cannot be applied to batch listener method accepting Collection or Iterable");
    }

    @Test
    @DisplayName("Senior Refinements v25: Method accepting Collection as non-first parameter throws BeanInitializationException")
    void batchListenerWithAckShouldThrowBeanInitializationException() throws Exception {
        Method method = BatchListenerWithAckService.class.getMethod("onBatchWithAcknowledgment", org.springframework.kafka.support.Acknowledgment.class, java.util.List.class);
        Idempotent idempotent = method.getAnnotation(Idempotent.class);

        assertThatThrownBy(() -> validator.validateIdempotentAnnotation(BatchListenerWithAckService.class, method, idempotent))
                .isInstanceOf(BeanInitializationException.class)
                .hasMessageContaining("@Idempotent cannot be applied to batch listener method accepting Collection or Iterable");
    }

    static class ConsumerRecordArrayBatchService {
        @Idempotent(key = "#p0[0].key()")
        public void onBatch(org.apache.kafka.clients.consumer.ConsumerRecord<?, ?>[] records) {
            java.util.Objects.requireNonNull(records);
        }
    }

    static class KafkaListenerObjectArrayBatchService {
        @org.springframework.kafka.annotation.KafkaListener(topics = "orders", batch = "true")
        @Idempotent(key = "#p0[0]")
        public void onBatch(String[] messages) {
            java.util.Objects.requireNonNull(messages);
        }
    }

    static class KafkaListenerSingleRecordArrayPayloadService {
        @org.springframework.kafka.annotation.KafkaListener(topics = "orders")
        @Idempotent(key = "#p0[0]")
        public void onSingleArray(String[] tags) {
            java.util.Objects.requireNonNull(tags);
        }

        @org.springframework.kafka.annotation.KafkaListener(topics = "orders")
        @Idempotent(key = "#p0")
        public void onSingleWithHeaderArray(String orderId, @org.springframework.messaging.handler.annotation.Header("roles") String[] roles) {
            java.util.Objects.requireNonNull(orderId);
            java.util.Objects.requireNonNull(roles);
        }
    }

    static class MessageArrayBatchService {
        @Idempotent(key = "#p0[0].payload")
        public void onBatch(org.springframework.messaging.Message<?>[] messages) {
            java.util.Objects.requireNonNull(messages);
        }
    }

    static class KafkaListenerByteArraySingleService {
        @org.springframework.kafka.annotation.KafkaListener(topics = "orders")
        @Idempotent(key = "#p0.length")
        public void onSingle(byte[] payload) {
            java.util.Objects.requireNonNull(payload);
        }
    }

    @Test
    @DisplayName("Senior Refinements v54: Method accepting ConsumerRecord[] throws BeanInitializationException")
    void batchListenerConsumerRecordArrayShouldThrowBeanInitializationException() throws Exception {
        Method method = ConsumerRecordArrayBatchService.class.getMethod("onBatch", org.apache.kafka.clients.consumer.ConsumerRecord[].class);
        Idempotent idempotent = method.getAnnotation(Idempotent.class);

        assertThatThrownBy(() -> validator.validateIdempotentAnnotation(ConsumerRecordArrayBatchService.class, method, idempotent))
                .isInstanceOf(BeanInitializationException.class)
                .hasMessageContaining("@Idempotent cannot be applied to batch listener method");
    }

    @Test
    @DisplayName("Senior Refinements Round 70: @KafkaListener(batch = 'true') method accepting non-primitive array throws BeanInitializationException")
    void kafkaListenerObjectArrayBatchShouldThrowBeanInitializationException() throws Exception {
        Method method = KafkaListenerObjectArrayBatchService.class.getMethod("onBatch", String[].class);
        Idempotent idempotent = method.getAnnotation(Idempotent.class);

        assertThatThrownBy(() -> validator.validateIdempotentAnnotation(KafkaListenerObjectArrayBatchService.class, method, idempotent))
                .isInstanceOf(BeanInitializationException.class)
                .hasMessageContaining("@Idempotent cannot be applied to batch listener method");
    }

    @Test
    @DisplayName("Senior Refinements Round 70: single-record @KafkaListener accepting String[] payload or @Header String[] passes validation")
    void kafkaListenerSingleRecordWithArrayPayloadOrHeaderArrayShouldPassValidation() throws Exception {
        Method m1 = KafkaListenerSingleRecordArrayPayloadService.class.getMethod("onSingleArray", String[].class);
        assertThatCode(() -> validator.validateIdempotentAnnotation(
                KafkaListenerSingleRecordArrayPayloadService.class, m1, m1.getAnnotation(Idempotent.class)))
                .doesNotThrowAnyException();

        Method m2 = KafkaListenerSingleRecordArrayPayloadService.class.getMethod("onSingleWithHeaderArray", String.class, String[].class);
        assertThatCode(() -> validator.validateIdempotentAnnotation(
                KafkaListenerSingleRecordArrayPayloadService.class, m2, m2.getAnnotation(Idempotent.class)))
                .doesNotThrowAnyException();

        Method mBatchMsg = MessageArrayBatchService.class.getMethod("onBatch", org.springframework.messaging.Message[].class);
        assertThatThrownBy(() -> validator.validateIdempotentAnnotation(
                MessageArrayBatchService.class, mBatchMsg, mBatchMsg.getAnnotation(Idempotent.class)))
                .isInstanceOf(BeanInitializationException.class)
                .hasMessageContaining("@Idempotent cannot be applied to batch listener method");
    }

    @Test
    @DisplayName("Senior Refinements v54: @KafkaListener method accepting byte[] payload is permitted as single message")
    void kafkaListenerByteArraySingleMessageShouldPassValidation() throws Exception {
        Method method = KafkaListenerByteArraySingleService.class.getMethod("onSingle", byte[].class);
        Idempotent idempotent = method.getAnnotation(Idempotent.class);

        assertThatCode(() -> validator.validateIdempotentAnnotation(KafkaListenerByteArraySingleService.class, method, idempotent))
                .doesNotThrowAnyException();
    }

    static class IterableBatchService {
        @org.springframework.kafka.annotation.KafkaListener(topics = "orders")
        @Idempotent(key = "#p0.iterator().next()")
        public void onBatch(Iterable<String> items) {
            java.util.Objects.requireNonNull(items);
        }
    }

    @Test
    @DisplayName("Senior Refinements v55: Method accepting Iterable with @KafkaListener throws BeanInitializationException")
    void batchListenerIterableShouldThrowBeanInitializationException() throws Exception {
        Method method = IterableBatchService.class.getMethod("onBatch", Iterable.class);
        Idempotent idempotent = method.getAnnotation(Idempotent.class);

        assertThatThrownBy(() -> validator.validateIdempotentAnnotation(IterableBatchService.class, method, idempotent))
                .isInstanceOf(BeanInitializationException.class)
                .hasMessageContaining("@Idempotent cannot be applied to batch listener method");
    }

    static class BusinessCollectionService {
        @Idempotent(key = "#orderId")
        public void processOrderItems(String orderId, java.util.List<String> items) {
            java.util.Objects.requireNonNull(orderId);
            java.util.Objects.requireNonNull(items);
        }

        @Idempotent(key = "#orderId")
        public void processWithIterable(String orderId, Iterable<String> items) {
            java.util.Objects.requireNonNull(orderId);
            java.util.Objects.requireNonNull(items);
        }
    }

    @Test
    @DisplayName("Senior Refinements Round 63: Non-Kafka methods accepting List or Iterable pass startup validation")
    void nonKafkaMethodsWithCollectionOrIterableShouldPassValidation() throws Exception {
        Method listMethod = BusinessCollectionService.class.getMethod("processOrderItems", String.class, java.util.List.class);
        Idempotent listIdempotent = listMethod.getAnnotation(Idempotent.class);
        assertThatCode(() -> validator.validateIdempotentAnnotation(BusinessCollectionService.class, listMethod, listIdempotent))
                .doesNotThrowAnyException();

        Method iterMethod = BusinessCollectionService.class.getMethod("processWithIterable", String.class, Iterable.class);
        Idempotent iterIdempotent = iterMethod.getAnnotation(Idempotent.class);
        assertThatCode(() -> validator.validateIdempotentAnnotation(BusinessCollectionService.class, iterMethod, iterIdempotent))
                .doesNotThrowAnyException();
    }

    static class InvalidConditionService {
        @Idempotent(key = "#p0", condition = "#{invalid expression syntax")
        public void process(Object obj) {
            java.util.Objects.requireNonNull(obj);
        }
    }

    @Test
    @DisplayName("Senior Refinements v26: Invalid condition SpEL syntax throws BeanInitializationException")
    void invalidConditionSpelShouldThrowBeanInitializationException() throws Exception {
        Method method = InvalidConditionService.class.getMethod("process", Object.class);
        Idempotent idempotent = method.getAnnotation(Idempotent.class);

        assertThatThrownBy(() -> validator.validateIdempotentAnnotation(InvalidConditionService.class, method, idempotent))
                .isInstanceOf(BeanInitializationException.class)
                .hasMessageContaining("Invalid SpEL expression in @Idempotent(condition");
    }

    static class NonTransactionalIdempotentService {
        @Idempotent(key = "#p0")
        public void process(String id) {
            java.util.Objects.requireNonNull(id);
        }
    }

    static class TransactionalIdempotentService {
        @org.springframework.transaction.annotation.Transactional
        @Idempotent(key = "#p0")
        public void process(String id) {
            java.util.Objects.requireNonNull(id);
        }
    }

    @Test
    @DisplayName("Senior Refinements v28: Non-transactional method with @Idempotent logs WARN and passes startup validation")
    void nonTransactionalMethodShouldPassWithWarning() throws Exception {
        Method method = NonTransactionalIdempotentService.class.getMethod("process", String.class);
        Idempotent idempotent = method.getAnnotation(Idempotent.class);

        assertThatCode(() -> validator.validateIdempotentAnnotation(NonTransactionalIdempotentService.class, method, idempotent))
                .doesNotThrowAnyException();
    }

    @Test
    @DisplayName("Senior Refinements v28: Transactional method with @Idempotent passes startup validation")
    void transactionalMethodShouldPassValidation() throws Exception {
        Method method = TransactionalIdempotentService.class.getMethod("process", String.class);
        Idempotent idempotent = method.getAnnotation(Idempotent.class);

        assertThatCode(() -> validator.validateIdempotentAnnotation(TransactionalIdempotentService.class, method, idempotent))
                .doesNotThrowAnyException();
    }

    static class ReactiveMonoService {
        @Idempotent(key = "#p0")
        public reactor.core.publisher.Mono<String> processMono(String id) {
            return reactor.core.publisher.Mono.just(id);
        }
    }

    static class ReactiveFluxService {
        @Idempotent(key = "#p0")
        public reactor.core.publisher.Flux<String> processFlux(String id) {
            return reactor.core.publisher.Flux.just(id);
        }
    }

    @Test
    @DisplayName("Senior Refinements v29: Reactive method returning Mono throws BeanInitializationException")
    void reactiveMonoMethodShouldThrowBeanInitializationException() throws Exception {
        Method method = ReactiveMonoService.class.getMethod("processMono", String.class);
        Idempotent idempotent = method.getAnnotation(Idempotent.class);

        assertThatThrownBy(() -> validator.validateIdempotentAnnotation(ReactiveMonoService.class, method, idempotent))
                .isInstanceOf(BeanInitializationException.class)
                .hasMessageContaining("@Idempotent cannot be applied to reactive method returning reactor.core.publisher.Mono");
    }

    @Test
    @DisplayName("Senior Refinements v29: Reactive method returning Flux throws BeanInitializationException")
    void reactiveFluxMethodShouldThrowBeanInitializationException() throws Exception {
        Method method = ReactiveFluxService.class.getMethod("processFlux", String.class);
        Idempotent idempotent = method.getAnnotation(Idempotent.class);

        assertThatThrownBy(() -> validator.validateIdempotentAnnotation(ReactiveFluxService.class, method, idempotent))
                .isInstanceOf(BeanInitializationException.class)
                .hasMessageContaining("@Idempotent cannot be applied to reactive method returning reactor.core.publisher.Flux");
    }

    interface SampleOrderConsumer {
        @Idempotent(key = "#p0")
        void handle(String id);
    }

    static class SampleOrderConsumerImpl implements SampleOrderConsumer {
        @Override
        public void handle(String id) {
            java.util.Objects.requireNonNull(id);
        }
    }

    @Test
    @DisplayName("Senior Refinements v30: @Idempotent declared only on interface throws BeanInitializationException")
    void interfaceOnlyIdempotentShouldThrowBeanInitializationException() throws Exception {
        Method method = SampleOrderConsumerImpl.class.getMethod("handle", String.class);
        Idempotent idempotent = org.springframework.core.annotation.AnnotatedElementUtils.findMergedAnnotation(method, Idempotent.class);

        assertThatThrownBy(() -> validator.validateIdempotentAnnotation(SampleOrderConsumerImpl.class, method, idempotent))
                .isInstanceOf(BeanInitializationException.class)
                .hasMessageContaining("Spring AOP @annotation pointcuts do not match interface annotations in CGLIB proxy mode");
    }

    @Test
    @DisplayName("Senior Refinements v30: @Idempotent validated directly on interface type throws BeanInitializationException")
    void interfaceTypeValidationShouldThrowBeanInitializationException() throws Exception {
        Method method = SampleOrderConsumer.class.getMethod("handle", String.class);
        Idempotent idempotent = method.getAnnotation(Idempotent.class);

        assertThatThrownBy(() -> validator.validateIdempotentAnnotation(SampleOrderConsumer.class, method, idempotent))
                .isInstanceOf(BeanInitializationException.class)
                .hasMessageContaining("Spring AOP @annotation pointcuts do not match interface annotations in CGLIB proxy mode");
    }

    static class LazyAnnotatedService {
        static final java.util.concurrent.atomic.AtomicBoolean INSTANTIATED = new java.util.concurrent.atomic.AtomicBoolean(false);

        public LazyAnnotatedService() {
            INSTANTIATED.set(true);
        }

        @Idempotent(key = "#id")
        public void processLazy(String id) {
            java.util.Objects.requireNonNull(id);
        }
    }

    static class ScopedAnnotatedService {
        @Idempotent(key = "#id")
        public void processScoped(String id) {
            java.util.Objects.requireNonNull(id);
        }
    }

    @Test
    @DisplayName("Senior Refinements v36: afterSingletonsInstantiated does not eagerly instantiate @Lazy beans")
    void shouldNotEagerlyInstantiateLazyBeans() throws Exception {
        assertThat(LazyAnnotatedService.class.getMethod("processLazy", String.class)).isNotNull();
        try (GenericApplicationContext context = createLazyContext()) {
            LazyAnnotatedService.INSTANTIATED.set(false);

            IdempotentStartupValidator startupValidator = new IdempotentStartupValidator();
            startupValidator.setApplicationContext(context);
            startupValidator.afterSingletonsInstantiated();

            // Bean class was validated without invoking its constructor!
            assertThat(LazyAnnotatedService.INSTANTIATED.get()).isFalse();
        }
    }

    private GenericApplicationContext createLazyContext() {
        GenericApplicationContext context = new GenericApplicationContext();
        org.springframework.beans.factory.support.RootBeanDefinition lazyBd = new org.springframework.beans.factory.support.RootBeanDefinition(LazyAnnotatedService.class);
        lazyBd.setLazyInit(true);
        context.registerBeanDefinition("lazyService", lazyBd);
        context.refresh();
        return context;
    }

    @Test
    @DisplayName("Senior Refinements v36: afterSingletonsInstantiated inspects prototype/scoped beans without calling getBean()")
    void shouldInspectScopedBeansWithoutEagerInstantiation() throws Exception {
        assertThat(ScopedAnnotatedService.class.getMethod("processScoped", String.class)).isNotNull();
        try (GenericApplicationContext context = createScopedContext()) {
            IdempotentStartupValidator startupValidator = new IdempotentStartupValidator();
            startupValidator.setApplicationContext(context);

            assertThatCode(startupValidator::afterSingletonsInstantiated).doesNotThrowAnyException();
        }
    }

    private GenericApplicationContext createScopedContext() {
        GenericApplicationContext context = new GenericApplicationContext();
        org.springframework.beans.factory.support.RootBeanDefinition prototypeBd = new org.springframework.beans.factory.support.RootBeanDefinition(ScopedAnnotatedService.class);
        prototypeBd.setScope(org.springframework.beans.factory.config.BeanDefinition.SCOPE_PROTOTYPE);
        context.registerBeanDefinition("scopedService", prototypeBd);
        context.refresh();
        return context;
    }

    static class DomainReturnTypeService {
        record OrderResult(String status, double amount) {}

        @Idempotent(key = "#orderId")
        public OrderResult processOrder(String orderId) {
            java.util.Objects.requireNonNull(orderId);
            return new OrderResult("OK", 100.0);
        }

        @Idempotent(key = "#orderId")
        public java.util.Optional<OrderResult> findOrder(String orderId) {
            java.util.Objects.requireNonNull(orderId);
            return java.util.Optional.empty();
        }
    }

    @Test
    @DisplayName("Senior Refinements v39: validateIdempotentAnnotation allows domain return types with swallowDuplicate=true without startup failure")
    void shouldAllowDomainReturnTypesWithSwallowDuplicateWithoutThrowing() throws NoSuchMethodException {
        IdempotentStartupValidator validator = new IdempotentStartupValidator();
        Method domainMethod = DomainReturnTypeService.class.getMethod("processOrder", String.class);
        Idempotent domainAnn = domainMethod.getAnnotation(Idempotent.class);

        assertThatCode(() -> validator.validateIdempotentAnnotation(DomainReturnTypeService.class, domainMethod, domainAnn))
                .doesNotThrowAnyException();

        Method optionalMethod = DomainReturnTypeService.class.getMethod("findOrder", String.class);
        Idempotent optionalAnn = optionalMethod.getAnnotation(Idempotent.class);

        assertThatCode(() -> validator.validateIdempotentAnnotation(DomainReturnTypeService.class, optionalMethod, optionalAnn))
                .doesNotThrowAnyException();
    }

    @Test
    @DisplayName("Senior Refinements v39: isSafeSwallowReturnType correctly identifies safe container/void types vs domain objects")
    void shouldClassifySafeSwallowReturnTypesCorrectly() {
        assertThat(IdempotentStartupValidator.isSafeSwallowReturnType(void.class)).isTrue();
        assertThat(IdempotentStartupValidator.isSafeSwallowReturnType(Void.class)).isTrue();
        assertThat(IdempotentStartupValidator.isSafeSwallowReturnType(java.util.Optional.class)).isTrue();
        assertThat(IdempotentStartupValidator.isSafeSwallowReturnType(java.util.List.class)).isTrue();
        assertThat(IdempotentStartupValidator.isSafeSwallowReturnType(java.util.Set.class)).isTrue();
        assertThat(IdempotentStartupValidator.isSafeSwallowReturnType(java.util.Map.class)).isTrue();
        assertThat(IdempotentStartupValidator.isSafeSwallowReturnType(java.util.concurrent.CompletableFuture.class)).isTrue();

        assertThat(IdempotentStartupValidator.isSafeSwallowReturnType(DomainReturnTypeService.OrderResult.class)).isFalse();
        assertThat(IdempotentStartupValidator.isSafeSwallowReturnType(String.class)).isFalse();
        assertThat(IdempotentStartupValidator.isSafeSwallowReturnType(int.class)).isFalse();
        assertThat(IdempotentStartupValidator.isSafeSwallowReturnType(boolean.class)).isFalse();
    }

    static abstract class BaseOrderConsumer {
        @Idempotent(key = "#p0")
        public void processOrder(String orderId) {
            java.util.Objects.requireNonNull(orderId);
        }
    }

    static class CustomOverriddenConsumer extends BaseOrderConsumer {
        @Override
        public void processOrder(String orderId) {
            super.processOrder(orderId);
        }
    }

    static class CustomInheritedConsumer extends BaseOrderConsumer {
        // does not override processOrder
    }

    @Test
    @DisplayName("Senior Refinements v42: Overridden method without explicit @Idempotent throws BeanInitializationException with subclass instruction")
    void overriddenMethodWithoutExplicitAnnotationShouldThrowSubclassInstruction() throws Exception {
        Method method = CustomOverriddenConsumer.class.getMethod("processOrder", String.class);
        Idempotent idempotent = org.springframework.core.annotation.AnnotatedElementUtils.findMergedAnnotation(method, Idempotent.class);

        assertThatThrownBy(() -> validator.validateIdempotentAnnotation(CustomOverriddenConsumer.class, method, idempotent))
                .isInstanceOf(BeanInitializationException.class)
                .hasMessageContaining("Overridden method CustomOverriddenConsumer#processOrder must explicitly declare @Idempotent on the subclass method");
    }

    @Test
    @DisplayName("Senior Refinements v42: Inherited method without override passes validation")
    void inheritedMethodWithoutOverrideShouldPassValidation() throws Exception {
        Method method = CustomInheritedConsumer.class.getMethod("processOrder", String.class);
        Idempotent idempotent = org.springframework.core.annotation.AnnotatedElementUtils.findMergedAnnotation(method, Idempotent.class);

        assertThatCode(() -> validator.validateIdempotentAnnotation(CustomInheritedConsumer.class, method, idempotent))
                .doesNotThrowAnyException();
    }

    static class SendToConsumerService {
        @Idempotent(key = "#orderId")
        @org.springframework.messaging.handler.annotation.SendTo("reply-topic")
        public String handleOrderWithReply(String orderId) {
            return "REPLY-" + orderId;
        }
    }

    @Test
    @DisplayName("Senior Refinements v46: hasSendToAnnotation detects @SendTo and validator logs warning for swallowDuplicate = true")
    void sendToWithSwallowDuplicateShouldBeDetected() throws Exception {
        Method method = SendToConsumerService.class.getMethod("handleOrderWithReply", String.class);
        Idempotent idempotent = method.getAnnotation(Idempotent.class);

        assertThat(IdempotentStartupValidator.hasSendToAnnotation(method, SendToConsumerService.class)).isTrue();
        assertThatCode(() -> validator.validateIdempotentAnnotation(SendToConsumerService.class, method, idempotent))
                .doesNotThrowAnyException();
    }

    static class AsyncTransactionalConsumerService {
        @Idempotent(key = "#orderId")
        @org.springframework.transaction.annotation.Transactional
        public java.util.concurrent.CompletableFuture<String> handleAsyncTransactional(String orderId) {
            return java.util.concurrent.CompletableFuture.completedFuture("OK-" + orderId);
        }
    }

    @Test
    @DisplayName("Senior Refinements v47: Validator validates @Idempotent + @Transactional with CompletableFuture without throwing exception")
    void asyncTransactionalWithCompletableFutureShouldValidateWithoutException() throws Exception {
        Method method = AsyncTransactionalConsumerService.class.getMethod("handleAsyncTransactional", String.class);
        Idempotent idempotent = method.getAnnotation(Idempotent.class);

        assertThatCode(() -> validator.validateIdempotentAnnotation(AsyncTransactionalConsumerService.class, method, idempotent))
                .doesNotThrowAnyException();
    }

    static class DomainIterableCart implements Iterable<String> {
        @Override
        @org.springframework.lang.NonNull
        public java.util.Iterator<String> iterator() {
            return java.util.Collections.emptyIterator();
        }
    }

    static class JsonNodeConsumerService {
        @Idempotent(key = "#node.get('orderId').asText()")
        public void processJsonNode(com.fasterxml.jackson.databind.JsonNode node) {
            java.util.Objects.requireNonNull(node);
        }

        @Idempotent(key = "#cart.toString()")
        public void processCart(DomainIterableCart cart) {
            java.util.Objects.requireNonNull(cart);
        }
    }

    @Test
    @DisplayName("Senior Refinements v50: Validator permits JsonNode and domain models implementing Iterable")
    void shouldPermitJsonNodeAndDomainIterableParameters() throws Exception {
        Method jsonNodeMethod = JsonNodeConsumerService.class.getMethod("processJsonNode", com.fasterxml.jackson.databind.JsonNode.class);
        Idempotent idempotentNode = jsonNodeMethod.getAnnotation(Idempotent.class);
        assertThatCode(() -> validator.validateIdempotentAnnotation(JsonNodeConsumerService.class, jsonNodeMethod, idempotentNode))
                .doesNotThrowAnyException();

        Method cartMethod = JsonNodeConsumerService.class.getMethod("processCart", DomainIterableCart.class);
        Idempotent idempotentCart = cartMethod.getAnnotation(Idempotent.class);
        assertThatCode(() -> validator.validateIdempotentAnnotation(JsonNodeConsumerService.class, cartMethod, idempotentCart))
                .doesNotThrowAnyException();
    }

    static class FlowPublisherService {
        @Idempotent(key = "#orderId")
        public java.util.concurrent.Flow.Publisher<String> processFlow(String orderId) {
            java.util.Objects.requireNonNull(orderId);
            return null;
        }
    }

    @Test
    @DisplayName("Senior Refinements Round 58: Validator rejects methods returning JDK 9+ Flow.Publisher")
    void shouldRejectFlowPublisherReturnType() throws Exception {
        Method method = FlowPublisherService.class.getMethod("processFlow", String.class);
        Idempotent idempotent = method.getAnnotation(Idempotent.class);

        assertThatThrownBy(() -> validator.validateIdempotentAnnotation(FlowPublisherService.class, method, idempotent))
                .isInstanceOf(BeanInitializationException.class)
                .hasMessageContaining("reactive method returning java.util.concurrent.Flow$Publisher")
                .hasMessageContaining("Reactive publishers execute lazily");
    }

    static class JakartaTransactionalConsumerService {
        @Idempotent(key = "#orderId")
        @jakarta.transaction.Transactional
        public void handleJakartaTransactional(String orderId) {
            java.util.Objects.requireNonNull(orderId);
        }

        @Idempotent(key = "#orderId")
        @jakarta.transaction.Transactional
        public java.util.concurrent.CompletableFuture<String> handleJakartaAsyncTransactional(String orderId) {
            return java.util.concurrent.CompletableFuture.completedFuture("OK-" + orderId);
        }
    }

    @Test
    @DisplayName("Senior Refinements Round 60: Validator recognizes @Idempotent + @jakarta.transaction.Transactional")
    void jakartaTransactionalShouldValidateSuccessfully() throws Exception {
        Method method = JakartaTransactionalConsumerService.class.getMethod("handleJakartaTransactional", String.class);
        Idempotent idempotent = method.getAnnotation(Idempotent.class);
        assertThatCode(() -> validator.validateIdempotentAnnotation(JakartaTransactionalConsumerService.class, method, idempotent))
                .doesNotThrowAnyException();

        Method asyncMethod = JakartaTransactionalConsumerService.class.getMethod("handleJakartaAsyncTransactional", String.class);
        Idempotent asyncIdempotent = asyncMethod.getAnnotation(Idempotent.class);
        assertThatCode(() -> validator.validateIdempotentAnnotation(JakartaTransactionalConsumerService.class, asyncMethod, asyncIdempotent))
                .doesNotThrowAnyException();
    }

    static abstract class AbstractMethodConsumerService {
        @Idempotent(key = "#orderId")
        public abstract void processOrder(String orderId);
    }

    @Test
    @DisplayName("Senior Refinements Round 62: @Idempotent on abstract method throws BeanInitializationException")
    void abstractMethodShouldThrowBeanInitializationException() throws Exception {
        Method method = AbstractMethodConsumerService.class.getMethod("processOrder", String.class);
        Idempotent idempotent = method.getAnnotation(Idempotent.class);

        assertThatThrownBy(() -> validator.validateIdempotentAnnotation(AbstractMethodConsumerService.class, method, idempotent))
                .isInstanceOf(BeanInitializationException.class)
                .hasMessageContaining("@Idempotent cannot be applied to abstract method: AbstractMethodConsumerService#processOrder")
                .hasMessageContaining("Method annotations are not inherited by overriding methods in subclasses");
    }

    @Test
    @DisplayName("Senior Refinements Round 71: Validator validates global idembox.idempotency.default-ttl and default-lock-timeout at startup")
    void shouldValidateGlobalDefaultTtlAndLockTimeoutAtStartup() {
        io.github.alexlitvinov.idembox.starter.config.IdemboxProperties props =
                new io.github.alexlitvinov.idembox.starter.config.IdemboxProperties();
        props.getIdempotency().setDefaultTtl("14d");
        props.getIdempotency().setDefaultLockTimeout("5m");

        IdempotentStartupValidator propValidator = new IdempotentStartupValidator(props);
        assertThatCode(() -> propValidator.validateGlobalProperties(props)).doesNotThrowAnyException();

        // Invalid default-ttl
        props.getIdempotency().setDefaultTtl("invalid-ttl");
        assertThatThrownBy(() -> propValidator.validateGlobalProperties(props))
                .isInstanceOf(BeanInitializationException.class)
                .hasMessageContaining("Invalid duration in idembox.idempotency.default-ttl");

        // Blank default-ttl
        props.getIdempotency().setDefaultTtl("   ");
        assertThatThrownBy(() -> propValidator.validateGlobalProperties(props))
                .isInstanceOf(BeanInitializationException.class)
                .hasMessageContaining("Invalid idembox.idempotency.default-ttl: cannot be blank");

        // Restore valid default-ttl, set invalid default-lock-timeout
        props.getIdempotency().setDefaultTtl("7d");
        props.getIdempotency().setDefaultLockTimeout("0s");
        assertThatThrownBy(() -> propValidator.validateGlobalProperties(props))
                .isInstanceOf(BeanInitializationException.class)
                .hasMessageContaining("Invalid duration in idembox.idempotency.default-lock-timeout");
    }

    static class SpringTransactionalConsumerService {
        @Idempotent(key = "#orderId")
        @org.springframework.transaction.annotation.Transactional
        public void handleSpringTransactional(String orderId) {
            java.util.Objects.requireNonNull(orderId);
        }

        @Idempotent(key = "#orderId")
        @org.springframework.transaction.annotation.Transactional
        public java.util.concurrent.CompletableFuture<String> handleSpringAsyncTransactional(String orderId) {
            return java.util.concurrent.CompletableFuture.completedFuture("OK-" + orderId);
        }
    }

    @Test
    @DisplayName("Senior Refinements Round 77: findDeclaredTransactionalInfo recognizes @Transactional at startup without active transaction and triggers CompletionStage check")
    void shouldRecognizeDeclaredTransactionalAtStartupWithoutActiveTransaction() throws Exception {
        org.springframework.transaction.support.TransactionSynchronizationManager.setActualTransactionActive(false);

        Method syncTxMethod = SpringTransactionalConsumerService.class.getMethod("handleSpringTransactional", String.class);
        TransactionalAnnotationUtils.TransactionalInfo syncInfo =
                TransactionalAnnotationUtils.findDeclaredTransactionalInfo(syncTxMethod, SpringTransactionalConsumerService.class);
        org.assertj.core.api.Assertions.assertThat(syncInfo.isTransactional()).isTrue();

        Method asyncSpringTxMethod = SpringTransactionalConsumerService.class.getMethod("handleSpringAsyncTransactional", String.class);
        TransactionalAnnotationUtils.TransactionalInfo asyncSpringInfo =
                TransactionalAnnotationUtils.findDeclaredTransactionalInfo(asyncSpringTxMethod, SpringTransactionalConsumerService.class);
        org.assertj.core.api.Assertions.assertThat(asyncSpringInfo.isTransactional()).isTrue();

        Method asyncTxMethod = JakartaTransactionalConsumerService.class.getMethod("handleJakartaAsyncTransactional", String.class);
        TransactionalAnnotationUtils.TransactionalInfo asyncInfo =
                TransactionalAnnotationUtils.findDeclaredTransactionalInfo(asyncTxMethod, JakartaTransactionalConsumerService.class);
        org.assertj.core.api.Assertions.assertThat(asyncInfo.isTransactional()).isTrue();

        Idempotent asyncIdempotent = asyncTxMethod.getAnnotation(Idempotent.class);
        assertThatCode(() -> validator.validateIdempotentAnnotation(JakartaTransactionalConsumerService.class, asyncTxMethod, asyncIdempotent))
                .doesNotThrowAnyException();
    }

    static final class FinalConsumerService {
        @Idempotent(key = "#orderId")
        public void process(String orderId) {
            java.util.Objects.requireNonNull(orderId);
        }
    }

    @Test
    @DisplayName("Issue 4: Validator rejects final classes with clear diagnostic message")
    void shouldRejectFinalClass() throws Exception {
        Method method = FinalConsumerService.class.getMethod("process", String.class);
        Idempotent idempotent = method.getAnnotation(Idempotent.class);

        assertThatThrownBy(() -> validator.validateIdempotentAnnotation(FinalConsumerService.class, method, idempotent))
                .isInstanceOf(BeanInitializationException.class)
                .hasMessageContaining("@Idempotent cannot be applied to final class")
                .hasMessageContaining("open class");
    }

    static class ReadOnlyTransactionalConsumerService {
        @Idempotent(key = "#orderId")
        @org.springframework.transaction.annotation.Transactional(readOnly = true)
        public void processReadOnly(String orderId) {
            java.util.Objects.requireNonNull(orderId);
        }
    }

    @Test
    @DisplayName("Deep Audit Issue 1: Validator detects @Transactional(readOnly = true) and allows startup with warning")
    void shouldDetectReadOnlyTransactionalAnnotation() throws Exception {
        Method method = ReadOnlyTransactionalConsumerService.class.getMethod("processReadOnly", String.class);
        Idempotent idempotent = method.getAnnotation(Idempotent.class);

        TransactionalAnnotationUtils.TransactionalInfo txInfo =
                TransactionalAnnotationUtils.findDeclaredTransactionalInfo(method, ReadOnlyTransactionalConsumerService.class);
        assertThat(txInfo.isTransactional()).isTrue();
        assertThat(txInfo.isReadOnly()).isTrue();

        assertThatCode(() -> validator.validateIdempotentAnnotation(ReadOnlyTransactionalConsumerService.class, method, idempotent))
                .doesNotThrowAnyException();
    }
}
