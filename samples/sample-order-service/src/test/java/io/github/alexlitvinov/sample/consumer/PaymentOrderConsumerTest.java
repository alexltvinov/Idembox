package io.github.alexlitvinov.sample.consumer;

import io.github.alexlitvinov.idembox.core.annotation.Idempotent;
import io.github.alexlitvinov.sample.event.OrderCreatedEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.lang.reflect.Method;
import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

class PaymentOrderConsumerTest {

    private final PaymentOrderConsumer consumer = new PaymentOrderConsumer();

    @Test
    @DisplayName("onOrderCreated should process event without throwing exceptions")
    void onOrderCreatedShouldProcessSuccessfully() {
        OrderCreatedEvent event = new OrderCreatedEvent("ORD-101", "CUST-5", new BigDecimal("250.00"), "RUB");

        assertThatNoException().isThrownBy(() -> consumer.onOrderCreated(event));
    }

    @Test
    @DisplayName("PaymentOrderConsumer should be annotated as Spring @Component")
    void consumerShouldHaveComponentAnnotation() {
        assertThat(PaymentOrderConsumer.class.isAnnotationPresent(Component.class)).isTrue();
    }

    @Test
    @DisplayName("onOrderCreated method must have exact @KafkaListener configuration")
    void onOrderCreatedShouldHaveKafkaListenerAnnotation() throws NoSuchMethodException {
        Method method = PaymentOrderConsumer.class.getMethod("onOrderCreated", OrderCreatedEvent.class);
        KafkaListener kafkaListener = method.getAnnotation(KafkaListener.class);

        assertThat(kafkaListener).isNotNull();
        assertThat(kafkaListener.topics()).containsExactly("order-events-topic");
        assertThat(kafkaListener.groupId()).isEqualTo("payment-service-group");
    }

    @Test
    @DisplayName("onOrderCreated method must have @Idempotent configured with key, group, ttl, and swallowDuplicate")
    void onOrderCreatedShouldHaveIdempotentAnnotation() throws NoSuchMethodException {
        Method method = PaymentOrderConsumer.class.getMethod("onOrderCreated", OrderCreatedEvent.class);
        Idempotent idempotent = method.getAnnotation(Idempotent.class);

        assertThat(idempotent).isNotNull();
        assertThat(idempotent.condition()).isEqualTo("#event != null");
        assertThat(idempotent.key()).isEqualTo("#event.orderId");
        assertThat(idempotent.consumerGroup()).isEqualTo("payment-service-group");
        assertThat(idempotent.ttl()).isEqualTo("7d");
        assertThat(idempotent.swallowDuplicate()).isTrue();
    }

    @Test
    @DisplayName("Senior Refinements v23: onOrderCreated should safely handle null event without NullPointerException")
    void onOrderCreatedShouldHandleNullEventSafely() {
        assertThatNoException().isThrownBy(() -> consumer.onOrderCreated(null));
    }
}
