package io.github.alexlitvinov.sample.consumer;

import io.github.alexlitvinov.idembox.core.annotation.Idempotent;
import io.github.alexlitvinov.sample.event.OrderCreatedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class PaymentOrderConsumer {

    private static final Logger log = LoggerFactory.getLogger(PaymentOrderConsumer.class);

    /**
     * Consumes order-created events and processes payment.
     * The @Idempotent annotation automatically guards against duplicate deliveries (at-least-once).
     * If the same message arrives twice, subsequent invocations are intercepted and skipped.
     */
    @KafkaListener(topics = "order-events-topic", groupId = "payment-service-group")
    @Idempotent(
            condition = "#event != null",
            key = "#event.orderId",
            consumerGroup = "payment-service-group",
            ttl = "7d",
            swallowDuplicate = true
    )
    public void onOrderCreated(OrderCreatedEvent event) {
        if (event == null) {
            log.warn("Received null OrderCreatedEvent, skipping processing.");
            return;
        }
        log.info("Processing payment for order {} with amount {} {}", event.orderId(), event.amount(), event.currency());
        // Simulating business logic (e.g. charging card)
        log.info("Payment successfully processed for order {}", event.orderId());
    }
}
