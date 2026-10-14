package io.github.alexlitvinov.idembox.starter.test.service;

import io.github.alexlitvinov.idembox.core.publisher.OutboxPublisher;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Map;
import java.util.UUID;

@Service
public class TransactionalOrderService {

    private final OutboxPublisher outboxPublisher;

    public TransactionalOrderService(OutboxPublisher outboxPublisher) {
        this.outboxPublisher = outboxPublisher;
    }

    @Transactional
    public UUID placeOrderSuccessfully(String orderId) {
        return outboxPublisher.publish(
                "orders-topic",
                orderId,
                Map.of("orderId", orderId, "status", "NEW")
        );
    }

    @Transactional
    public void placeOrderWithRollback(String orderId) {
        outboxPublisher.publish(
                "orders-topic",
                orderId,
                Map.of("orderId", orderId, "status", "NEW")
        );
        throw new IllegalStateException("Business validation failed, transaction must rollback!");
    }
}
