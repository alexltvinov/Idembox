package io.github.alexlitvinov.sample.event;

import java.math.BigDecimal;
import java.math.RoundingMode;

public record OrderCreatedEvent(
        String orderId,
        String customerId,
        BigDecimal amount,
        String currency
) {
    public OrderCreatedEvent {
        if (amount != null) {
            amount = amount.setScale(2, RoundingMode.HALF_UP);
        }
    }
}
