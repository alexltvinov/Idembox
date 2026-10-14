package io.github.alexlitvinov.idembox.starter.test.service;

import io.github.alexlitvinov.idembox.core.annotation.Idempotent;
import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicInteger;

@Service
public class SamplePaymentService {

    private final AtomicInteger executionCount = new AtomicInteger(0);

    public record PaymentRequest(String orderId, double amount) {}

    @Idempotent(key = "#request.orderId", consumerGroup = "payment-group", ttl = "1d")
    public String processPayment(PaymentRequest request) {
        executionCount.incrementAndGet();
        return "SUCCESS-" + request.orderId();
    }

    @Idempotent(key = "#request.orderId", consumerGroup = "failing-group", ttl = "1d")
    public void processWithTransientFailure(PaymentRequest request, boolean shouldFail) {
        if (shouldFail) {
            throw new RuntimeException("Simulated payment gateway timeout: " + request.orderId());
        }
        executionCount.incrementAndGet();
    }

    public int getExecutionCount() {
        return executionCount.get();
    }

    public void reset() {
        executionCount.set(0);
    }
}
