package io.github.alexlitvinov.idembox.starter.test;

import io.github.alexlitvinov.idembox.starter.test.service.SamplePaymentService;
import io.github.alexlitvinov.idembox.starter.test.service.SamplePaymentService.PaymentRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PostgresIdempotencyIntegrationTest extends AbstractIntegrationTest {

    @Autowired
    private SamplePaymentService paymentService;

    @BeforeEach
    void setUp() {
        paymentService.reset();
    }

    @Test
    @DisplayName("Sequential duplicates: Second call with identical key should be safely skipped")
    void shouldExecuteOnceOnDuplicateSequentialCalls() {
        PaymentRequest request = new PaymentRequest("ORD-SEQ-001", 1500.0);

        String firstResult = paymentService.processPayment(request);
        assertThat(firstResult).isEqualTo("SUCCESS-ORD-SEQ-001");
        assertThat(paymentService.getExecutionCount()).isEqualTo(1);

        // Second call with same orderId (duplicate delivery)
        String secondResult = paymentService.processPayment(request);
        assertThat(secondResult).isNull(); // swallowed duplicate
        assertThat(paymentService.getExecutionCount()).isEqualTo(1);
    }

    @Test
    @DisplayName("Concurrent race condition: 10 parallel threads with same key must execute target logic exactly ONCE")
    void shouldDeduplicateConcurrentCallsSafely() throws InterruptedException {
        int threadsCount = 10;
        PaymentRequest request = new PaymentRequest("ORD-CONCURRENT-999", 5000.0);

        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch finishLatch = new CountDownLatch(threadsCount);

        for (int i = 0; i < threadsCount; i++) {
            new Thread(() -> {
                try {
                    startLatch.await(); // wait for simultaneous launch
                    paymentService.processPayment(request);
                } catch (Exception ignored) {
                } finally {
                    finishLatch.countDown();
                }
            }, "concurrent-idem-test-" + i).start();
        }

        startLatch.countDown(); // trigger all threads at once
        boolean completedInTime = finishLatch.await(10, TimeUnit.SECONDS);

        assertThat(completedInTime).isTrue();
        assertThat(paymentService.getExecutionCount())
                .as("Business logic must have executed strictly once despite concurrent arrivals")
                .isEqualTo(1);
    }

    @Test
    @DisplayName("Failure rollback: Failed execution must release key so Kafka retries can succeed")
    void shouldReleaseLockOnFailureAllowingRetry() {
        PaymentRequest request = new PaymentRequest("ORD-FAIL-001", 300.0);

        // 1. First execution fails with exception
        assertThatThrownBy(() -> paymentService.processWithTransientFailure(request, true))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Simulated payment gateway timeout");

        assertThat(paymentService.getExecutionCount()).isEqualTo(0);

        // 2. Retry with the same key succeeds
        paymentService.processWithTransientFailure(request, false);
        assertThat(paymentService.getExecutionCount()).isEqualTo(1);
    }
}
