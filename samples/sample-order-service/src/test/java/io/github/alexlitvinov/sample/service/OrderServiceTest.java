package io.github.alexlitvinov.sample.service;

import io.github.alexlitvinov.idembox.core.publisher.OutboxPublisher;
import io.github.alexlitvinov.sample.event.OrderCreatedEvent;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.jdbc.core.JdbcTemplate;

import java.math.BigDecimal;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OrderServiceTest {

    @Mock
    private OutboxPublisher outboxPublisher;

    @Mock
    private JdbcTemplate jdbcTemplate;

    @InjectMocks
    private OrderService orderService;

    @Test
    @DisplayName("Fix 8: createOrder persists order in business table and publishes Outbox message within same transaction")
    void shouldPersistOrderAndPublishOutboxEvent() {
        String customerId = "CUST-99";
        BigDecimal amount = new BigDecimal("1250.50");

        when(jdbcTemplate.update(
                eq(OrderService.INSERT_ORDER_SQL),
                anyString(), eq(customerId), eq(amount), eq("CREATED")
        )).thenReturn(1);

        String orderId = orderService.createOrder(customerId, amount);

        assertThat(orderId).startsWith("ORD-");

        // 1. Verify business entity inserted into 'orders' table
        verify(jdbcTemplate).update(
                eq(OrderService.INSERT_ORDER_SQL),
                eq(orderId),
                eq(customerId),
                eq(amount),
                eq("CREATED")
        );

        // 2. Verify Outbox event published
        ArgumentCaptor<OrderCreatedEvent> eventCaptor = ArgumentCaptor.forClass(OrderCreatedEvent.class);
        verify(outboxPublisher).publish(
                eq("order-events-topic"),
                eq(orderId),
                eventCaptor.capture(),
                any()
        );

        OrderCreatedEvent event = eventCaptor.getValue();
        assertThat(event.orderId()).isEqualTo(orderId);
        assertThat(event.customerId()).isEqualTo(customerId);
        assertThat(event.amount()).isEqualTo(amount);
        assertThat(event.currency()).isEqualTo("RUB");
    }

    @Test
    @DisplayName("Fix 6: OrderCreatedEvent should normalize BigDecimal scale to 2 so equals works across sources")
    void shouldNormalizeBigDecimalScaleInOrderCreatedEvent() {
        OrderCreatedEvent event1 = new OrderCreatedEvent("ORD-1", "CUST-1", new BigDecimal("100.0"), "RUB");
        OrderCreatedEvent event2 = new OrderCreatedEvent("ORD-1", "CUST-1", new BigDecimal("100.00"), "RUB");
        OrderCreatedEvent event3 = new OrderCreatedEvent("ORD-1", "CUST-1", new BigDecimal("100.000"), "RUB");

        assertThat(event1).isEqualTo(event2);
        assertThat(event2).isEqualTo(event3);
        assertThat(event1.amount().scale()).isEqualTo(2);
    }

    @Test
    @DisplayName("Defense in Depth: createOrder parameters should have @NotBlank, @NotNull, and @Positive constraints")
    void shouldHaveValidationConstraintsOnCreateOrderParameters() throws NoSuchMethodException {
        jakarta.validation.ValidatorFactory factory = jakarta.validation.Validation.buildDefaultValidatorFactory();
        jakarta.validation.executable.ExecutableValidator executableValidator = factory.getValidator().forExecutables();

        java.lang.reflect.Method method = OrderService.class.getMethod("createOrder", String.class, BigDecimal.class);

        // 1. Invalid blank customerId and negative amount
        Object[] invalidArgs = new Object[]{"   ", new BigDecimal("-10.00")};
        java.util.Set<jakarta.validation.ConstraintViolation<OrderService>> violations =
                executableValidator.validateParameters(orderService, method, invalidArgs);

        assertThat(violations).hasSize(2);

        // 2. Null amount
        Object[] nullAmountArgs = new Object[]{"CUST-1", null};
        java.util.Set<jakarta.validation.ConstraintViolation<OrderService>> nullViolations =
                executableValidator.validateParameters(orderService, method, nullAmountArgs);

        assertThat(nullViolations).hasSize(1);

        // 3. Valid parameters
        Object[] validArgs = new Object[]{"CUST-1", new BigDecimal("100.00")};
        java.util.Set<jakarta.validation.ConstraintViolation<OrderService>> validViolations =
                executableValidator.validateParameters(orderService, method, validArgs);

        assertThat(validViolations).isEmpty();
    }

    @Test
    @DisplayName("Senior Refinements v20 & Round 74: createOrder with existing idempotencyKey and matching parameters returns existing customer-scoped orderId without duplicate insert or outbox event")
    void shouldDeduplicateOrderWhenIdempotencyKeyAlreadyExists() {
        String customerId = "CUST-10";
        BigDecimal amount = new BigDecimal("500.00");
        String idempotencyKey = "client-txn-999";
        String expectedOrderId = OrderService.normalizeOrderId(customerId, idempotencyKey);

        when(jdbcTemplate.queryForList(eq(OrderService.SELECT_ORDER_SQL), eq(expectedOrderId)))
                .thenReturn(java.util.List.of(Map.of("customer_id", customerId, "amount", amount)));

        String orderId = orderService.createOrder(customerId, amount, idempotencyKey);

        assertThat(orderId).isEqualTo(expectedOrderId);
        // Business table insert and outbox publication MUST be skipped for duplicate HTTP request!
        verify(jdbcTemplate, org.mockito.Mockito.never()).update(anyString(), any(), any(), any(), any());
        verify(outboxPublisher, org.mockito.Mockito.never()).publish(anyString(), anyString(), any(), any());
    }

    @Test
    @DisplayName("Senior Refinements v20 & Round 74: createOrder with new idempotencyKey creates order with derived customer-scoped orderId and publishes outbox event")
    void shouldCreateNewOrderWhenIdempotencyKeyDoesNotExist() {
        String customerId = "CUST-10";
        BigDecimal amount = new BigDecimal("500.00");
        String idempotencyKey = "client-txn-1000";
        String expectedOrderId = OrderService.normalizeOrderId(customerId, idempotencyKey);

        when(jdbcTemplate.queryForList(eq(OrderService.SELECT_ORDER_SQL), eq(expectedOrderId)))
                .thenReturn(java.util.Collections.emptyList());
        when(jdbcTemplate.update(
                eq(OrderService.INSERT_ORDER_SQL),
                eq(expectedOrderId), eq(customerId), eq(amount), eq("CREATED")
        )).thenReturn(1);

        String orderId = orderService.createOrder(customerId, amount, idempotencyKey);

        assertThat(orderId).isEqualTo(expectedOrderId);
        verify(jdbcTemplate).update(
                eq(OrderService.INSERT_ORDER_SQL),
                eq(expectedOrderId),
                eq(customerId),
                eq(amount),
                eq("CREATED")
        );
        verify(outboxPublisher).publish(
                eq("order-events-topic"),
                eq(expectedOrderId),
                any(OrderCreatedEvent.class),
                any()
        );
    }

    @Test
    @DisplayName("Senior Refinements v23 & Round 74: Concurrent HTTP insert hitting ON CONFLICT DO NOTHING (0 rows affected) safely returns orderId without Outbox duplicate")
    void shouldHandleConcurrentInsertConflictWithoutPublishingOutbox() {
        String customerId = "CUST-RACE";
        BigDecimal amount = new BigDecimal("250.00");
        String idempotencyKey = "race-txn-555";
        String expectedOrderId = OrderService.normalizeOrderId(customerId, idempotencyKey);

        // Initial check returned empty (race condition window), post-conflict check returns matching order
        when(jdbcTemplate.queryForList(eq(OrderService.SELECT_ORDER_SQL), eq(expectedOrderId)))
                .thenReturn(java.util.Collections.emptyList())
                .thenReturn(java.util.List.of(Map.of("customer_id", customerId, "amount", amount)));

        // Concurrent thread already inserted row -> ON CONFLICT DO NOTHING returns 0 affected rows
        when(jdbcTemplate.update(
                eq(OrderService.INSERT_ORDER_SQL),
                eq(expectedOrderId), eq(customerId), eq(amount), eq("CREATED")
        )).thenReturn(0);

        String orderId = orderService.createOrder(customerId, amount, idempotencyKey);

        assertThat(orderId).isEqualTo(expectedOrderId);
        // Outbox must NOT be published again for duplicate insert
        org.mockito.Mockito.verify(outboxPublisher, org.mockito.Mockito.never()).publish(anyString(), anyString(), any(), any());
    }

    @Test
    @DisplayName("Senior Refinements Round 74: Same Idempotency-Key across different customerIds produces isolated non-colliding orderIds without Cross-Tenant DoS")
    void shouldIsolateIdempotencyKeyAcrossDifferentCustomersWithoutCollision() {
        String sharedIdempotencyKey = "req-001";
        BigDecimal amount = new BigDecimal("100.00");

        String orderIdCustomerA = OrderService.normalizeOrderId("customer-A", sharedIdempotencyKey);
        String orderIdCustomerB = OrderService.normalizeOrderId("customer-B", sharedIdempotencyKey);

        assertThat(orderIdCustomerA).startsWith("ORD-").hasSize(64);
        assertThat(orderIdCustomerB).startsWith("ORD-").hasSize(64);
        assertThat(orderIdCustomerA).isNotEqualTo(orderIdCustomerB);

        when(jdbcTemplate.queryForList(eq(OrderService.SELECT_ORDER_SQL), anyString()))
                .thenReturn(java.util.Collections.emptyList());
        when(jdbcTemplate.update(eq(OrderService.INSERT_ORDER_SQL), anyString(), anyString(), eq(amount), eq("CREATED")))
                .thenReturn(1);

        OrderService.OrderCreationResult resultA = orderService.createOrderDetailed("customer-A", amount, sharedIdempotencyKey);
        OrderService.OrderCreationResult resultB = orderService.createOrderDetailed("customer-B", amount, sharedIdempotencyKey);

        assertThat(resultA.created()).isTrue();
        assertThat(resultA.orderId()).isEqualTo(orderIdCustomerA);
        assertThat(resultB.created()).isTrue();
        assertThat(resultB.orderId()).isEqualTo(orderIdCustomerB);
    }

    @Test
    @DisplayName("Senior Refinements Round 72 & Round 74: Reusing Idempotency-Key by the same customer with a different amount throws IdempotencyConflictException")
    void shouldThrowConflictWhenIdempotencyKeyReusedWithDifferentAmount() {
        String idempotencyKey = "tampered-amount-key";
        String expectedOrderId = OrderService.normalizeOrderId("customer-A", idempotencyKey);

        when(jdbcTemplate.queryForList(eq(OrderService.SELECT_ORDER_SQL), eq(expectedOrderId)))
                .thenReturn(java.util.List.of(Map.of("customer_id", "customer-A", "amount", new BigDecimal("100.00"))));

        org.assertj.core.api.Assertions.assertThatThrownBy(() ->
                orderService.createOrderDetailed("customer-A", new BigDecimal("10000.00"), idempotencyKey))
                .isInstanceOf(OrderService.IdempotencyConflictException.class)
                .hasMessageContaining("different order amount");

        verify(jdbcTemplate, org.mockito.Mockito.never()).update(anyString(), any(), any(), any(), any());
        verify(outboxPublisher, org.mockito.Mockito.never()).publish(anyString(), anyString(), any(), any());
    }

    @Test
    @DisplayName("Senior Refinements v22: Overlong idempotency key (> 60 chars) is normalized to fit VARCHAR(64) limit")
    void shouldNormalizeOverlongIdempotencyKeyToFitVarchar64() {
        String customerId = "CUST-OVERLONG";
        BigDecimal amount = new BigDecimal("75.00");
        String overlongKey = "a".repeat(128); // 128 characters

        String orderId = orderService.createOrder(customerId, amount, overlongKey);

        assertThat(orderId)
                .startsWith("ORD-")
                .hasSizeLessThanOrEqualTo(64);
    }

    @Test
    @DisplayName("Senior Refinements v25: normalizeOrderId strips null bytes and generates random UUID if blank after sanitization")
    void shouldSanitizeNullBytesInNormalizeOrderId() {
        assertThat(OrderService.normalizeOrderId("key-with-\0-null")).isEqualTo("ORD-key-with--null");
        assertThat(OrderService.normalizeOrderId("key-with-\\u0000-null")).isEqualTo("ORD-key-with--null");
        assertThat(OrderService.normalizeOrderId("\0\0\0")).startsWith("ORD-");
        assertThat(OrderService.normalizeOrderId("\\u0000\\u0000")).startsWith("ORD-");
        assertThat(OrderService.normalizeOrderId("   ")).startsWith("ORD-");
        assertThat(OrderService.normalizeOrderId(null)).startsWith("ORD-");
    }

    @Test
    @DisplayName("Senior Refinements Round 75: normalizeOrderId prevents colon delimiter collision and createOrderDetailed normalizes customerId whitespace")
    void shouldPreventColonDelimiterCollisionAndNormalizeCustomerIdWhitespace() {
        // 1. Colon delimiter collision prevention: ("tenant:1", "2") vs ("tenant", "1:2")
        String id1 = OrderService.normalizeOrderId("tenant:1", "2");
        String id2 = OrderService.normalizeOrderId("tenant", "1:2");
        assertThat(id1).isNotEqualTo(id2);

        // 2. Consistent customerId trimming on first request and retry prevents false 409 Conflict
        String rawCustomerId = "  CUST-TRIM-1  ";
        String cleanCustomerId = "CUST-TRIM-1";
        BigDecimal amount = new BigDecimal("200.00");
        String idempotencyKey = "idem-trim-key";
        String expectedOrderId = OrderService.normalizeOrderId(cleanCustomerId, idempotencyKey);

        // First call inserts cleanCustomerId ("CUST-TRIM-1"); retry passes rawCustomerId ("  CUST-TRIM-1  ")
        when(jdbcTemplate.queryForList(eq(OrderService.SELECT_ORDER_SQL), eq(expectedOrderId)))
                .thenReturn(java.util.List.of(Map.of("customer_id", cleanCustomerId, "amount", amount)));

        OrderService.OrderCreationResult retryResult =
                orderService.createOrderDetailed(rawCustomerId, amount, idempotencyKey);

        assertThat(retryResult.orderId()).isEqualTo(expectedOrderId);
        assertThat(retryResult.created()).isFalse();
    }
}

