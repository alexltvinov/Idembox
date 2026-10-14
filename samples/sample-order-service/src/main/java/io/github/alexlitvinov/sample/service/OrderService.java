package io.github.alexlitvinov.sample.service;

import io.github.alexlitvinov.idembox.core.publisher.OutboxPublisher;
import io.github.alexlitvinov.idembox.core.util.IdemboxUtils;
import io.github.alexlitvinov.sample.event.OrderCreatedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.validation.annotation.Validated;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.UUID;

@Validated
@Service
public class OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);

    static final String INSERT_ORDER_SQL = """
            INSERT INTO orders (id, customer_id, amount, status, created_at)
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT (id) DO NOTHING
            """;

    static final String SELECT_ORDER_SQL = "SELECT customer_id, amount FROM orders WHERE id = ?";

    public static class IdempotencyConflictException extends IllegalStateException {
        private final String orderId;

        public IdempotencyConflictException(String orderId, String message) {
            super(message);
            this.orderId = orderId;
        }

        public String getOrderId() {
            return orderId;
        }
    }

    private final OutboxPublisher outboxPublisher;
    private final JdbcTemplate jdbcTemplate;

    public OrderService(OutboxPublisher outboxPublisher, JdbcTemplate jdbcTemplate) {
        this.outboxPublisher = outboxPublisher;
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * Creates an order and enqueues event in Outbox within the SAME database transaction.
     * Dual-write guarantee: if the transaction rolls back, no message is ever sent to Kafka.
     */
    public record OrderCreationResult(String orderId, boolean created) {}

    @Transactional
    public OrderCreationResult createOrderDetailed(
            @NotBlank String customerId,
            @NotNull @Positive BigDecimal amount) {
        return createOrderDetailed(customerId, amount, null);
    }

    /**
     * Creates an order with customer-scoped HTTP ingress idempotency key support.
     * If an order with derived or provided (customerId, idempotencyKey) already exists, verifies that customerId and amount
     * match the original request and returns existing orderId with created=false to prevent duplicate billing
     * and duplicate Outbox event dispatch on client network retries.
     * Throws {@link IdempotencyConflictException} if the key is reused by the same customer with a different amount.
     */
    @Transactional
    public OrderCreationResult createOrderDetailed(
            @NotBlank String customerId,
            @NotNull @Positive BigDecimal amount,
            String idempotencyKey) {

        String cleanCustomerId = sanitizeCustomerId(customerId);
        String orderId = normalizeOrderId(cleanCustomerId, idempotencyKey);
        if (idempotencyKey != null && !idempotencyKey.isBlank()) {
            if (verifyExistingOrderIfPresent(orderId, cleanCustomerId, amount, idempotencyKey)) {
                log.info("Order {} already exists for customerId='{}', idempotencyKey='{}' with matching parameters. Returning existing order without duplicate Outbox publication.",
                        orderId, cleanCustomerId, idempotencyKey);
                return new OrderCreationResult(orderId, false);
            }
        }

        // 1. Save order to real business table with concurrent race condition guard (ON CONFLICT DO NOTHING prevents transaction abort)
        int affected = jdbcTemplate.update(
                INSERT_ORDER_SQL,
                orderId, cleanCustomerId, amount, "CREATED"
        );
        if (affected == 0) {
            verifyExistingOrderIfPresent(orderId, cleanCustomerId, amount, idempotencyKey);
            log.info("Concurrent insert conflict detected for orderId={}. Returning existing order without duplicate Outbox publication.", orderId);
            return new OrderCreationResult(orderId, false);
        }
        log.info("Persisted order {} for customer {} with amount {}", orderId, cleanCustomerId, amount);

        // 2. Publish outbox event transactionally
        OrderCreatedEvent event = new OrderCreatedEvent(orderId, cleanCustomerId, amount, "RUB");
        outboxPublisher.publish(
                "order-events-topic",
                orderId, // Kafka partition key
                event,
                Map.of("source", "sample-order-service", "version", "1.0")
        );

        log.info("Enqueued Outbox event for order {}", orderId);
        return new OrderCreationResult(orderId, true);
    }

    private boolean verifyExistingOrderIfPresent(
            String orderId,
            String customerId,
            BigDecimal amount,
            String idempotencyKey) {
        java.util.List<Map<String, Object>> rows = jdbcTemplate.queryForList(SELECT_ORDER_SQL, orderId);
        if (rows == null || rows.isEmpty()) {
            return false;
        }
        Map<String, Object> existing = rows.get(0);
        if (existing != null) {
            Object existingCustomerObj = existing.get("customer_id");
            String existingCustomer = existingCustomerObj != null ? sanitizeCustomerId(existingCustomerObj.toString()) : null;
            String expectedCustomer = sanitizeCustomerId(customerId);
            if (existingCustomer != null && !existingCustomer.equals(expectedCustomer)) {
                log.warn("Idempotency-Key conflict for orderId={}: existing customer_id='{}' does not match request customerId='{}'",
                        orderId, existingCustomerObj, customerId);
                throw new IdempotencyConflictException(
                        orderId,
                        "Idempotency-Key conflict: key was already used with different request parameters");
            }
            Object existingAmountObj = existing.get("amount");
            BigDecimal existingAmount = toBigDecimal(existingAmountObj);
            if (existingAmount != null && amount != null && existingAmount.compareTo(amount) != 0) {
                log.warn("Idempotency-Key conflict for orderId={}: existing amount={} does not match request amount={}",
                        orderId, existingAmount, amount);
                throw new IdempotencyConflictException(
                        orderId,
                        "Idempotency-Key conflict: key was already used with a different order amount");
            }
        }
        return true;
    }

    private static String sanitizeCustomerId(String customerId) {
        if (customerId == null) {
            return null;
        }
        return customerId.replace("\\u0000", "").replace("\0", "").trim();
    }

    private static BigDecimal toBigDecimal(Object val) {
        if (val == null) {
            return null;
        }
        if (val instanceof BigDecimal bd) {
            return bd;
        }
        if (val instanceof Number num) {
            return new BigDecimal(num.toString());
        }
        return new BigDecimal(val.toString());
    }

    @Transactional
    public String createOrder(
            @NotBlank String customerId,
            @NotNull @Positive BigDecimal amount) {
        return createOrderDetailed(customerId, amount, null).orderId();
    }

    @Transactional
    public String createOrder(
            @NotBlank String customerId,
            @NotNull @Positive BigDecimal amount,
            String idempotencyKey) {
        return createOrderDetailed(customerId, amount, idempotencyKey).orderId();
    }

    static String normalizeOrderId(String idempotencyKey) {
        return normalizeOrderId(null, idempotencyKey);
    }

    static String normalizeOrderId(String customerId, String idempotencyKey) {
        if (idempotencyKey == null || idempotencyKey.isBlank()) {
            return "ORD-" + UUID.randomUUID();
        }
        String sanitized = idempotencyKey.replace("\\u0000", "").replace("\0", "").trim();
        if (sanitized.isEmpty()) {
            return "ORD-" + UUID.randomUUID();
        }
        if (customerId != null && !customerId.isBlank()) {
            String sanitizedCustomer = sanitizeCustomerId(customerId);
            if (sanitizedCustomer != null && !sanitizedCustomer.isEmpty()) {
                String scopedInput = sanitizedCustomer.length() + ":" + sanitizedCustomer + ":" + sanitized;
                return "ORD-" + IdemboxUtils.sha256Hex(scopedInput.getBytes(StandardCharsets.UTF_8)).substring(0, 60);
            }
        }
        if (sanitized.length() <= 60) {
            return "ORD-" + sanitized;
        }
        // Normalize overlong key to ensure total length never exceeds VARCHAR(64)
        return "ORD-" + IdemboxUtils.sha256Hex(sanitized.getBytes(StandardCharsets.UTF_8)).substring(0, 60);
    }
}
