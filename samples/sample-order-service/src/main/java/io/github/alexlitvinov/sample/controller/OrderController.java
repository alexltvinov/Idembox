package io.github.alexlitvinov.sample.controller;

import io.github.alexlitvinov.sample.service.OrderService;
import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.util.Map;

@RestController
@RequestMapping("/api/orders")
@Validated
public class OrderController {

    private final OrderService orderService;

    public OrderController(OrderService orderService) {
        this.orderService = orderService;
    }

    /**
     * Endpoint to manually trigger an Outbox event.
     * Example: {@code POST http://localhost:8080/api/orders?customerId=user-42&amount=1500}
     */
    @PostMapping
    public ResponseEntity<Map<String, Object>> createOrder(
            @RequestParam(defaultValue = "customer-1") @NotBlank @Size(min = 1, max = 64) String customerId,
            @RequestParam(defaultValue = "100.00") @NotNull @Positive @DecimalMax("9999999999.99") BigDecimal amount,
            @org.springframework.web.bind.annotation.RequestHeader(value = "Idempotency-Key", required = false)
            @Size(max = 60, message = "Idempotency-Key must not exceed 60 characters") String idempotencyKey) {

        OrderService.OrderCreationResult result = (idempotencyKey != null && !idempotencyKey.isBlank())
                ? orderService.createOrderDetailed(customerId, amount, idempotencyKey)
                : orderService.createOrderDetailed(customerId, amount);

        if (!result.created()) {
            return ResponseEntity.ok(Map.of(
                    "status", "EXISTS",
                    "orderId", result.orderId(),
                    "message", "Order already exists, duplicate ignored"
            ));
        }

        return ResponseEntity.ok(Map.of(
                "status", "CREATED",
                "orderId", result.orderId(),
                "message", "Order persisted and transactional Outbox event published to Kafka!"
        ));
    }

    @org.springframework.web.bind.annotation.ExceptionHandler(jakarta.validation.ConstraintViolationException.class)
    public ResponseEntity<Map<String, Object>> handleConstraintViolation(jakarta.validation.ConstraintViolationException ex) {
        Map<String, String> errors = new java.util.LinkedHashMap<>();
        for (jakarta.validation.ConstraintViolation<?> violation : ex.getConstraintViolations()) {
            String property = violation.getPropertyPath() != null ? violation.getPropertyPath().toString() : "param";
            errors.put(property, violation.getMessage());
        }
        return ResponseEntity.badRequest().body(Map.of(
                "status", "BAD_REQUEST",
                "message", "Validation failed",
                "errors", errors
        ));
    }

    @org.springframework.web.bind.annotation.ExceptionHandler(OrderService.IdempotencyConflictException.class)
    public ResponseEntity<Map<String, Object>> handleIdempotencyConflict(OrderService.IdempotencyConflictException ex) {
        return ResponseEntity.status(org.springframework.http.HttpStatus.CONFLICT).body(Map.of(
                "status", "CONFLICT",
                "message", ex.getMessage() != null ? ex.getMessage() : "Idempotency-Key conflict with different request parameters"
        ));
    }
}
