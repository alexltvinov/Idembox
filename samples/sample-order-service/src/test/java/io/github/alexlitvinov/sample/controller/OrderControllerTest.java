package io.github.alexlitvinov.sample.controller;

import io.github.alexlitvinov.sample.service.OrderService;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.ConstraintViolationException;
import jakarta.validation.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.ResponseEntity;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@ExtendWith(MockitoExtension.class)
class OrderControllerTest {

    @Mock
    private OrderService orderService;

    @InjectMocks
    private OrderController orderController;

    private MockMvc mockMvc;

    @BeforeEach
    void setUp() {
        mockMvc = MockMvcBuilders.standaloneSetup(orderController).build();
    }

    @Test
    @DisplayName("Should create order successfully when parameters are valid")
    void shouldCreateOrderSuccessfully() throws Exception {
        when(orderService.createOrderDetailed(eq("cust-1"), eq(new BigDecimal("150.00"))))
                .thenReturn(new OrderService.OrderCreationResult("ORD-12345", true));

        mockMvc.perform(post("/api/orders")
                        .param("customerId", "cust-1")
                        .param("amount", "150.00"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("CREATED"))
                .andExpect(jsonPath("$.orderId").value("ORD-12345"))
                .andExpect(jsonPath("$.message").value("Order persisted and transactional Outbox event published to Kafka!"));
    }

    @Test
    @DisplayName("Senior Refinements v20: Should pass Idempotency-Key header to OrderService")
    void shouldPassIdempotencyKeyHeaderToOrderService() throws Exception {
        when(orderService.createOrderDetailed(eq("cust-1"), eq(new BigDecimal("150.00")), eq("client-key-42")))
                .thenReturn(new OrderService.OrderCreationResult("ORD-client-key-42", true));

        mockMvc.perform(post("/api/orders")
                        .param("customerId", "cust-1")
                        .param("amount", "150.00")
                        .header("Idempotency-Key", "client-key-42"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("CREATED"))
                .andExpect(jsonPath("$.orderId").value("ORD-client-key-42"))
                .andExpect(jsonPath("$.message").value("Order persisted and transactional Outbox event published to Kafka!"));
    }

    @Test
    @DisplayName("Senior Refinements Round 51: Should return status EXISTS when Idempotency-Key is duplicate")
    void shouldReturnStatusExistsWhenIdempotencyKeyIsDuplicate() throws Exception {
        when(orderService.createOrderDetailed(eq("cust-1"), eq(new BigDecimal("150.00")), eq("client-key-dup")))
                .thenReturn(new OrderService.OrderCreationResult("ORD-client-key-dup", false));

        mockMvc.perform(post("/api/orders")
                        .param("customerId", "cust-1")
                        .param("amount", "150.00")
                        .header("Idempotency-Key", "client-key-dup"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("EXISTS"))
                .andExpect(jsonPath("$.orderId").value("ORD-client-key-dup"))
                .andExpect(jsonPath("$.message").value("Order already exists, duplicate ignored"));
    }

    @Test
    @DisplayName("Senior Refinements v14: handleConstraintViolation returns HTTP 400 Bad Request with field details")
    void handleConstraintViolationShouldReturnBadRequestWithDetails() {
        @SuppressWarnings("unchecked")
        ConstraintViolation<Object> violation = mock(ConstraintViolation.class);
        Path path = mock(Path.class);
        when(path.toString()).thenReturn("createOrder.amount");
        when(violation.getPropertyPath()).thenReturn(path);
        when(violation.getMessage()).thenReturn("must be greater than 0");

        ConstraintViolationException ex = new ConstraintViolationException("Validation failed", Set.of(violation));

        ResponseEntity<Map<String, Object>> response = orderController.handleConstraintViolation(ex);

        assertThat(response.getStatusCode().value()).isEqualTo(400);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody()).containsEntry("status", "BAD_REQUEST");
        assertThat(response.getBody()).containsEntry("message", "Validation failed");

        @SuppressWarnings("unchecked")
        Map<String, String> errors = (Map<String, String>) response.getBody().get("errors");
        assertThat(errors).containsEntry("createOrder.amount", "must be greater than 0");
    }

    @Test
    @DisplayName("Senior Refinements v14: handleConstraintViolation handles empty violations gracefully")
    void handleConstraintViolationWithEmptyViolations() {
        ConstraintViolationException ex = new ConstraintViolationException("Validation failed", Collections.emptySet());

        ResponseEntity<Map<String, Object>> response = orderController.handleConstraintViolation(ex);

        assertThat(response.getStatusCode().value()).isEqualTo(400);
        assertThat(response.getBody()).isNotNull();
        assertThat(response.getBody()).containsEntry("status", "BAD_REQUEST");
    }

    @Test
    @DisplayName("Senior Refinements v22: Idempotency-Key parameter has @Size(max = 60) constraint")
    void idempotencyKeyParameterShouldHaveSizeConstraint() throws NoSuchMethodException {
        java.lang.reflect.Method method = OrderController.class.getMethod("createOrder", String.class, BigDecimal.class, String.class);
        java.lang.annotation.Annotation[] annotations = method.getParameterAnnotations()[2];
        jakarta.validation.constraints.Size sizeAnnotation = null;
        for (java.lang.annotation.Annotation ann : annotations) {
            if (ann instanceof jakarta.validation.constraints.Size size) {
                sizeAnnotation = size;
                break;
            }
        }
        assertThat(sizeAnnotation).isNotNull();
        assertThat(sizeAnnotation.max()).isEqualTo(60);
    }

    @Test
    @DisplayName("Senior Refinements Round 72 & Round 74: Should return HTTP 409 Conflict when Idempotency-Key is reused with different amount")
    void shouldReturnConflictWhenIdempotencyKeyReusedWithMismatchedParameters() throws Exception {
        when(orderService.createOrderDetailed(eq("cust-2"), eq(new BigDecimal("999.00")), eq("client-key-conflict")))
                .thenThrow(new OrderService.IdempotencyConflictException(
                        "ORD-client-key-conflict",
                        "Idempotency-Key conflict: key was already used with a different order amount"));

        mockMvc.perform(post("/api/orders")
                        .param("customerId", "cust-2")
                        .param("amount", "999.00")
                        .header("Idempotency-Key", "client-key-conflict"))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.status").value("CONFLICT"))
                .andExpect(jsonPath("$.message").value("Idempotency-Key conflict: key was already used with a different order amount"));
    }
}
