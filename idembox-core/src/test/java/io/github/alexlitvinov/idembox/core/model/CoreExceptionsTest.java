package io.github.alexlitvinov.idembox.core.model;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CoreExceptionsTest {

    @Test
    @DisplayName("DuplicateMessageException should store eventKey and consumerGroup and format error message")
    void duplicateMessageExceptionShouldRetainDetails() {
        DuplicateMessageException ex = new DuplicateMessageException("ORDER-555", "order-processor-group");

        assertThat(ex).isInstanceOf(RuntimeException.class);
        assertThat(ex.getEventKey()).isEqualTo("ORDER-555");
        assertThat(ex.getConsumerGroup()).isEqualTo("order-processor-group");
        assertThat(ex.getMessage())
                .contains("Duplicate event detected")
                .contains("ORDER-555")
                .contains("order-processor-group");
    }

    @Test
    @DisplayName("ConcurrentProcessingException should store eventKey and consumerGroup and format in-flight message")
    void concurrentProcessingExceptionShouldRetainDetails() {
        ConcurrentProcessingException ex = new ConcurrentProcessingException("ORDER-777", "payment-group");

        assertThat(ex).isInstanceOf(RuntimeException.class);
        assertThat(ex.getEventKey()).isEqualTo("ORDER-777");
        assertThat(ex.getConsumerGroup()).isEqualTo("payment-group");
        assertThat(ex.getMessage())
                .contains("currently being processed by another worker")
                .contains("ORDER-777")
                .contains("payment-group");
    }
}
