package io.github.alexlitvinov.idembox.core.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a consumer method (e.g. {@code @KafkaListener}) for idempotent execution.
 * <p>
 * Example usage:
 * <pre>{@code
 * @KafkaListener(topics = "order-events", groupId = "order-processing-group")
 * @Idempotent(
 *     key = "#root.args[0].orderId",
 *     consumerGroup = "order-processing-group",
 *     ttl = "7d",
 *     lockTimeout = "2m"
 * )
 * public void handleOrderCreated(OrderCreatedEvent event) {
 *     // Executed at most once for given event.id
 * }
 * }</pre>
 * <p>
 * <b>Important Spring AOP Limitation (Self-Invocation):</b>
 * Calls to an {@code @Idempotent} method from another method within the same bean (e.g. {@code this.handleOrderCreated(event)})
 * bypass the Spring AOP proxy mechanism. As a result, the idempotency aspect will <b>NOT</b> be executed.
 * To ensure idempotency is applied, invoke the method from an external Spring bean, or inject the bean into itself (self-injection).
 * <p>
 * <b>Single-Record Consumer Scope:</b>
 * This annotation is designed exclusively for Single-Record Listeners. For batch listeners (e.g. {@code List<OrderCreatedEvent>}),
 * do not annotate the batch listener directly. Instead, iterate over the batch and invoke an {@code @Idempotent} method on a
 * delegated Spring service for each individual event.
 * <p>
 * <b>Return Types with {@code swallowDuplicate = true}:</b>
 * Swallowing duplicates is intended for {@code void} listener methods, or methods returning container types ({@link java.util.Optional},
 * {@link java.util.concurrent.CompletableFuture}, {@link java.util.Collection}, {@link java.util.Map}). If applied to methods with domain
 * return types (e.g. {@code PaymentResult}), a swallowed duplicate returns {@code null}, which may lead to {@link NullPointerException}
 * in synchronous callers. For methods returning business domain objects, configure {@code swallowDuplicate = false} and catch
 * {@link io.github.alexlitvinov.idembox.core.model.DuplicateMessageException}, or return {@link java.util.Optional}.
 */
@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface Idempotent {

    /**
     * Spring Expression Language (SpEL) expression defining whether idempotency check should be applied.
     * Defaults to "" (always applied). If evaluates to false, idempotency check is skipped
     * and method proceeds directly without locking or deduplication.
     */
    String condition() default "";

    /**
     * Spring Expression Language (SpEL) expression to extract unique deduplication key from method parameters.
     * Examples: {@code "#event.orderId"}, {@code "#root.args[0].id"}, {@code "#p0.key"}.
     */
    String key();

    /**
     * Logical consumer group or boundary name. Allows multiple distinct consumer groups to each process the event once.
     * If left blank, the aspect automatically derives the consumer group (first querying the active Kafka consumer group
     * via {@code KafkaUtils.getConsumerGroupId()}, or falling back to the target method signature {@code DeclaringClass#methodName})
     * to prevent independent consumers from unintentionally muting each other.
     * <p>
     * <b>Note on Expression Support:</b>
     * Supports Spring property placeholder resolution (e.g. {@code "${app.kafka.consumer-group}"}),
     * but does <b>not</b> support SpEL evaluation of method arguments (e.g. {@code "#event.group"}).
     * If parameter-dependent deduplication partitioning is needed, include it in the {@link #key()} expression.
     */
    String consumerGroup() default "";

    /**
     * Overall business retention window for deduplication history (after successful completion).
     * Formats: "7d", "24h", "30m", "10s", or ISO-8601 duration "PT24H".
     * If left blank, falls back to {@code idembox.idempotency.default-ttl} (default "7d").
     */
    String ttl() default "";

    /**
     * Maximum duration a worker can hold an in-flight lock during processing.
     * If a worker crashes or dies, another node can safely take over and retry the message after this duration.
     * If left blank, falls back to {@code idembox.idempotency.default-lock-timeout} (default "2m").
     */
    String lockTimeout() default "";

    /**
     * Whether to quietly swallow completed duplicates (default {@code true}) and acknowledge the message,
     * or throw {@code DuplicateMessageException} if set to {@code false}.
     */
    boolean swallowDuplicate() default true;

    /**
     * Whether to permit retrying previously permanently failed events (status FAILED, e.g. Dead Letter Queue replays).
     * Defaults to {@code false} (failed events are treated as duplicate failures).
     * If {@code true}, or if Kafka header {@code X-Idempotent-Replay: true} is present, the failed status is reset
     * and the event is allowed to re-execute.
     */
    boolean allowFailedRetry() default false;

    /**
     * Exception types that are considered fatal/terminal (unrecoverable, e.g. poison pills, validation errors).
     * When any of these exceptions (or their subclasses) are thrown, the idempotent record is marked as FAILED (terminal),
     * preventing infinite poison pill retries when {@link #allowFailedRetry()} is {@code false}.
     */
    Class<? extends Throwable>[] terminalExceptions() default {};

    /**
     * Exception types that are retryable. If specified and non-empty, any exception not matching retryableExceptions
     * (or matching terminalExceptions) will be treated as fatal/terminal.
     */
    Class<? extends Throwable>[] retryableExceptions() default {};
}

