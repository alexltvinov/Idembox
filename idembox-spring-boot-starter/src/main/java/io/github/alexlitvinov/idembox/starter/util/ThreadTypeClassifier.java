package io.github.alexlitvinov.idembox.starter.util;

import java.lang.reflect.Method;
import java.util.regex.Pattern;

/**
 * Utility for robust thread classification (Kafka I/O network threads,
 * web container request threads, and non-blocking / event loop threads).
 * <p>
 * Replaces fragile substring heuristics (such as {@code name.contains("kafka")} or {@code name.contains("web")})
 * with exact type inspection, Spring Web request context checks, and strict thread name patterns.
 */
public final class ThreadTypeClassifier {

    private static final Pattern KAFKA_NETWORK_THREAD_PATTERN = Pattern.compile(
            "^(?:kafka-producer-network-thread|kafka-coordinator-heartbeat-thread|kafka-consumer-network-thread)(?:\\b|\\s*\\|).*",
            Pattern.CASE_INSENSITIVE
    );

    private static final Pattern WEB_CONTAINER_THREAD_PATTERN = Pattern.compile(
            "^(?:" +
                    "http-(?:nio|apr|bio)(?:-\\d+)?-exec-\\d+.*|" +
                    "https-(?:nio|apr|bio)(?:-\\d+)?-exec-\\d+.*|" +
                    "catalina-exec-\\d+.*|" +
                    "(?:jetty-)?qtp\\d+-\\d+.*|" +
                    "(?:default|undertow|xnio)(?:[-_ ]\\d+)?[-_ ]task[-_ ]\\d+.*|" +
                    "grizzly-(?:http-server|kernel|worker)-\\d+.*|" +
                    "reactor-http-(?:nio|epoll|kqueue)-\\d+.*" +
            ")$",
            Pattern.CASE_INSENSITIVE
    );

    private static final Pattern EVENT_LOOP_THREAD_PATTERN = Pattern.compile(
            ".*(?:eventloopgroup|event-loop|eventloop|reactor-http|reactor-tcp).*",
            Pattern.CASE_INSENSITIVE
    );

    private ThreadTypeClassifier() {
    }

    /**
     * Determines whether the given thread is an internal Kafka network I/O thread
     * (e.g. {@code KafkaProducer} network sender or coordinator heartbeat thread).
     * <p>
     * Note: Standard {@code @KafkaListener} consumer threads are worker threads and will return {@code false}.
     */
    public static boolean isKafkaNetworkThread(Thread thread) {
        if (thread == null) {
            return false;
        }
        // Direct type check: Kafka client network threads inherit from KafkaThread
        if (thread instanceof org.apache.kafka.common.utils.KafkaThread) {
            return true;
        }
        String name = thread.getName();
        if (name == null || name.isBlank()) {
            return false;
        }
        return KAFKA_NETWORK_THREAD_PATTERN.matcher(name).matches()
                || name.contains("kafka-producer-network-thread")
                || name.contains("kafka-coordinator-heartbeat-thread")
                || name.contains("kafka-consumer-network-thread");
    }

    /**
     * Determines whether the given thread is a web container request handling thread
     * (Tomcat, Jetty, Undertow, Grizzly) or is currently executing an active HTTP web request.
     */
    public static boolean isWebContainerThread(Thread thread) {
        if (thread == null) {
            return false;
        }
        // If the thread is executing an active Spring Web HTTP request
        if (thread == Thread.currentThread() && hasActiveSpringWebRequest()) {
            return true;
        }
        String name = thread.getName();
        if (name == null || name.isBlank()) {
            return false;
        }
        return WEB_CONTAINER_THREAD_PATTERN.matcher(name).matches();
    }

    /**
     * Determines whether the thread is a reactive non-blocking scheduler thread (Project Reactor)
     * or a Netty event loop thread where blocking operations (sleep, blocking I/O) are prohibited.
     */
    public static boolean isEventLoopOrNonBlockingThread(Thread thread) {
        if (thread == null) {
            return false;
        }
        // 1. Project Reactor marker interface
        try {
            Class<?> nonBlockingClass = Class.forName("reactor.core.scheduler.NonBlocking", false,
                    thread.getContextClassLoader() != null ? thread.getContextClassLoader() : ThreadTypeClassifier.class.getClassLoader());
            if (nonBlockingClass.isInstance(thread)) {
                return true;
            }
        } catch (Throwable ignored) {
        }
        // 2. Netty FastThreadLocalThread
        try {
            Class<?> nettyThreadClass = Class.forName("io.netty.util.concurrent.FastThreadLocalThread", false,
                    thread.getContextClassLoader() != null ? thread.getContextClassLoader() : ThreadTypeClassifier.class.getClassLoader());
            if (nettyThreadClass.isInstance(thread)) {
                return true;
            }
        } catch (Throwable ignored) {
        }
        // 3. Structured pattern matching for event loop thread names
        String name = thread.getName();
        if (name == null || name.isBlank()) {
            return false;
        }
        return EVENT_LOOP_THREAD_PATTERN.matcher(name).matches();
    }

    private static boolean hasActiveSpringWebRequest() {
        try {
            Class<?> holderClass = Class.forName(
                    "org.springframework.web.context.request.RequestContextHolder",
                    false,
                    ThreadTypeClassifier.class.getClassLoader()
            );
            Method getAttributes = holderClass.getMethod("getRequestAttributes");
            return getAttributes.invoke(null) != null;
        } catch (Throwable ignored) {
            return false;
        }
    }
}
