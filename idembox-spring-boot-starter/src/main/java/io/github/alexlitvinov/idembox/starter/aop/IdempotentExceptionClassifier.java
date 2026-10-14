package io.github.alexlitvinov.idembox.starter.aop;

import io.github.alexlitvinov.idembox.core.annotation.Idempotent;
import org.springframework.kafka.listener.ListenerExecutionFailedException;

import java.sql.SQLException;
import java.util.Locale;

/**
 * Classifies exceptions encountered during idempotent execution into terminal vs retryable,
 * fatal Kafka producer/consumer errors, and PostgreSQL aborted states.
 */
public class IdempotentExceptionClassifier {

    public boolean isTerminalException(Idempotent idempotent, Throwable t) {
        if (t == null) {
            return false;
        }
        Class<? extends Throwable>[] terminalClasses = idempotent != null ? idempotent.terminalExceptions() : null;
        Class<? extends Throwable>[] retryableClasses = idempotent != null ? idempotent.retryableExceptions() : null;

        int terminalDepth = getExceptionDepth(terminalClasses, t);
        int retryableDepth = getExceptionDepth(retryableClasses, t);

        if (terminalDepth >= 0 && (retryableDepth < 0 || terminalDepth <= retryableDepth)) {
            return true;
        }
        if (retryableDepth >= 0 && retryableDepth < terminalDepth) {
            return false;
        }
        if (retryableClasses != null && retryableClasses.length > 0) {
            // When an explicit whitelist of retryable exceptions is configured, anything not matching is terminal
            return retryableDepth < 0;
        }
        return isKnownFatalException(t);
    }

    public int getExceptionDepth(Class<? extends Throwable>[] classes, Throwable t) {
        if (classes == null || classes.length == 0 || t == null) {
            return -1;
        }
        int bestDepth = -1;
        for (Class<? extends Throwable> clazz : classes) {
            int depth = calculateHierarchyDepth(clazz, t.getClass());
            if (depth >= 0 && (bestDepth == -1 || depth < bestDepth)) {
                bestDepth = depth;
            }
        }
        return bestDepth;
    }

    public int calculateHierarchyDepth(Class<?> ruleClass, Class<?> exceptionClass) {
        return TransactionalAnnotationUtils.getDepth(exceptionClass, ruleClass);
    }

    private static final java.util.Map<Throwable, IdempotentMetadataException> THROWABLE_METADATA =
            java.util.Collections.synchronizedMap(new java.util.WeakHashMap<>());

    public static void attachMetadata(Throwable t, String key, String consumerGroup) {
        if (t == null) {
            return;
        }
        IdempotentMetadataException metadata = new IdempotentMetadataException(key, consumerGroup);
        THROWABLE_METADATA.put(t, metadata);
        try {
            for (Throwable s : t.getSuppressed()) {
                if (s instanceof IdempotentMetadataException) {
                    return;
                }
            }
            t.addSuppressed(metadata);
        } catch (Throwable ignored) {
            // Some exceptions disable suppression
        }
    }

    public static IdempotentMetadataException findMetadata(Throwable t) {
        Throwable current = t;
        int depth = 0;
        while (current != null && depth++ < 25) {
            if (current instanceof IdempotentMetadataException ime) {
                return ime;
            }
            IdempotentMetadataException cached = THROWABLE_METADATA.get(current);
            if (cached != null) {
                return cached;
            }
            for (Throwable s : current.getSuppressed()) {
                if (s instanceof IdempotentMetadataException ime) {
                    return ime;
                }
            }
            current = current.getCause();
        }
        return null;
    }

    public boolean isKnownFatalException(Throwable t) {
        Throwable current = t;
        int depth = 0;
        while (current != null && depth++ < 25) {
            if (current instanceof IllegalArgumentException
                    || current instanceof IllegalStateException
                    || current instanceof ClassCastException
                    || current instanceof NoSuchMethodException) {
                return true;
            }
            String className = current.getClass().getName();
            if (className.equals("org.apache.kafka.common.errors.RecordTooLargeException")
                    || className.equals("org.apache.kafka.common.errors.SerializationException")
                    || className.equals("org.springframework.kafka.support.serializer.DeserializationException")
                    || className.equals("org.springframework.messaging.converter.MessageConversionException")
                    || className.equals("org.springframework.messaging.handler.invocation.MethodArgumentResolutionException")
                    || className.equals("org.springframework.messaging.handler.annotation.support.MethodArgumentNotValidException")
                    || className.equals("org.springframework.web.bind.MethodArgumentNotValidException")
                    || className.equals("org.springframework.expression.EvaluationException")
                    || className.equals("jakarta.validation.ValidationException")
                    || className.equals("javax.validation.ValidationException")
                    || isValidationException(current)) {
                return true;
            }
            if (current instanceof ListenerExecutionFailedException listenerEx) {
                if (listenerEx.getCause() != null && isKnownFatalException(listenerEx.getCause())) {
                    return true;
                }
            }
            current = current.getCause();
        }
        return false;
    }

    private static boolean isValidationException(Throwable t) {
        Class<?> clazz = t.getClass();
        while (clazz != null && clazz != Object.class) {
            String name = clazz.getName();
            if ("jakarta.validation.ValidationException".equals(name)
                    || "javax.validation.ValidationException".equals(name)
                    || "org.springframework.expression.EvaluationException".equals(name)) {
                return true;
            }
            clazz = clazz.getSuperclass();
        }
        return false;
    }

    public static boolean isTransactionAbortedException(Throwable t) {
        Throwable current = t;
        int depth = 0;
        while (current != null && depth++ < 20) {
            if (current instanceof SQLException sqlEx) {
                if ("25P02".equals(sqlEx.getSQLState())) {
                    return true;
                }
            }
            String msg = current.getMessage();
            if (msg != null && (msg.contains("25P02") || msg.toLowerCase(Locale.ROOT).contains("current transaction is aborted"))) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }
}
