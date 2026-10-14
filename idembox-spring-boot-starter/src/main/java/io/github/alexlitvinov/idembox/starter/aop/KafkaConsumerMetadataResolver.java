package io.github.alexlitvinov.idembox.starter.aop;

import io.github.alexlitvinov.idembox.core.spel.SpelKeyExtractor;
import io.github.alexlitvinov.idembox.core.util.IdemboxUtils;
import io.github.alexlitvinov.idembox.starter.util.ThreadTypeClassifier;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.expression.EvaluationException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.KafkaUtils;
import org.springframework.messaging.Message;
import org.springframework.util.StringValueResolver;

import java.lang.annotation.Annotation;
import java.lang.reflect.Array;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.WildcardType;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

/**
 * Resolves Kafka consumer metadata, listener attributes, parameter types,
 * replay headers, reactive return types, and default return values for idempotent methods.
 */
public class KafkaConsumerMetadataResolver {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerMetadataResolver.class);

    public static String normalizeConsumerGroup(String group) {
        if (group == null) {
            return "";
        }
        if (group.length() <= 255) {
            return group;
        }
        int prefixLen = 190;
        if (Character.isHighSurrogate(group.charAt(prefixLen - 1))) {
            prefixLen--;
        }
        String prefix = group.substring(0, prefixLen);
        String hash = IdemboxUtils.sha256Hex(group);
        return prefix + "_" + hash.substring(0, 32);
    }

    public String resolveKey(String rawKeyExpression, Object target, Method method, Object[] args, SpelKeyExtractor spelKeyExtractor) {
        try {
            return spelKeyExtractor.extractKey(rawKeyExpression, target, method, args);
        } catch (EvaluationException | IllegalStateException ex) {
            String fallbackKey = findFallbackKafkaKey(args);
            if (fallbackKey != null && !fallbackKey.isBlank()) {
                log.debug("SpEL key evaluation failed ('{}') on Kafka tombstone record (null payload). Falling back to Kafka message key='{}'",
                        ex.getMessage(), fallbackKey);
                return fallbackKey.trim();
            }
            throw ex;
        }
    }

    public static String findFallbackKafkaKey(Object[] args) {
        if (!isTombstoneRecord(args)) {
            return null;
        }
        for (Object arg : args) {
            if (arg instanceof ConsumerRecord<?, ?> cr) {
                if (cr.key() != null) {
                    return stringifyKey(cr.key());
                }
            } else if (arg instanceof Message<?> springMessage) {
                Object keyHeader = springMessage.getHeaders().get(KafkaHeaders.RECEIVED_KEY);
                if (keyHeader == null) {
                    keyHeader = springMessage.getHeaders().get(KafkaHeaders.KEY);
                }
                if (keyHeader != null) {
                    return stringifyKey(keyHeader);
                }
            }
        }
        return null;
    }

    public static boolean isTombstoneRecord(Object[] args) {
        if (args == null) {
            return false;
        }
        for (Object arg : args) {
            if (arg instanceof ConsumerRecord<?, ?> cr) {
                return isNullOrKafkaNull(cr.value());
            }
            if (arg instanceof Message<?> springMessage) {
                if (isNullOrKafkaNull(springMessage.getPayload())) {
                    return true;
                }
            }
        }
        for (Object arg : args) {
            if (isNullOrKafkaNull(arg)) {
                return true;
            }
        }
        return false;
    }

    public static boolean isNullOrKafkaNull(Object value) {
        return value == null || "org.springframework.kafka.support.KafkaNull".equals(value.getClass().getName());
    }

    public String resolveConsumerGroup(String rawConsumerGroup, Method method, Object[] args, StringValueResolver valueResolver) {
        if (rawConsumerGroup != null && !rawConsumerGroup.isBlank()) {
            return rawConsumerGroup;
        }
        try {
            String currentGroupId = KafkaUtils.getConsumerGroupId();
            if (currentGroupId != null && !currentGroupId.isBlank()) {
                return currentGroupId;
            }
        } catch (Throwable ignored) {
        }
        String fromArgs = findFallbackKafkaGroupId(args);
        if (fromArgs != null && !fromArgs.isBlank()) {
            return fromArgs;
        }
        if (valueResolver != null) {
            try {
                String fromEnv = valueResolver.resolveStringValue("${spring.kafka.consumer.group-id:}");
                if (fromEnv != null && !fromEnv.isBlank() && !fromEnv.startsWith("${")) {
                    return fromEnv.trim();
                }
            } catch (Throwable ignored) {
            }
        }
        return method.getDeclaringClass().getName() + "#" + method.getName();
    }

    public static String findFallbackKafkaGroupId(Object[] args) {
        if (args == null) {
            return null;
        }
        for (Object arg : args) {
            if (arg instanceof Message<?> springMessage) {
                Object groupHeader = springMessage.getHeaders().get(KafkaHeaders.GROUP_ID);
                if (groupHeader != null) {
                    String val = groupHeader.toString().trim();
                    if (!val.isBlank() && val.length() <= 255) {
                        return val;
                    }
                }
            }
        }
        return null;
    }

    public static boolean isReactive(Object obj) {
        if (obj == null) {
            return false;
        }
        return isReactiveType(obj.getClass());
    }

    public static boolean isReactiveType(Class<?> clazz) {
        if (clazz == null || clazz == Object.class || clazz == void.class) {
            return false;
        }
        String name = clazz.getName();
        if ("org.reactivestreams.Publisher".equals(name)
                || "java.util.concurrent.Flow$Publisher".equals(name)
                || Flow.Publisher.class.isAssignableFrom(clazz)
                || name.startsWith("reactor.core.publisher.")
                || name.startsWith("io.reactivex.")
                || name.startsWith("io.smallrye.mutiny.")) {
            return true;
        }
        for (Class<?> implementedInterface : clazz.getInterfaces()) {
            if (isReactiveType(implementedInterface)) {
                return true;
            }
        }
        return isReactiveType(clazz.getSuperclass());
    }

    public static boolean isReplayRequested(Method method, Object[] args) {
        if (args != null) {
            for (Object arg : args) {
                if (arg instanceof ConsumerRecord<?, ?> record) {
                    if (record.headers() != null) {
                        for (Header header : record.headers()) {
                            if (header.key() != null && header.key().equalsIgnoreCase("X-Idempotent-Replay")) {
                                byte[] val = header.value();
                                if (val != null && "true".equalsIgnoreCase(new String(val, StandardCharsets.UTF_8).trim())) {
                                    return true;
                                }
                            }
                        }
                    }
                } else if (arg instanceof Message<?> springMessage) {
                    Object headerVal = springMessage.getHeaders().get("X-Idempotent-Replay");
                    if (headerVal == null) {
                        headerVal = springMessage.getHeaders().get("x-idempotent-replay");
                    }
                    if (headerVal instanceof byte[] b) {
                        if ("true".equalsIgnoreCase(new String(b, StandardCharsets.UTF_8).trim())) {
                            return true;
                        }
                    } else if (headerVal != null && "true".equalsIgnoreCase(headerVal.toString().trim())) {
                        return true;
                    }
                } else if (arg instanceof Headers headers) {
                    for (Header header : headers) {
                        if (header.key() != null && header.key().equalsIgnoreCase("X-Idempotent-Replay")) {
                            byte[] val = header.value();
                            if (val != null && "true".equalsIgnoreCase(new String(val, StandardCharsets.UTF_8).trim())) {
                                return true;
                            }
                        }
                    }
                }
            }
        }
        if (method != null && args != null) {
            Annotation[][] paramAnnotations = method.getParameterAnnotations();
            for (int i = 0; i < Math.min(paramAnnotations.length, args.length); i++) {
                for (Annotation ann : paramAnnotations[i]) {
                    if (ann instanceof org.springframework.messaging.handler.annotation.Header headerAnn) {
                        String name = headerAnn.value().isEmpty() ? headerAnn.name() : headerAnn.value();
                        if ("X-Idempotent-Replay".equalsIgnoreCase(name)) {
                            Object val = args[i];
                            if (val instanceof Boolean b && b) {
                                return true;
                            }
                            if (val != null && "true".equalsIgnoreCase(val.toString().trim())) {
                                return true;
                            }
                        }
                    }
                }
            }
        }
        return false;
    }

    public static String stringifyKey(Object key) {
        if (key == null) {
            return null;
        }
        if (key instanceof byte[] bytes) {
            return HexFormat.of().formatHex(bytes);
        }
        if (key.getClass().isArray()) {
            return SpelKeyExtractor.stringifyArray(key);
        }
        return key.toString();
    }

    public static boolean isEventLoopOrNonBlockingThread(Thread thread) {
        return ThreadTypeClassifier.isEventLoopOrNonBlockingThread(thread);
    }

    public static boolean hasKafkaListenerAnnotation(Class<?> targetClass, Method method) {
        try {
            KafkaListener ann = AnnotatedElementUtils.findMergedAnnotation(method, KafkaListener.class);
            if (ann == null && targetClass != null) {
                ann = AnnotatedElementUtils.findMergedAnnotation(targetClass, KafkaListener.class);
            }
            return ann != null;
        } catch (Throwable ignored) {
            return false;
        }
    }

    public boolean isKafkaBatchListener(Class<?> targetClass, Method method, StringValueResolver valueResolver) {
        try {
            KafkaListener ann = AnnotatedElementUtils.findMergedAnnotation(method, KafkaListener.class);
            if (ann == null && targetClass != null) {
                ann = AnnotatedElementUtils.findMergedAnnotation(targetClass, KafkaListener.class);
            }
            if (ann != null) {
                String batch = ann.batch();
                if (!batch.isEmpty()) {
                    String resolved = valueResolver != null ? valueResolver.resolveStringValue(batch) : batch;
                    return "true".equalsIgnoreCase(resolved != null ? resolved.trim() : "");
                }
            }
            return false;
        } catch (Throwable ignored) {
            return false;
        }
    }

    public static boolean isKafkaRecordOrMessageType(Type type) {
        if (type == null) return false;
        if (type instanceof Class<?> clazz) {
            return ConsumerRecord.class.isAssignableFrom(clazz)
                    || ConsumerRecords.class.isAssignableFrom(clazz)
                    || Message.class.isAssignableFrom(clazz);
        }
        if (type instanceof ParameterizedType pt) {
            return isKafkaRecordOrMessageType(pt.getRawType());
        }
        if (type instanceof WildcardType wt) {
            for (Type upper : wt.getUpperBounds()) {
                if (isKafkaRecordOrMessageType(upper)) return true;
            }
        }
        return false;
    }

    public static boolean isKafkaBatchParameterType(Type type) {
        if (type == null) return false;
        if (type instanceof Class<?> clazz) {
            if (ConsumerRecords.class.isAssignableFrom(clazz)) return true;
            return clazz.isArray() && isKafkaRecordOrMessageType(clazz.getComponentType());
        }
        if (type instanceof GenericArrayType gat) {
            return isKafkaRecordOrMessageType(gat.getGenericComponentType());
        }
        if (type instanceof ParameterizedType pt) {
            Type raw = pt.getRawType();
            if (raw instanceof Class<?> rawClass && Iterable.class.isAssignableFrom(rawClass)) {
                Type[] args = pt.getActualTypeArguments();
                return args.length > 0 && isKafkaRecordOrMessageType(args[0]);
            }
        }
        return false;
    }

    public static Object getDefaultReturnValue(Class<?> returnType) {
        if (!returnType.isPrimitive()) {
            if (returnType.isArray()) {
                return Array.newInstance(returnType.getComponentType(), 0);
            }
            if (Optional.class.isAssignableFrom(returnType)) {
                return Optional.empty();
            }
            if (List.class.isAssignableFrom(returnType)) {
                return Collections.emptyList();
            }
            if (Set.class.isAssignableFrom(returnType)) {
                return Collections.emptySet();
            }
            if (Collection.class.isAssignableFrom(returnType) || Iterable.class.isAssignableFrom(returnType)) {
                return Collections.emptyList();
            }
            if (Map.class.isAssignableFrom(returnType)) {
                return Collections.emptyMap();
            }
            if (CompletionStage.class.isAssignableFrom(returnType)) {
                return CompletableFuture.completedFuture(null);
            }
            return null;
        }
        if (returnType == boolean.class) return false;
        if (returnType == byte.class) return (byte) 0;
        if (returnType == short.class) return (short) 0;
        if (returnType == int.class) return 0;
        if (returnType == long.class) return 0L;
        if (returnType == float.class) return 0.0f;
        if (returnType == double.class) return 0.0d;
        if (returnType == char.class) return '\0';
        return null;
    }
}
