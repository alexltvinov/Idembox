package io.github.alexlitvinov.idembox.starter.aop;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.alexlitvinov.idembox.core.annotation.Idempotent;
import io.github.alexlitvinov.idembox.core.spel.DurationParser;
import io.github.alexlitvinov.idembox.starter.config.IdemboxProperties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanInitializationException;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.EmbeddedValueResolverAware;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.expression.Expression;
import org.springframework.expression.spel.SpelNode;
import org.springframework.expression.spel.ast.ConstructorReference;
import org.springframework.expression.spel.ast.TypeReference;
import org.springframework.expression.spel.standard.SpelExpression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.lang.NonNull;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.util.ClassUtils;
import org.springframework.util.ReflectionUtils;
import org.springframework.util.StringValueResolver;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Parameter;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Validates all {@link Idempotent} annotations on bean methods at application startup.
 * Prevents runtime failures caused by typos in duration strings (e.g. "7 days") or invalid SpEL syntax.
 */
public class IdempotentStartupValidator implements SmartInitializingSingleton, ApplicationContextAware, EmbeddedValueResolverAware {

    private static final Logger log = LoggerFactory.getLogger(IdempotentStartupValidator.class);

    private final SpelExpressionParser expressionParser = new SpelExpressionParser();
    private ApplicationContext applicationContext;
    private StringValueResolver valueResolver;
    private IdemboxProperties properties;

    public IdempotentStartupValidator() {
    }

    public IdempotentStartupValidator(IdemboxProperties properties) {
        this.properties = properties;
    }

    @Override
    public void setApplicationContext(@NonNull ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    @Override
    public void setEmbeddedValueResolver(@NonNull StringValueResolver resolver) {
        this.valueResolver = resolver;
    }

    public void validateGlobalProperties(IdemboxProperties props) {
        if (props == null || props.getIdempotency() == null) {
            return;
        }
        String defaultTtl = resolve(props.getIdempotency().getDefaultTtl());
        if (defaultTtl == null || defaultTtl.isBlank()) {
            throw new BeanInitializationException("Invalid idembox.idempotency.default-ttl: cannot be blank");
        }
        try {
            Duration ttl = DurationParser.parse(defaultTtl, null);
            if (ttl == null || ttl.isNegative() || ttl.isZero()) {
                throw new IllegalArgumentException("default-ttl must be positive, got: " + ttl);
            }
        } catch (Exception ex) {
            throw new BeanInitializationException(
                    "Invalid duration in idembox.idempotency.default-ttl (\"" + defaultTtl + "\"): " + ex.getMessage(), ex);
        }

        String defaultLockTimeout = resolve(props.getIdempotency().getDefaultLockTimeout());
        if (defaultLockTimeout == null || defaultLockTimeout.isBlank()) {
            throw new BeanInitializationException("Invalid idembox.idempotency.default-lock-timeout: cannot be blank");
        }
        try {
            Duration lockTimeout = DurationParser.parse(defaultLockTimeout, null);
            if (lockTimeout == null || lockTimeout.isNegative() || lockTimeout.isZero()) {
                throw new IllegalArgumentException("default-lock-timeout must be positive, got: " + lockTimeout);
            }
        } catch (Exception ex) {
            throw new BeanInitializationException(
                    "Invalid duration in idembox.idempotency.default-lock-timeout (\"" + defaultLockTimeout + "\"): " + ex.getMessage(), ex);
        }
    }

    @Override
    public void afterSingletonsInstantiated() {
        IdemboxProperties effectiveProps = this.properties;
        if (effectiveProps == null && applicationContext != null) {
            try {
                effectiveProps = applicationContext.getBeanProvider(IdemboxProperties.class).getIfAvailable();
            } catch (Throwable ignored) {
            }
        }
        validateGlobalProperties(effectiveProps);

        if (applicationContext == null) {
            return;
        }

        ConfigurableListableBeanFactory beanFactory = (applicationContext.getAutowireCapableBeanFactory() instanceof ConfigurableListableBeanFactory factory) ? factory : null;

        String[] beanNames = applicationContext.getBeanDefinitionNames();
        for (String beanName : beanNames) {
            Class<?> targetClass = null;

            if (beanFactory != null && beanFactory.containsBeanDefinition(beanName)) {
                BeanDefinition bd = beanFactory.getBeanDefinition(beanName);
                if (!bd.isSingleton() || bd.isLazyInit() || bd.isAbstract()) {
                    // Inspect class without eager bean initialization
                    try {
                        Class<?> beanType = beanFactory.getType(beanName);
                        if (beanType != null) {
                            targetClass = ClassUtils.getUserClass(beanType);
                        }
                    } catch (Throwable ignored) {
                        continue;
                    }
                    if (targetClass != null) {
                        final Class<?> effectiveTargetClass = targetClass;
                        ReflectionUtils.doWithMethods(effectiveTargetClass, method -> {
                            Idempotent idempotent = AnnotatedElementUtils.findMergedAnnotation(method, Idempotent.class);
                            if (idempotent != null) {
                                validateIdempotentAnnotation(effectiveTargetClass, method, idempotent);
                            }
                        });
                    }
                    continue;
                }
            }

            try {
                Object bean = applicationContext.getBean(beanName);
                targetClass = AopUtils.getTargetClass(bean);
            } catch (Throwable t) {
                // Fallback to bean type inspection if bean cannot be instantiated early
                try {
                    Class<?> beanType = applicationContext.getType(beanName);
                    if (beanType != null) {
                        targetClass = ClassUtils.getUserClass(beanType);
                    }
                } catch (Throwable ignored) {
                    continue;
                }
            }

            if (targetClass == null) {
                continue;
            }

            final Class<?> effectiveTargetClass = targetClass;
            ReflectionUtils.doWithMethods(effectiveTargetClass, method -> {
                Idempotent idempotent = AnnotatedElementUtils.findMergedAnnotation(method, Idempotent.class);
                if (idempotent != null) {
                    validateIdempotentAnnotation(effectiveTargetClass, method, idempotent);
                }
            });
        }
    }

    public void validateIdempotentAnnotation(Class<?> targetClass, Method method, Idempotent idempotent) {
        Class<?> effectiveClass = targetClass != null ? targetClass : method.getDeclaringClass();
        String methodDesc = effectiveClass.getSimpleName() + "#" + method.getName();

        validateClassModifiers(effectiveClass, methodDesc);
        validateProxyDeclaration(effectiveClass, method, methodDesc);
        validateMethodModifiers(method, methodDesc);
        validateNotBatchListener(effectiveClass, method, methodDesc);

        Class<?> returnType = method.getReturnType();
        if (IdempotentConsumerAspect.isReactiveType(returnType)) {
            throw new BeanInitializationException("@Idempotent cannot be applied to reactive method returning " + returnType.getName() + ": " + methodDesc +
                    ". Reactive publishers execute lazily and are incompatible with synchronous/CompletionStage idempotency interception.");
        }

        validateExpressions(idempotent, methodDesc);
        validateDurations(idempotent, methodDesc);
        validateTransactionalAndReturnTypeWarnings(effectiveClass, method, idempotent, returnType, methodDesc);

        log.debug("Validated @Idempotent annotation on {}", methodDesc);
    }

    private void validateNotBatchListener(Class<?> effectiveClass, Method method, String methodDesc) {
        Parameter[] parameters = method.getParameters();
        boolean isKafkaListener = IdempotentConsumerAspect.hasKafkaListenerAnnotation(effectiveClass, method);
        boolean isBatchMethod = hasKafkaListenerBatchTrue(effectiveClass, method)
                || Arrays.stream(parameters)
                .filter(param -> !isHeaderParameter(param))
                .anyMatch(param -> {
                    Class<?> p = param.getType();
                    if (IdempotentConsumerAspect.isKafkaBatchParameterType(param.getParameterizedType())) {
                        return true;
                    }
                    if (ConsumerRecords.class.isAssignableFrom(p)) {
                        return true;
                    }
                    if (p.isArray() && (ConsumerRecord.class.isAssignableFrom(p.getComponentType())
                            || Message.class.isAssignableFrom(p.getComponentType()))) {
                        return true;
                    }
                    return isKafkaListener && Iterable.class.isAssignableFrom(p)
                            && !JsonNode.class.isAssignableFrom(p);
                });
        if (isBatchMethod) {
            throw new BeanInitializationException("@Idempotent cannot be applied to batch listener method accepting Collection or Iterable, or Array: " + methodDesc +
                    ". @Idempotent is designed for single-record consumers; batch consumers must iterate and invoke an idempotent service per element.");
        }
    }

    private void validateExpressions(Idempotent idempotent, String methodDesc) {
        String rawKey = resolve(idempotent.key());
        if (rawKey == null || rawKey.isBlank()) {
            throw new BeanInitializationException("Invalid @Idempotent configuration on " + methodDesc + ": key expression cannot be blank");
        }

        try {
            Expression expression = expressionParser.parseExpression(rawKey);
            if (expression instanceof SpelExpression parsedExpression) {
                checkForDisallowedNodes(parsedExpression.getAST());
            }
        } catch (Exception ex) {
            throw new BeanInitializationException("Invalid SpEL expression in @Idempotent(key = \"" + rawKey + "\") on " + methodDesc + ": " + ex.getMessage(), ex);
        }

        String rawCondition = resolve(idempotent.condition());
        if (rawCondition != null && !rawCondition.isBlank()) {
            try {
                Expression expression = expressionParser.parseExpression(rawCondition);
                if (expression instanceof SpelExpression parsedExpression) {
                    checkForDisallowedNodes(parsedExpression.getAST());
                }
            } catch (Exception ex) {
                throw new BeanInitializationException("Invalid SpEL expression in @Idempotent(condition = \"" + rawCondition + "\") on " + methodDesc + ": " + ex.getMessage(), ex);
            }
        }
    }

    private void validateDurations(Idempotent idempotent, String methodDesc) {
        String rawTtl = resolve(idempotent.ttl());
        if (rawTtl != null && !rawTtl.isBlank()) {
            try {
                Duration ttl = DurationParser.parse(rawTtl, null);
                if (ttl == null || ttl.isNegative() || ttl.isZero()) {
                    throw new IllegalArgumentException("ttl must be positive, got: " + ttl);
                }
            } catch (Exception ex) {
                throw new BeanInitializationException("Invalid ttl duration in @Idempotent(ttl = \"" + rawTtl + "\") on " + methodDesc + ": " + ex.getMessage(), ex);
            }
        }

        String rawLockTimeout = resolve(idempotent.lockTimeout());
        if (rawLockTimeout != null && !rawLockTimeout.isBlank()) {
            try {
                Duration lockTimeout = DurationParser.parse(rawLockTimeout, null);
                if (lockTimeout == null || lockTimeout.isNegative() || lockTimeout.isZero()) {
                    throw new IllegalArgumentException("lockTimeout must be positive, got: " + lockTimeout);
                }
            } catch (Exception ex) {
                throw new BeanInitializationException("Invalid lockTimeout duration in @Idempotent(lockTimeout = \"" + rawLockTimeout + "\") on " + methodDesc + ": " + ex.getMessage(), ex);
            }
        }
    }

    private static void validateTransactionalAndReturnTypeWarnings(
            Class<?> effectiveClass, Method method, Idempotent idempotent, Class<?> returnType, String methodDesc) {
        TransactionalAnnotationUtils.TransactionalInfo txInfo =
                TransactionalAnnotationUtils.findDeclaredTransactionalInfo(method, effectiveClass);
        if (!txInfo.isTransactional()) {
            log.warn("Method '{}' is annotated with @Idempotent but not @Transactional. " +
                    "If this method invokes database mutations or subordinate @Transactional services alongside external I/O, " +
                    "failure of subsequent external I/O may cause duplicate execution on Kafka retry because the subordinate transaction commits independently. " +
                    "Ensure @Transactional surrounds the entire idempotent consumer method if atomic all-or-nothing rollback is required.",
                    methodDesc);
        } else if (txInfo.isReadOnly()) {
            log.warn("Method '{}' is annotated with @Idempotent alongside @Transactional(readOnly = true). " +
                    "Idempotent consumer records require database state transitions (marking COMPLETED or FAILED). " +
                    "A read-only transaction will cause idempotent record completion to be executed in a separate REQUIRES_NEW transaction, " +
                    "decoupling the business transaction from the idempotency record update. " +
                    "Consider removing readOnly = true for idempotent consumer methods.",
                    methodDesc);
        } else if (CompletionStage.class.isAssignableFrom(returnType)) {
            log.warn("Method '{}' is annotated with both @Idempotent and @Transactional and returns CompletionStage/CompletableFuture. " +
                    "Transactional boundary commits synchronously upon method return, before asynchronous stage completion. " +
                    "If the asynchronous stage fails later, the transaction has already committed! " +
                    "To ensure transactional integrity with asynchronous stages, execute transactional operations inside the stage using TransactionTemplate.",
                    methodDesc);
        }

        if (idempotent.swallowDuplicate()) {
            if (!isSafeSwallowReturnType(returnType)) {
                log.warn("Method '{}' is annotated with @Idempotent(swallowDuplicate = true) but has non-container return type '{}'. " +
                        "When duplicate messages are swallowed, getDefaultReturnValue returns null (or default primitive), " +
                        "which may cause NullPointerException in synchronous callers (e.g. calling result.getStatus()). " +
                        "For methods returning domain response objects, consider setting swallowDuplicate = false and handling DuplicateMessageException, " +
                        "or returning Optional<{}>.",
                        methodDesc, returnType.getName(), returnType.getSimpleName());
            }

            if (hasSendToAnnotation(method, effectiveClass)) {
                log.warn("Method '{}' is annotated with @SendTo / @SendToUser alongside @Idempotent(swallowDuplicate = true). " +
                        "When duplicate messages are swallowed, returning null causes Spring Kafka to publish a Tombstone record " +
                        "(null payload) to the reply topic. " +
                        "To prevent accidental tombstone records, set swallowDuplicate = false to let duplicates fail or bypass reply generation, " +
                        "or avoid @SendTo on idempotent listener methods that swallow duplicates.",
                        methodDesc);
            }
        }
    }

    private static void validateProxyDeclaration(Class<?> effectiveClass, Method method, String methodDesc) {
        if (isDeclaredOnInterface(effectiveClass, method)) {
            throw new BeanInitializationException(
                    "@Idempotent cannot be declared on an interface method: " + methodDesc + ". " +
                    "Spring AOP @annotation pointcuts do not match interface annotations in CGLIB proxy mode (proxy-target-class=true). " +
                    "Declare @Idempotent directly on the concrete class implementation method.");
        }

        boolean isDirectlyAnnotated = method.isAnnotationPresent(Idempotent.class)
                || Arrays.stream(method.getAnnotations()).anyMatch(a -> a.annotationType().isAnnotationPresent(Idempotent.class));
        if (!isDirectlyAnnotated) {
            throw new BeanInitializationException(
                    "Overridden method " + methodDesc + " must explicitly declare @Idempotent " +
                    "on the subclass method to enable Spring CGLIB proxy interception.");
        }
    }

    private static boolean isDeclaredOnInterface(Class<?> effectiveClass, Method method) {
        if (effectiveClass.isInterface() || method.getDeclaringClass().isInterface()) {
            return true;
        }
        for (Class<?> implementedInterface : ClassUtils.getAllInterfacesForClass(effectiveClass)) {
            try {
                Method interfaceMethod = implementedInterface.getMethod(method.getName(), method.getParameterTypes());
                if (interfaceMethod.isAnnotationPresent(Idempotent.class)
                        || Arrays.stream(interfaceMethod.getAnnotations()).anyMatch(a -> a.annotationType().isAnnotationPresent(Idempotent.class))) {
                    return true;
                }
            } catch (NoSuchMethodException ignored) {
            }
        }
        return false;
    }

    private static void validateClassModifiers(Class<?> effectiveClass, String methodDesc) {
        if (effectiveClass != null && Modifier.isFinal(effectiveClass.getModifiers())) {
            throw new BeanInitializationException(
                    "@Idempotent cannot be applied to final class '" + effectiveClass.getName() + "' (on " + methodDesc + "). " +
                    "Spring CGLIB proxies require classes to be non-final (e.g., use 'open class' in Kotlin or remove the final modifier in Java).");
        }
    }

    private static void validateMethodModifiers(Method method, String methodDesc) {
        int modifiers = method.getModifiers();
        if (Modifier.isPrivate(modifiers)) {
            throw new BeanInitializationException("@Idempotent cannot be applied to private method: " + methodDesc);
        }
        if (Modifier.isFinal(modifiers)) {
            throw new BeanInitializationException("@Idempotent cannot be applied to final method: " + methodDesc);
        }
        if (Modifier.isStatic(modifiers)) {
            throw new BeanInitializationException("@Idempotent cannot be applied to static method: " + methodDesc);
        }
        if (Modifier.isAbstract(modifiers)) {
            throw new BeanInitializationException("@Idempotent cannot be applied to abstract method: " + methodDesc +
                    ". Method annotations are not inherited by overriding methods in subclasses; declare @Idempotent directly on concrete implementation methods.");
        }
    }

    public static boolean hasSendToAnnotation(Method method, Class<?> targetClass) {
        if (method != null) {
            for (Annotation ann : method.getAnnotations()) {
                String name = ann.annotationType().getName();
                String simpleName = ann.annotationType().getSimpleName();
                if ("org.springframework.messaging.handler.annotation.SendTo".equals(name)
                        || "org.springframework.messaging.simp.annotation.SendToUser".equals(name)
                        || "SendTo".equals(simpleName)
                        || "SendToUser".equals(simpleName)) {
                    return true;
                }
            }
        }
        if (targetClass != null) {
            for (Annotation ann : targetClass.getAnnotations()) {
                String name = ann.annotationType().getName();
                String simpleName = ann.annotationType().getSimpleName();
                if ("org.springframework.messaging.handler.annotation.SendTo".equals(name)
                        || "org.springframework.messaging.simp.annotation.SendToUser".equals(name)
                        || "SendTo".equals(simpleName)
                        || "SendToUser".equals(simpleName)) {
                    return true;
                }
            }
        }
        return false;
    }

    private void checkForDisallowedNodes(SpelNode node) {
        if (node instanceof TypeReference) {
            throw new SecurityException("Type references like T(...) are not permitted in @Idempotent key expressions for security reasons: " + node.toStringAST());
        }
        if (node instanceof ConstructorReference) {
            throw new SecurityException("Constructor references like 'new ...' are not permitted in @Idempotent key expressions for security reasons: " + node.toStringAST());
        }
        for (int i = 0; i < node.getChildCount(); i++) {
            checkForDisallowedNodes(node.getChild(i));
        }
    }

    static boolean isSafeSwallowReturnType(Class<?> returnType) {
        return returnType == null
                || returnType == void.class
                || returnType == Void.class
                || Optional.class.isAssignableFrom(returnType)
                || Collection.class.isAssignableFrom(returnType)
                || Map.class.isAssignableFrom(returnType)
                || CompletableFuture.class.isAssignableFrom(returnType)
                || CompletionStage.class.isAssignableFrom(returnType);
    }

    private String resolve(String value) {
        if (valueResolver != null && value != null) {
            return valueResolver.resolveStringValue(value);
        }
        return value;
    }

    private boolean hasKafkaListenerBatchTrue(Class<?> targetClass, Method method) {
        try {
            KafkaListener ann = AnnotatedElementUtils.findMergedAnnotation(method, KafkaListener.class);
            if (ann == null && targetClass != null) {
                ann = AnnotatedElementUtils.findMergedAnnotation(targetClass, KafkaListener.class);
            }
            if (ann != null) {
                String batch = ann.batch();
                if (!batch.isEmpty()) {
                    String resolved = resolve(batch);
                    return "true".equalsIgnoreCase(resolved != null ? resolved.trim() : "");
                }
            }
        } catch (Throwable ignored) {
        }
        return false;
    }

    private static boolean isHeaderParameter(Parameter param) {
        try {
            return param.isAnnotationPresent(Header.class)
                    || param.isAnnotationPresent(Headers.class);
        } catch (Throwable ignored) {
            return false;
        }
    }
}
