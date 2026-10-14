package io.github.alexlitvinov.idembox.core.spel;

import io.github.alexlitvinov.idembox.core.util.IdemboxUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.context.expression.BeanFactoryResolver;
import org.springframework.context.expression.MethodBasedEvaluationContext;
import org.springframework.core.DefaultParameterNameDiscoverer;
import org.springframework.core.ParameterNameDiscoverer;
import org.springframework.expression.BeanResolver;
import org.springframework.expression.EvaluationContext;
import org.springframework.expression.EvaluationException;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.SpelNode;
import org.springframework.expression.spel.ast.ConstructorReference;
import org.springframework.expression.spel.ast.TypeReference;
import org.springframework.expression.spel.standard.SpelExpression;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.lang.NonNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HexFormat;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Thread-safe utility to evaluate SpEL expressions on method invocation parameters.
 * Fully supports expressions like {@code "#event.orderId"}, {@code "#root.args[0].id"}, {@code "#p0"},
 * and Spring bean references like {@code "@beanName.method(#event)"}.
 */
public class SpelKeyExtractor implements BeanFactoryAware {

    private static final Logger log = LoggerFactory.getLogger(SpelKeyExtractor.class);

    public static final int MAX_EXPRESSION_CACHE_SIZE = 1000;
    public static final int MAX_PARAM_NAMES_CACHE_SIZE = 1000;

    private final ExpressionParser expressionParser = new SpelExpressionParser();
    private final Map<String, String[]> paramNamesCache = new ConcurrentHashMap<>(64);
    private final ParameterNameDiscoverer parameterNameDiscoverer = new ParameterNameDiscoverer() {
        private final ParameterNameDiscoverer delegate = new DefaultParameterNameDiscoverer();

        @Override
        public String[] getParameterNames(@NonNull Method method) {
            String descriptor = getMethodDescriptor(method);
            String[] cached = paramNamesCache.get(descriptor);
            if (cached != null) {
                return cached;
            }
            String[] discovered = delegate.getParameterNames(method);
            if (discovered != null) {
                if (paramNamesCache.size() < MAX_PARAM_NAMES_CACHE_SIZE) {
                    paramNamesCache.putIfAbsent(descriptor, discovered);
                }
                return discovered;
            }
            return null;
        }

        @Override
        public String[] getParameterNames(@NonNull Constructor<?> constructor) {
            return delegate.getParameterNames(constructor);
        }
    };
    private final Map<String, Expression> expressionCache = new ConcurrentHashMap<>(64);
    private BeanResolver beanResolver;

    public SpelKeyExtractor() {
    }

    public SpelKeyExtractor(BeanResolver beanResolver) {
        this.beanResolver = beanResolver;
    }

    @Override
    public void setBeanFactory(@NonNull BeanFactory beanFactory) throws BeansException {
        this.beanResolver = new BeanFactoryResolver(beanFactory);
    }

    public int getCacheSize() {
        return expressionCache.size();
    }

    public void clearCache() {
        expressionCache.clear();
    }

    public int getParamNamesCacheSize() {
        return paramNamesCache.size();
    }

    public void clearParamNamesCache() {
        paramNamesCache.clear();
    }

    public static final int MAX_EXPRESSION_LENGTH = 1024;

    public Expression getOrParseExpression(String expressionString) {
        if (expressionString == null || expressionString.isBlank()) {
            throw new IllegalArgumentException("Expression cannot be blank");
        }
        if (expressionString.length() > MAX_EXPRESSION_LENGTH) {
            throw new IllegalArgumentException("Expression length exceeds maximum limit of " + MAX_EXPRESSION_LENGTH + " characters (actual: " + expressionString.length() + ")");
        }
        Expression cached = expressionCache.get(expressionString);
        if (cached != null) {
            return cached;
        }
        Expression parsed = expressionParser.parseExpression(expressionString);
        validateExpressionAst(parsed);
        if (expressionCache.size() < MAX_EXPRESSION_CACHE_SIZE) {
            expressionCache.putIfAbsent(expressionString, parsed);
        }
        return parsed;
    }

    public record MethodInvocationRoot(Object target, Method method, Object[] args) {
    }

    public boolean evaluateCondition(String conditionExpression, Method method, Object[] args) {
        return evaluateCondition(conditionExpression, null, method, args);
    }

    public boolean evaluateCondition(String conditionExpression, Object target, Method method, Object[] args) {
        if (conditionExpression == null || conditionExpression.isBlank()) {
            return true;
        }

        Expression expression = getOrParseExpression(conditionExpression);
        EvaluationContext context = createEvaluationContext(target, method, args);
        Object value = expression.getValue(context);

        if (value == null) {
            return false;
        }
        if (value instanceof Boolean booleanValue) {
            return booleanValue;
        }
        log.warn("Condition expression '{}' evaluated to non-boolean value '{}'. Coercing to boolean.", conditionExpression, value);
        return Boolean.parseBoolean(value.toString());
    }

    public String extractKey(String expressionString, Method method, Object[] args) {
        return extractKey(expressionString, null, method, args);
    }

    public String extractKey(String expressionString, Object target, Method method, Object[] args) {
        if (expressionString == null || expressionString.isBlank()) {
            throw new IllegalArgumentException("Idempotent key expression cannot be blank");
        }

        Expression expression = getOrParseExpression(expressionString);
        EvaluationContext context = createEvaluationContext(target, method, args);
        Object value = expression.getValue(context);

        if (value == null) {
            throw new IllegalStateException("Evaluated idempotent key expression '" + expressionString + "' returned null. " +
                    "Did you mean to use conditional guard @Idempotent(condition = '" + expressionString + " != null', key = '" + expressionString + "')? " +
                    "If the key field is optional, use SpEL safe navigation or condition: " +
                    "@Idempotent(condition = \"" + expressionString + " != null\", key = \"" + expressionString + "\")");
        }

        String key = stringifyValue(value).trim();
        if (key.contains("\\u0000") || key.indexOf('\0') >= 0) {
            key = key.replace("\\u0000", "").replace("\0", "").trim();
        }
        if (key.isEmpty()) {
            throw new IllegalStateException("Evaluated idempotent key expression '" + expressionString + "' returned an empty or blank key");
        }

        // B-Tree row size protection for PostgreSQL (~2704 byte limit on standard 8KB pages)
        byte[] utf8Bytes = key.getBytes(StandardCharsets.UTF_8);
        if (utf8Bytes.length > 2000) {
            return "SHA256:" + IdemboxUtils.sha256Hex(utf8Bytes);
        }

        return key;
    }

    private void validateExpressionAst(Expression expression) {
        if (expression instanceof SpelExpression parsedExpression) {
            SpelNode ast = parsedExpression.getAST();
            checkForDisallowedNodes(ast);
        }
    }

    private void checkForDisallowedNodes(SpelNode node) {
        if (node instanceof TypeReference) {
            throw new SecurityException("Type references like T(...) are not permitted in @Idempotent key expressions for security reasons: " + node.toStringAST());
        }
        if (node instanceof ConstructorReference) {
            throw new SecurityException("Constructor references like 'new ...' are not permitted in @Idempotent key expressions for security reasons: " + node.toStringAST());
        }
        for (int index = 0; index < node.getChildCount(); index++) {
            checkForDisallowedNodes(node.getChild(index));
        }
    }

    private EvaluationContext createEvaluationContext(Object target, Method method, Object[] args) {
        Object[] effectiveArgs = args != null ? args : new Object[0];
        MethodInvocationRoot root = new MethodInvocationRoot(target, method, effectiveArgs);
        MethodBasedEvaluationContext context = new MethodBasedEvaluationContext(root, method, effectiveArgs, parameterNameDiscoverer);

        // Security: block type locator so arbitrary static method execution and reflection via T(...) cannot be resolved
        context.setTypeLocator(typeName -> {
            throw new EvaluationException("Type references like T(...) are not permitted in @Idempotent key expressions: " + typeName);
        });

        if (this.beanResolver != null) {
            context.setBeanResolver(this.beanResolver);
        }

        bindArgumentVariables(context, effectiveArgs);
        return context;
    }

    private static void bindArgumentVariables(EvaluationContext context, Object[] arguments) {
        context.setVariable("args", arguments);
        for (int index = 0; index < arguments.length; index++) {
            context.setVariable("arg" + index, arguments[index]);
            context.setVariable("p" + index, arguments[index]);
            context.setVariable("a" + index, arguments[index]);
        }
    }

    private static String getMethodDescriptor(Method method) {
        StringBuilder builder = new StringBuilder(128);
        builder.append(method.getDeclaringClass().getName()).append('#').append(method.getName()).append('(');
        Class<?>[] params = method.getParameterTypes();
        for (int index = 0; index < params.length; index++) {
            if (index > 0) {
                builder.append(',');
            }
            builder.append(params[index].getName());
        }
        builder.append(')');
        return builder.toString();
    }

    public static String stringifyValue(Object value) {
        return stringifyValue(value, Collections.newSetFromMap(new IdentityHashMap<>()), 0);
    }

    private static String stringifyValue(Object value, Set<Object> visited, int depth) {
        if (value == null) {
            return "null";
        }
        if (depth > 16 || !visited.add(value)) {
            return "...";
        }
        try {
            if (value instanceof byte[] bytes) {
                return HexFormat.of().formatHex(bytes);
            }
            if (value.getClass().isArray()) {
                return stringifyArray(value, visited, depth);
            }
            if (value instanceof Map<?, ?> map) {
                return stringifyMap(map, visited, depth);
            }
            if (value instanceof Set<?> set) {
                return stringifySet(set, visited, depth);
            }
            if (value instanceof Collection<?> collection) {
                return stringifyCollection(collection, visited, depth);
            }
            return value.toString();
        } finally {
            visited.remove(value);
        }
    }

    private static String stringifyMap(Map<?, ?> map, Set<Object> visited, int depth) {
        if (map.isEmpty()) {
            return "{}";
        }
        List<Map.Entry<?, ?>> entries = new ArrayList<>(map.entrySet());
        entries.sort((first, second) -> compareMapEntries(first, second, visited, depth));
        StringBuilder builder = new StringBuilder("{");
        boolean isFirst = true;
        for (Map.Entry<?, ?> entry : entries) {
            if (!isFirst) {
                builder.append(", ");
            }
            isFirst = false;
            builder.append(stringifyValue(entry.getKey(), visited, depth + 1))
                    .append("=")
                    .append(stringifyValue(entry.getValue(), visited, depth + 1));
        }
        builder.append("}");
        return builder.toString();
    }

    private static int compareMapEntries(Map.Entry<?, ?> first, Map.Entry<?, ?> second, Set<Object> visited, int depth) {
        String keyFirst = stringifyValue(first.getKey(), visited, depth + 1);
        String keySecond = stringifyValue(second.getKey(), visited, depth + 1);
        int keyComparison = keyFirst.compareTo(keySecond);
        if (keyComparison != 0) {
            return keyComparison;
        }
        String valueFirst = stringifyValue(first.getValue(), visited, depth + 1);
        String valueSecond = stringifyValue(second.getValue(), visited, depth + 1);
        return valueFirst.compareTo(valueSecond);
    }

    private static String stringifySet(Set<?> set, Set<Object> visited, int depth) {
        if (set.isEmpty()) {
            return "[]";
        }
        List<String> items = new ArrayList<>(set.size());
        for (Object item : set) {
            items.add(stringifyValue(item, visited, depth + 1));
        }
        Collections.sort(items);
        return "[" + String.join(", ", items) + "]";
    }

    private static String stringifyCollection(Collection<?> coll, Set<Object> visited, int depth) {
        if (coll.isEmpty()) {
            return "[]";
        }
        List<String> items = new ArrayList<>(coll.size());
        for (Object item : coll) {
            items.add(stringifyValue(item, visited, depth + 1));
        }
        return "[" + String.join(", ", items) + "]";
    }

    public static String stringifyArray(Object array) {
        return stringifyArray(array, Collections.newSetFromMap(new IdentityHashMap<>()), 0);
    }

    private static String stringifyArray(Object array, Set<Object> visited, int depth) {
        if (array == null) {
            return "null";
        }
        if (array instanceof Object[] objArray) {
            StringBuilder sb = new StringBuilder("[");
            for (int i = 0; i < objArray.length; i++) {
                if (i > 0) {
                    sb.append(", ");
                }
                sb.append(stringifyValue(objArray[i], visited, depth + 1));
            }
            sb.append("]");
            return sb.toString();
        }
        if (array instanceof byte[] bytes) {
            return HexFormat.of().formatHex(bytes);
        }
        if (array instanceof int[] a) return Arrays.toString(a);
        if (array instanceof long[] a) return Arrays.toString(a);
        if (array instanceof short[] a) return Arrays.toString(a);
        if (array instanceof char[] a) return Arrays.toString(a);
        if (array instanceof double[] a) return Arrays.toString(a);
        if (array instanceof float[] a) return Arrays.toString(a);
        if (array instanceof boolean[] a) return Arrays.toString(a);
        return array.toString();
    }
}
