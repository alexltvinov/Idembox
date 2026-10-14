package io.github.alexlitvinov.idembox.starter.aop;

import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.springframework.util.ClassUtils;

import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;

/**
 * Utility for detecting transactional annotations across both Spring
 * ({@link org.springframework.transaction.annotation.Transactional}) and Jakarta
 * ({@code @jakarta.transaction.Transactional}) standards, and resolving their rollback rules
 * using class hierarchy depth distance matching Spring's RuleBasedTransactionAttribute standard.
 */
final class TransactionalAnnotationUtils {

    private static final Class<? extends Annotation> JAKARTA_TX_CLASS = loadJakartaTxClass();

    private static Class<? extends Annotation> loadJakartaTxClass() {
        try {
            if (ClassUtils.isPresent("jakarta.transaction.Transactional", TransactionalAnnotationUtils.class.getClassLoader())) {
                return ClassUtils.forName("jakarta.transaction.Transactional", TransactionalAnnotationUtils.class.getClassLoader())
                        .asSubclass(Annotation.class);
            }
        } catch (Throwable ignored) {
        }
        return null;
    }

    private TransactionalAnnotationUtils() {
    }

    record TransactionalInfo(boolean isTransactional, Object annotationSource) {
        boolean isReadOnly() {
            if (annotationSource instanceof Transactional springTx) {
                return springTx.readOnly();
            }
            return false;
        }

        boolean isRollbackFor(Throwable t) {
            if (!isTransactional || t == null) {
                return false;
            }
            if (annotationSource instanceof Transactional springTx) {
                return isSpringRollbackFor(springTx, t);
            }
            if (JAKARTA_TX_CLASS != null && JAKARTA_TX_CLASS.isInstance(annotationSource)) {
                return isJakartaRollbackFor((Annotation) annotationSource, t);
            }
            return true;
        }
    }

    private static final TransactionalInfo NON_TRANSACTIONAL = new TransactionalInfo(false, null);

    static TransactionalInfo findDeclaredTransactionalInfo(Method method, Class<?> targetClass) {
        if (method != null) {
            Transactional springTx = AnnotatedElementUtils.findMergedAnnotation(method, Transactional.class);
            if (springTx != null) {
                boolean declared = isDeclaredTransactionalPropagation(springTx);
                return new TransactionalInfo(declared, springTx);
            }
            if (JAKARTA_TX_CLASS != null) {
                Annotation jakartaTx = AnnotatedElementUtils.findMergedAnnotation(method, JAKARTA_TX_CLASS);
                if (jakartaTx != null) {
                    boolean declared = isDeclaredJakartaTransactional(jakartaTx);
                    return new TransactionalInfo(declared, jakartaTx);
                }
            }
        }

        if (targetClass != null) {
            Transactional springTx = AnnotatedElementUtils.findMergedAnnotation(targetClass, Transactional.class);
            if (springTx != null) {
                boolean declared = isDeclaredTransactionalPropagation(springTx);
                return new TransactionalInfo(declared, springTx);
            }
            if (JAKARTA_TX_CLASS != null) {
                Annotation jakartaTx = AnnotatedElementUtils.findMergedAnnotation(targetClass, JAKARTA_TX_CLASS);
                if (jakartaTx != null) {
                    boolean declared = isDeclaredJakartaTransactional(jakartaTx);
                    return new TransactionalInfo(declared, jakartaTx);
                }
            }
        }

        return NON_TRANSACTIONAL;
    }

    static TransactionalInfo findTransactionalInfo(Method method, Class<?> targetClass) {
        return findTransactionalInfo(method, targetClass, TransactionSynchronizationManager.isActualTransactionActive());
    }

    static TransactionalInfo findTransactionalInfo(Method method, Class<?> targetClass, boolean actualTransactionActive) {
        if (method != null) {
            Transactional springTx = AnnotatedElementUtils.findMergedAnnotation(method, Transactional.class);
            if (springTx != null) {
                boolean active = isTransactionalPropagation(springTx, actualTransactionActive);
                return new TransactionalInfo(active, springTx);
            }
            if (JAKARTA_TX_CLASS != null) {
                Annotation jakartaTx = AnnotatedElementUtils.findMergedAnnotation(method, JAKARTA_TX_CLASS);
                if (jakartaTx != null) {
                    boolean active = isJakartaTransactional(jakartaTx, actualTransactionActive);
                    return new TransactionalInfo(active, jakartaTx);
                }
            }
        }

        if (targetClass != null) {
            Transactional springTx = AnnotatedElementUtils.findMergedAnnotation(targetClass, Transactional.class);
            if (springTx != null) {
                boolean active = isTransactionalPropagation(springTx, actualTransactionActive);
                return new TransactionalInfo(active, springTx);
            }
            if (JAKARTA_TX_CLASS != null) {
                Annotation jakartaTx = AnnotatedElementUtils.findMergedAnnotation(targetClass, JAKARTA_TX_CLASS);
                if (jakartaTx != null) {
                    boolean active = isJakartaTransactional(jakartaTx, actualTransactionActive);
                    return new TransactionalInfo(active, jakartaTx);
                }
            }
        }

        if (actualTransactionActive) {
            return new TransactionalInfo(true, null);
        }

        return NON_TRANSACTIONAL;
    }

    private static boolean isDeclaredTransactionalPropagation(Transactional springTx) {
        if (springTx == null) {
            return false;
        }
        Propagation propagation = springTx.propagation();
        return propagation != Propagation.NOT_SUPPORTED
                && propagation != Propagation.NEVER
                && propagation != Propagation.SUPPORTS;
    }

    private static boolean isDeclaredJakartaTransactional(Annotation jakartaTx) {
        if (jakartaTx == null) {
            return false;
        }
        try {
            Object value = AnnotationUtils.getValue(jakartaTx, "value");
            if (value != null) {
                String typeName = value.toString();
                if ("NOT_SUPPORTED".equalsIgnoreCase(typeName)
                        || "NEVER".equalsIgnoreCase(typeName)
                        || "SUPPORTS".equalsIgnoreCase(typeName)) {
                    return false;
                }
            }
        } catch (Throwable ignored) {
        }
        return true;
    }

    private static boolean isTransactionalPropagation(Transactional springTx, boolean actualTransactionActive) {
        if (springTx == null) {
            return false;
        }
        Propagation propagation = springTx.propagation();
        if (propagation == Propagation.NOT_SUPPORTED || propagation == Propagation.NEVER) {
            return false;
        }
        return actualTransactionActive;
    }

    private static boolean isJakartaTransactional(Annotation jakartaTx, boolean actualTransactionActive) {
        if (jakartaTx == null) {
            return false;
        }
        try {
            Object value = AnnotationUtils.getValue(jakartaTx, "value");
            if (value != null) {
                String typeName = value.toString();
                if ("NOT_SUPPORTED".equalsIgnoreCase(typeName) || "NEVER".equalsIgnoreCase(typeName)) {
                    return false;
                }
            }
        } catch (Throwable ignored) {
        }
        return actualTransactionActive;
    }

    static boolean isSpringRollbackFor(Transactional txAnn, Throwable t) {
        if (txAnn == null || t == null) {
            return false;
        }
        Boolean explicitRollback = evaluateSpringRollbackRules(txAnn, t);
        if (explicitRollback != null) {
            return explicitRollback;
        }
        if (txAnn.propagation() == Propagation.SUPPORTS) {
            return true;
        }
        return (t instanceof RuntimeException) || (t instanceof Error);
    }

    private static Boolean evaluateSpringRollbackRules(Transactional txAnn, Throwable t) {
        int bestDepth = Integer.MAX_VALUE;
        boolean winnerIsRollback = false;

        // Check rollbackFor (classes)
        for (Class<? extends Throwable> rollbackClass : txAnn.rollbackFor()) {
            int depth = getDepth(t.getClass(), rollbackClass);
            if (depth >= 0 && depth < bestDepth) {
                bestDepth = depth;
                winnerIsRollback = true;
            }
        }

        // Check rollbackForClassName (strings)
        for (String rollbackClassName : txAnn.rollbackForClassName()) {
            int depth = getDepth(t.getClass(), rollbackClassName);
            if (depth >= 0 && depth < bestDepth) {
                bestDepth = depth;
                winnerIsRollback = true;
            }
        }

        // Check noRollbackFor (classes)
        for (Class<? extends Throwable> noRollbackClass : txAnn.noRollbackFor()) {
            int depth = getDepth(t.getClass(), noRollbackClass);
            if (depth >= 0 && depth < bestDepth) {
                bestDepth = depth;
                winnerIsRollback = false;
            }
        }

        // Check noRollbackForClassName (strings)
        for (String noRollbackClassName : txAnn.noRollbackForClassName()) {
            int depth = getDepth(t.getClass(), noRollbackClassName);
            if (depth >= 0 && depth < bestDepth) {
                bestDepth = depth;
                winnerIsRollback = false;
            }
        }

        if (bestDepth != Integer.MAX_VALUE) {
            return winnerIsRollback;
        }
        return null;
    }

    static boolean isJakartaRollbackFor(Annotation jakartaAnn, Throwable t) {
        if (jakartaAnn == null || t == null) {
            return false;
        }
        Boolean explicitRollback = evaluateJakartaRollbackRules(jakartaAnn, t);
        if (explicitRollback != null) {
            return explicitRollback;
        }
        if (isJakartaSupportsPropagation(jakartaAnn)) {
            return true;
        }
        return (t instanceof RuntimeException) || (t instanceof Error);
    }

    private static boolean isJakartaSupportsPropagation(Annotation jakartaTx) {
        try {
            Object value = AnnotationUtils.getValue(jakartaTx, "value");
            if (value != null && "SUPPORTS".equalsIgnoreCase(value.toString())) {
                return true;
            }
        } catch (Throwable ignored) {
        }
        return false;
    }

    @SuppressWarnings("SpellCheckingInspection")
    private static Boolean evaluateJakartaRollbackRules(Annotation jakartaAnn, Throwable t) {
        int bestDepth = Integer.MAX_VALUE;
        boolean winnerIsRollback = false;

        try {
            Class<?>[] rollbackOn = (Class<?>[]) AnnotationUtils.getValue(jakartaAnn, "rollbackOn");
            if (rollbackOn != null) {
                for (Class<?> rollbackClass : rollbackOn) {
                    int depth = getDepth(t.getClass(), rollbackClass);
                    if (depth >= 0 && depth < bestDepth) {
                        bestDepth = depth;
                        winnerIsRollback = true;
                    }
                }
            }

            Class<?>[] noRollbackOn = (Class<?>[]) AnnotationUtils.getValue(jakartaAnn, "dontRollbackOn");
            if (noRollbackOn != null) {
                for (Class<?> noRollbackClass : noRollbackOn) {
                    int depth = getDepth(t.getClass(), noRollbackClass);
                    if (depth >= 0 && depth < bestDepth) {
                        bestDepth = depth;
                        winnerIsRollback = false;
                    }
                }
            }
        } catch (Throwable ignored) {
        }

        if (bestDepth != Integer.MAX_VALUE) {
            return winnerIsRollback;
        }
        return null;
    }

    static int getDepth(Class<?> exceptionClass, Class<?> ruleClass) {
        if (ruleClass == null || exceptionClass == null
                || ruleClass == Object.class || ruleClass == Serializable.class
                || !ruleClass.isAssignableFrom(exceptionClass)) {
            return -1;
        }
        Queue<Class<?>> queue = new ArrayDeque<>();
        Set<Class<?>> visited = new HashSet<>();
        queue.add(exceptionClass);
        visited.add(exceptionClass);

        int depth = 0;
        int maxDepth = 32;

        while (!queue.isEmpty() && depth < maxDepth) {
            int levelSize = queue.size();
            for (int i = 0; i < levelSize; i++) {
                Class<?> clazz = queue.poll();
                if (clazz == ruleClass) {
                    return depth;
                }
                if (clazz != null) {
                    Class<?> superclass = clazz.getSuperclass();
                    if (superclass != null && superclass != Object.class && visited.add(superclass)) {
                        queue.add(superclass);
                    }
                    for (Class<?> implementedInterface : clazz.getInterfaces()) {
                        if (implementedInterface != Serializable.class && visited.add(implementedInterface)) {
                            queue.add(implementedInterface);
                        }
                    }
                }
            }
            depth++;
        }
        return -1;
    }

    static int getDepth(Class<?> exceptionClass, String ruleClassName) {
        if (ruleClassName == null || ruleClassName.isBlank() || exceptionClass == null) {
            return -1;
        }
        String target = ruleClassName.trim();
        String targetDot = target.replace('$', '.');
        if ("Object".equals(target) || "java.lang.Object".equals(targetDot)
                || "Serializable".equals(target) || "java.io.Serializable".equals(targetDot)) {
            return -1;
        }
        Queue<Class<?>> queue = new ArrayDeque<>();
        Set<Class<?>> visited = new HashSet<>();
        queue.add(exceptionClass);
        visited.add(exceptionClass);

        int depth = 0;
        int maxDepth = 32;
        int firstSubstringDepth = -1;

        while (!queue.isEmpty() && depth < maxDepth) {
            int levelSize = queue.size();
            for (int i = 0; i < levelSize; i++) {
                Class<?> clazz = queue.poll();
                if (clazz == null) {
                    continue;
                }
                if (matchesExactRuleClassName(clazz, target, targetDot)) {
                    return depth;
                }
                if (firstSubstringDepth == -1 && matchesRuleClassName(clazz, target, targetDot)) {
                    firstSubstringDepth = depth;
                }
                Class<?> superclass = clazz.getSuperclass();
                if (superclass != null && superclass != Object.class && visited.add(superclass)) {
                    queue.add(superclass);
                }
                for (Class<?> implementedInterface : clazz.getInterfaces()) {
                    if (implementedInterface != Serializable.class && visited.add(implementedInterface)) {
                        queue.add(implementedInterface);
                    }
                }
            }
            depth++;
        }
        return firstSubstringDepth;
    }

    private static boolean matchesExactRuleClassName(Class<?> clazz, String target, String targetDot) {
        String className = clazz.getName();
        String classNameDot = className.replace('$', '.');
        if (className.equals(target) || classNameDot.equals(targetDot) || clazz.getSimpleName().equals(target)) {
            return true;
        }
        String pkg = clazz.getPackageName();
        String nameWithoutPkg = pkg.isEmpty() ? className : className.substring(pkg.length() + 1);
        String nameWithoutPkgDot = nameWithoutPkg.replace('$', '.');
        if (nameWithoutPkg.equals(target) || nameWithoutPkgDot.equals(targetDot)) {
            return true;
        }
        if (!pkg.isEmpty() && targetDot.length() > pkg.length() + 1 && classNameDot.startsWith(targetDot + ".")) {
            return true;
        }
        return classNameDot.endsWith("." + targetDot) && targetDot.endsWith(clazz.getSimpleName());
    }

    private static boolean matchesRuleClassName(Class<?> clazz, String target, String targetDot) {
        if (matchesExactRuleClassName(clazz, target, targetDot)) {
            return true;
        }
        String className = clazz.getName();
        String pkg = clazz.getPackageName();
        String nameWithoutPkg = pkg.isEmpty() ? className : className.substring(pkg.length() + 1);
        String nameWithoutPkgDot = nameWithoutPkg.replace('$', '.');
        return nameWithoutPkg.contains(target) || nameWithoutPkgDot.contains(targetDot);
    }
}
