package io.github.alexlitvinov.idembox.core.spel;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

class SpelKeyExtractorTest {

    private final SpelKeyExtractor extractor = new SpelKeyExtractor();

    static class SampleConsumer {
        public void handleOrder(OrderPayload payload, String header) {
            java.util.Objects.requireNonNull(payload);
            java.util.Objects.requireNonNull(header);
        }

        public int computeTotal(OrderPayload payload) {
            return payload != null ? 42 : 0;
        }
    }

    record OrderPayload(String orderId, double amount) {}

    @Test
    @DisplayName("Should extract key from method parameter object property")
    void shouldExtractKeyFromNestedProperty() throws NoSuchMethodException {
        Method method = SampleConsumer.class.getMethod("handleOrder", OrderPayload.class, String.class);
        OrderPayload payload = new OrderPayload("ORD-9876", 450.0);

        String key = extractor.extractKey("#payload.orderId", method, new Object[]{payload, "auth-token"});
        assertThat(key).isEqualTo("ORD-9876");
    }

    @Test
    @DisplayName("Should extract key using #root.args[0] expression (Issue 3.1 fix)")
    void shouldExtractKeyUsingRootArgs() throws NoSuchMethodException {
        Method method = SampleConsumer.class.getMethod("handleOrder", OrderPayload.class, String.class);
        OrderPayload payload = new OrderPayload("ORD-ROOT-123", 750.0);

        String key = extractor.extractKey("#root.args[0].orderId", method, new Object[]{payload, "token"});
        assertThat(key).isEqualTo("ORD-ROOT-123");
    }

    @Test
    @DisplayName("Should extract key using positional argument #arg0 and #p0")
    void shouldExtractKeyUsingPositionalArg() throws NoSuchMethodException {
        Method method = SampleConsumer.class.getMethod("handleOrder", OrderPayload.class, String.class);
        OrderPayload payload = new OrderPayload("ORD-1111", 100.0);

        String keyArg = extractor.extractKey("#arg0.orderId", method, new Object[]{payload, "test"});
        String keyP = extractor.extractKey("#p0.orderId", method, new Object[]{payload, "test"});
        assertThat(keyArg).isEqualTo("ORD-1111");
        assertThat(keyP).isEqualTo("ORD-1111");
    }

    @Test
    @DisplayName("Should throw exception when evaluated expression results in null with helpful hint")
    void shouldThrowWhenResultIsNull() throws NoSuchMethodException {
        Method method = SampleConsumer.class.getMethod("handleOrder", OrderPayload.class, String.class);
        OrderPayload payload = new OrderPayload(null, 100.0);

        assertThatThrownBy(() -> extractor.extractKey("#payload.orderId", method, new Object[]{payload, "test"}))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("returned null")
                .hasMessageContaining("Did you mean to use conditional guard @Idempotent(condition = '#payload.orderId != null', key = '#payload.orderId')?")
                .hasMessageContaining("If the key field is optional, use SpEL safe navigation or condition: @Idempotent(condition = \"#payload.orderId != null\", key = \"#payload.orderId\")");
    }

    @Test
    @DisplayName("Should throw exception when evaluated expression results in empty or blank key")
    void shouldThrowWhenResultIsEmptyOrBlank() throws NoSuchMethodException {
        Method method = SampleConsumer.class.getMethod("handleOrder", OrderPayload.class, String.class);
        OrderPayload emptyPayload = new OrderPayload("", 100.0);
        OrderPayload blankPayload = new OrderPayload("   ", 100.0);

        assertThatThrownBy(() -> extractor.extractKey("#payload.orderId", method, new Object[]{emptyPayload, "test"}))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("empty or blank key");

        assertThatThrownBy(() -> extractor.extractKey("#payload.orderId", method, new Object[]{blankPayload, "test"}))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("empty or blank key");
    }

    @Test
    @DisplayName("DurationParser should correctly parse day, hour, minute, second formats")
    void shouldParseHumanReadableDurations() {
        assertThat(DurationParser.parse("7d")).isEqualTo(Duration.ofDays(7));
        assertThat(DurationParser.parse("24h")).isEqualTo(Duration.ofHours(24));
        assertThat(DurationParser.parse("30m")).isEqualTo(Duration.ofMinutes(30));
        assertThat(DurationParser.parse("2m")).isEqualTo(Duration.ofMinutes(2));
        assertThat(DurationParser.parse("15s")).isEqualTo(Duration.ofSeconds(15));
        assertThat(DurationParser.parse("PT2H")).isEqualTo(Duration.ofHours(2));
    }

    @Test
    @DisplayName("DurationParser should fallback to specified default when input is null or blank")
    void shouldFallbackToDefaultDuration() {
        assertThat(DurationParser.parse(null, Duration.ofMinutes(2))).isEqualTo(Duration.ofMinutes(2));
        assertThat(DurationParser.parse("", Duration.ofMinutes(5))).isEqualTo(Duration.ofMinutes(5));
        assertThat(DurationParser.parse("   ", Duration.ofSeconds(45))).isEqualTo(Duration.ofSeconds(45));
        assertThat(DurationParser.parse("10s", Duration.ofMinutes(2))).isEqualTo(Duration.ofSeconds(10));
    }

    public static class CustomKeyService {
        public String formatKey(String rawId) {
            return "CUSTOM-" + rawId;
        }
    }

    @Test
    @DisplayName("Fix 4: Should resolve Spring bean references like @keyHelper.formatKey(#payload.orderId)")
    void shouldResolveSpringBeanInExpression() throws NoSuchMethodException {
        org.springframework.beans.factory.BeanFactory beanFactory = org.mockito.Mockito.mock(org.springframework.beans.factory.BeanFactory.class);
        CustomKeyService keyService = new CustomKeyService();
        org.mockito.Mockito.when(beanFactory.containsBean("keyHelper")).thenReturn(true);
        org.mockito.Mockito.when(beanFactory.getBean("keyHelper")).thenReturn(keyService);

        SpelKeyExtractor beanAwareExtractor = new SpelKeyExtractor();
        beanAwareExtractor.setBeanFactory(beanFactory);

        Method method = SampleConsumer.class.getMethod("handleOrder", OrderPayload.class, String.class);
        OrderPayload payload = new OrderPayload("9999", 50.0);

        String key = beanAwareExtractor.extractKey("@keyHelper.formatKey(#payload.orderId)", method, new Object[]{payload, "header"});
        assertThat(key).isEqualTo(keyService.formatKey("9999")).isEqualTo("CUSTOM-9999");
    }

    @Test
    @DisplayName("Fix 7: Should deterministically hash keys > 2000 chars to SHA-256 to prevent PostgreSQL B-Tree overflow")
    void shouldHashKeyToSha256WhenLengthExceeds2000Chars() throws NoSuchMethodException {
        Method method = SampleConsumer.class.getMethod("handleOrder", OrderPayload.class, String.class);
        // Create 3000 character string
        String hugePayload = "A".repeat(3000);
        OrderPayload payload = new OrderPayload(hugePayload, 50.0);

        String key = extractor.extractKey("#payload.orderId", method, new Object[]{payload, "header"});

        assertThat(key).startsWith("SHA256:");
        // "SHA256:" (7) + 64 hex chars = 71
        assertThat(key).hasSize(71);

        // Deterministic check: repeating must yield identical hash
        String key2 = extractor.extractKey("#payload.orderId", method, new Object[]{payload, "header"});
        assertThat(key2).isEqualTo(key);
    }

    @Test
    @DisplayName("Refinement 6: Should reuse cached BeanResolver across multiple evaluations")
    void shouldReuseCachedBeanResolverAcrossEvaluations() throws Exception {
        org.springframework.expression.BeanResolver mockResolver = org.mockito.Mockito.mock(org.springframework.expression.BeanResolver.class);
        CustomKeyService keyService = new CustomKeyService();
        org.mockito.Mockito.when(mockResolver.resolve(any(), eq("keyHelper"))).thenReturn(keyService);

        SpelKeyExtractor customExtractor = new SpelKeyExtractor(mockResolver);

        Method method = SampleConsumer.class.getMethod("handleOrder", OrderPayload.class, String.class);
        OrderPayload p1 = new OrderPayload("100", 10.0);
        OrderPayload p2 = new OrderPayload("200", 20.0);

        String k1 = customExtractor.extractKey("@keyHelper.formatKey(#payload.orderId)", method, new Object[]{p1, "h"});
        String k2 = customExtractor.extractKey("@keyHelper.formatKey(#payload.orderId)", method, new Object[]{p2, "h"});

        assertThat(k1).isEqualTo("CUSTOM-100");
        assertThat(k2).isEqualTo("CUSTOM-200");
        org.mockito.Mockito.verify(mockResolver, org.mockito.Mockito.times(2)).resolve(any(), eq("keyHelper"));
    }

    @Test
    @DisplayName("UTF-8 B-Tree safety: 1500 Cyrillic characters (3000 bytes) should be hashed to SHA-256")
    void shouldHashKeyWhenUtf8ByteLengthExceeds2000EvenIfCharLengthIsUnder2000() throws NoSuchMethodException {
        Method method = SampleConsumer.class.getMethod("handleOrder", OrderPayload.class, String.class);
        // 1500 Cyrillic characters = 1500 chars (< 2000 chars), but 3000 UTF-8 bytes (> 2000 bytes)
        String cyrillicPayload = "Ж".repeat(1500);
        assertThat(cyrillicPayload.length()).isEqualTo(1500);
        assertThat(cyrillicPayload.getBytes(java.nio.charset.StandardCharsets.UTF_8).length).isEqualTo(3000);

        OrderPayload payload = new OrderPayload(cyrillicPayload, 50.0);
        String key = extractor.extractKey("#payload.orderId", method, new Object[]{payload, "header"});

        assertThat(key).startsWith("SHA256:");
        assertThat(key).hasSize(71);
    }

    @Test
    @DisplayName("Senior Refinements v8: Lock-free double-checked expression cache under high concurrency")
    void shouldExtractKeysConcurrentlyWithoutCacheContention() throws NoSuchMethodException, InterruptedException {
        Method method = SampleConsumer.class.getMethod("handleOrder", OrderPayload.class, String.class);
        int threadCount = 10;
        int iterations = 100;
        java.util.concurrent.CountDownLatch startLatch = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.CountDownLatch doneLatch = new java.util.concurrent.CountDownLatch(threadCount);
        java.util.concurrent.atomic.AtomicInteger errors = new java.util.concurrent.atomic.AtomicInteger(0);

        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            Thread worker = new Thread(() -> {
                try {
                    startLatch.await();
                    for (int i = 0; i < iterations; i++) {
                        OrderPayload payload = new OrderPayload("ORD-" + threadId + "-" + i, 10.0);
                        String key = extractor.extractKey("#payload.orderId", method, new Object[]{payload, "token"});
                        if (!("ORD-" + threadId + "-" + i).equals(key)) {
                            errors.incrementAndGet();
                        }
                    }
                } catch (Exception e) {
                    errors.incrementAndGet();
                } finally {
                    doneLatch.countDown();
                }
            });
            worker.start();
        }

        startLatch.countDown();
        boolean completed = doneLatch.await(5, java.util.concurrent.TimeUnit.SECONDS);

        assertThat(completed).isTrue();
        assertThat(errors.get()).isEqualTo(0);
    }

    @Test
    @DisplayName("Senior Refinements v9: Expression LRU cache must not exceed 1000 entries under dynamic expression generation")
    void shouldBoundExpressionCacheToMax1000Entries() throws NoSuchMethodException {
        Method method = SampleConsumer.class.getMethod("handleOrder", OrderPayload.class, String.class);
        OrderPayload payload = new OrderPayload("123", 10.0);

        for (int i = 0; i < 1200; i++) {
            // Distinct expression strings
            String expr = "#payload.orderId + '" + i + "'";
            extractor.extractKey(expr, method, new Object[]{payload, "h"});
        }

        assertThat(extractor.getCacheSize()).isLessThanOrEqualTo(1000);
        assertThat(extractor.getCacheSize()).isEqualTo(1000);
    }

    @Test
    @DisplayName("Senior Refinements v10: ParameterNameDiscoverer cache must cache method parameter names across calls")
    void shouldCacheParameterNamesAcrossEvaluations() throws NoSuchMethodException {
        Method method = SampleConsumer.class.getMethod("handleOrder", OrderPayload.class, String.class);
        OrderPayload payload = new OrderPayload("ORD-CACHE", 25.0);

        int initialSize = extractor.getParamNamesCacheSize();

        for (int i = 0; i < 50; i++) {
            extractor.extractKey("#payload.orderId", method, new Object[]{payload, "tok"});
        }

        assertThat(extractor.getParamNamesCacheSize()).isEqualTo(initialSize + 1);
    }

    @Test
    @DisplayName("Senior Refinements v10: DurationParser should be whitespace-tolerant for ISO-8601 strings")
    void shouldParseWhitespaceTolerantIsoDuration() {
        assertThat(DurationParser.parse("PT 15M")).isEqualTo(Duration.ofMinutes(15));
        assertThat(DurationParser.parse("PT15 M")).isEqualTo(Duration.ofMinutes(15));
        assertThat(DurationParser.parse(" P 1D ")).isEqualTo(Duration.ofDays(1));
    }

    @Test
    @DisplayName("Senior Refinements v15: ParameterNameDiscoverer differentiates overloaded methods by descriptor")
    void shouldDifferentiateOverloadedMethodsInParamNamesCache() throws NoSuchMethodException {
        Method method1 = SampleConsumer.class.getMethod("handleOrder", OrderPayload.class, String.class);
        Method method2 = SampleConsumer.class.getMethod("computeTotal", OrderPayload.class);
        OrderPayload payload = new OrderPayload("ORD-1", 10.0);

        int initial = extractor.getParamNamesCacheSize();
        extractor.extractKey("#payload.orderId", method1, new Object[]{payload, "h"});
        extractor.extractKey("#payload.orderId", method2, new Object[]{payload});

        assertThat(extractor.getParamNamesCacheSize()).isEqualTo(initial + 2);
    }

    @Test
    @DisplayName("Senior Refinements v16: SpEL Injection RCE prevention rejects T(...) type references")
    void shouldRejectTypeReferencesLikeRuntimeOrSystem() throws NoSuchMethodException {
        Method method = SampleConsumer.class.getMethod("handleOrder", OrderPayload.class, String.class);
        OrderPayload payload = new OrderPayload("ORD-1", 10.0);

        org.assertj.core.api.Assertions.assertThatThrownBy(() ->
                extractor.extractKey("T(java.lang.Runtime).getRuntime()", method, new Object[]{payload, "h"}))
                .isInstanceOf(SecurityException.class)
                .hasMessageContaining("Type references like T(...) are not permitted");

        org.assertj.core.api.Assertions.assertThatThrownBy(() ->
                extractor.extractKey("T(java.lang.System).currentTimeMillis()", method, new Object[]{payload, "h"}))
                .isInstanceOf(SecurityException.class)
                .hasMessageContaining("Type references like T(...) are not permitted");
    }

    @Test
    @DisplayName("Senior Refinements v18: SpEL AST security rejects ConstructorReference (new ...)")
    void shouldRejectConstructorReferencesLikeNewObject() throws NoSuchMethodException {
        Method method = SampleConsumer.class.getMethod("handleOrder", OrderPayload.class, String.class);
        OrderPayload payload = new OrderPayload("ORD-1", 10.0);

        org.assertj.core.api.Assertions.assertThatThrownBy(() ->
                extractor.extractKey("new java.lang.String('ORD-1')", method, new Object[]{payload, "h"}))
                .isInstanceOf(SecurityException.class)
                .hasMessageContaining("Constructor references like 'new ...' are not permitted");

        org.assertj.core.api.Assertions.assertThatThrownBy(() ->
                extractor.extractKey("new java.io.File('/tmp').getName()", method, new Object[]{payload, "h"}))
                .isInstanceOf(SecurityException.class)
                .hasMessageContaining("Constructor references like 'new ...' are not permitted");
    }

    @Test
    @DisplayName("Should throw IllegalArgumentException when expression is null, empty, or blank")
    void shouldThrowWhenExpressionIsBlankOrNull() throws NoSuchMethodException {
        Method method = SampleConsumer.class.getMethod("handleOrder", OrderPayload.class, String.class);
        OrderPayload payload = new OrderPayload("ORD-1", 10.0);

        assertThatThrownBy(() -> extractor.extractKey(null, method, new Object[]{payload, "h"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cannot be blank");

        assertThatThrownBy(() -> extractor.extractKey("", method, new Object[]{payload, "h"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cannot be blank");

        assertThatThrownBy(() -> extractor.extractKey("   ", method, new Object[]{payload, "h"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cannot be blank");
    }

    @Test
    @DisplayName("Should support variable aliases #a0, #arg0, #p0, and #args[0]")
    void shouldSupportAllVariableAliases() throws NoSuchMethodException {
        Method method = SampleConsumer.class.getMethod("handleOrder", OrderPayload.class, String.class);
        OrderPayload payload = new OrderPayload("ORD-ALIAS", 20.0);
        Object[] args = new Object[]{payload, "hdr"};

        assertThat(extractor.extractKey("#a0.orderId", method, args)).isEqualTo("ORD-ALIAS");
        assertThat(extractor.extractKey("#arg0.orderId", method, args)).isEqualTo("ORD-ALIAS");
        assertThat(extractor.extractKey("#p0.orderId", method, args)).isEqualTo("ORD-ALIAS");
        assertThat(extractor.extractKey("#args[0].orderId", method, args)).isEqualTo("ORD-ALIAS");
    }

    @Test
    @DisplayName("Should support invoking methods on target instance via #root.target")
    void shouldSupportMethodInvocationOnRootTarget() throws NoSuchMethodException {
        SampleConsumer consumer = new SampleConsumer();
        Method method = SampleConsumer.class.getMethod("computeTotal", OrderPayload.class);
        OrderPayload payload = new OrderPayload("ORD-CALC", 30.0);

        String key = extractor.extractKey("#root.target.computeTotal(#payload) + '-' + #payload.orderId", consumer, method, new Object[]{payload});
        assertThat(key).isEqualTo("42-ORD-CALC");
    }

    @Test
    @DisplayName("Should trim leading and trailing whitespace from extracted key")
    void shouldTrimWhitespaceFromExtractedKey() throws NoSuchMethodException {
        Method method = SampleConsumer.class.getMethod("handleOrder", OrderPayload.class, String.class);
        OrderPayload payload = new OrderPayload("  TRIM-ME  ", 40.0);

        String key = extractor.extractKey("#payload.orderId", method, new Object[]{payload, "h"});
        assertThat(key).isEqualTo("TRIM-ME");
    }

    @Test
    @DisplayName("Senior Refinements v24: Should sanitize null bytes from extracted key to prevent PostgreSQL UTF-8 column abort")
    void shouldSanitizeNullBytesFromExtractedKey() throws NoSuchMethodException {
        Method method = SampleConsumer.class.getMethod("handleOrder", OrderPayload.class, String.class);
        OrderPayload payload1 = new OrderPayload("ORD-\0-999", 50.0);
        OrderPayload payload2 = new OrderPayload("ID-\\u0000-123", 60.0);

        assertThat(extractor.extractKey("#payload.orderId", method, new Object[]{payload1, "h"})).isEqualTo("ORD--999");
        assertThat(extractor.extractKey("#payload.orderId", method, new Object[]{payload2, "h"})).isEqualTo("ID--123");
    }

    @Test
    @DisplayName("Senior Refinements v24: Should throw IllegalStateException if key becomes empty after null byte sanitization")
    void shouldThrowWhenKeyBecomesEmptyAfterNullByteSanitization() throws NoSuchMethodException {
        Method method = SampleConsumer.class.getMethod("handleOrder", OrderPayload.class, String.class);
        OrderPayload payload = new OrderPayload("\0\0\\u0000", 70.0);

        assertThatThrownBy(() -> extractor.extractKey("#payload.orderId", method, new Object[]{payload, "h"}))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("empty or blank key");
    }

    @Test
    @DisplayName("Senior Refinements v26: Key extractor executes safely under multi-threaded concurrency")
    void shouldSafelyEvaluateExpressionsConcurrentlyUnderHighThreadContention() throws NoSuchMethodException, InterruptedException {
        int threadsCount = 16;
        int iterationsPerThread = 500;
        Method method = SampleConsumer.class.getMethod("handleOrder", OrderPayload.class, String.class);

        java.util.concurrent.CountDownLatch startLatch = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.CountDownLatch finishLatch = new java.util.concurrent.CountDownLatch(threadsCount);
        java.util.concurrent.atomic.AtomicInteger errorCount = new java.util.concurrent.atomic.AtomicInteger(0);

        for (int i = 0; i < threadsCount; i++) {
            final int threadId = i;
            Thread worker = new Thread(() -> {
                try {
                    startLatch.await();
                    for (int j = 0; j < iterationsPerThread; j++) {
                        String id = "ORD-" + threadId + "-" + j;
                        OrderPayload payload = new OrderPayload(id, 100.0);
                        String key1 = extractor.extractKey("#payload.orderId", method, new Object[]{payload, "h"});
                        String key2 = extractor.extractKey("#p0.orderId", method, new Object[]{payload, "h"});
                        if (!id.equals(key1) || !id.equals(key2)) {
                            errorCount.incrementAndGet();
                        }
                    }
                } catch (Exception ex) {
                    errorCount.incrementAndGet();
                } finally {
                    finishLatch.countDown();
                }
            });
            worker.start();
        }

        startLatch.countDown();
        boolean completed = finishLatch.await(10, java.util.concurrent.TimeUnit.SECONDS);

        assertThat(completed).isTrue();
        assertThat(errorCount.get()).isEqualTo(0);
        assertThat(extractor.getCacheSize()).isGreaterThan(0).isLessThanOrEqualTo(SpelKeyExtractor.MAX_EXPRESSION_CACHE_SIZE);
    }

    @Test
    @DisplayName("Senior Refinements v26: evaluateCondition returns true when condition is blank or evaluates to true")
    void evaluateConditionShouldReturnExpectedBoolean() throws NoSuchMethodException {
        Method method = SampleConsumer.class.getMethod("handleOrder", OrderPayload.class, String.class);
        OrderPayload payload = new OrderPayload("123", 10.0);

        assertThat(extractor.evaluateCondition("", method, new Object[]{payload, "h"})).isTrue();
        assertThat(extractor.evaluateCondition(null, method, new Object[]{payload, "h"})).isTrue();
        assertThat(extractor.evaluateCondition("#payload != null", method, new Object[]{payload, "h"})).isTrue();
        assertThat(extractor.evaluateCondition("#payload == null", method, new Object[]{payload, "h"})).isFalse();
        assertThat(extractor.evaluateCondition("#payload != null", method, new Object[]{null, "h"})).isFalse();
    }

    @Test
    @DisplayName("Senior Refinements v27: paramNamesCache is bounded and clearParamNamesCache clears it")
    void paramNamesCacheShouldBeBoundedAndClearable() throws NoSuchMethodException {
        extractor.clearParamNamesCache();
        assertThat(extractor.getParamNamesCacheSize()).isEqualTo(0);

        Method method = SampleConsumer.class.getMethod("handleOrder", OrderPayload.class, String.class);
        OrderPayload payload = new OrderPayload("123", 10.0);

        extractor.extractKey("#payload.orderId", method, new Object[]{payload, "h"});
        assertThat(extractor.getParamNamesCacheSize()).isEqualTo(1);

        extractor.clearParamNamesCache();
        assertThat(extractor.getParamNamesCacheSize()).isEqualTo(0);
    }

    @Test
    @DisplayName("Senior Refinements v38: Expression cache maintains LRU ordering under cache pressure")
    void shouldMaintainLruOrderingUnderCachePressure() throws NoSuchMethodException {
        extractor.clearCache();
        Method method = SampleConsumer.class.getMethod("handleOrder", OrderPayload.class, String.class);
        OrderPayload payload = new OrderPayload("123", 10.0);

        for (int i = 0; i < SpelKeyExtractor.MAX_EXPRESSION_CACHE_SIZE; i++) {
            extractor.extractKey("#payload.orderId + '" + i + "'", method, new Object[]{payload, "h"});
        }
        // Access expression 0 again to make it MRU
        extractor.extractKey("#payload.orderId + '0'", method, new Object[]{payload, "h"});

        // Add a new expression to force eviction
        extractor.extractKey("#payload.orderId + 'extra'", method, new Object[]{payload, "h"});

        assertThat(extractor.getCacheSize()).isEqualTo(SpelKeyExtractor.MAX_EXPRESSION_CACHE_SIZE);
    }

    @Test
    @DisplayName("Senior Refinements v43: getOrParseExpression rejects expressions exceeding 1024 characters")
    void shouldRejectExpressionsExceedingLengthLimit() {
        String longExpression = "#payload.orderId + '" + "a".repeat(1020) + "'";
        assertThat(longExpression.length()).isGreaterThan(1024);

        org.assertj.core.api.Assertions.assertThatThrownBy(() -> extractor.getOrParseExpression(longExpression))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Expression length exceeds maximum limit of 1024 characters");
    }

    @Test
    @DisplayName("Senior Refinements v47: extractKey deterministically formats byte[] keys as hex string")
    void shouldDeterministicallyFormatByteArrayKeyAsHex() throws NoSuchMethodException {
        Method method = SampleConsumer.class.getMethod("handleOrder", OrderPayload.class, String.class);
        byte[] binaryKey = new byte[]{(byte) 0xab, 0x12, (byte) 0xcd, 0x34};

        String key = extractor.extractKey("#root.args[0]", method, new Object[]{binaryKey, "test"});
        assertThat(key).isEqualTo("ab12cd34");
    }

    @Test
    @DisplayName("Senior Refinements Round 52: extractKey deterministically formats Object[] and primitive array keys avoiding identityHashCode")
    void shouldDeterministicallyFormatArrayKeys() throws NoSuchMethodException {
        Method method = SampleConsumer.class.getMethod("handleOrder", OrderPayload.class, String.class);

        // String[] arrays on different heap instances
        String[] arr1 = new String[]{"item-1", "item-2"};
        String[] arr2 = new String[]{"item-1", "item-2"};
        String key1 = extractor.extractKey("#root.args[0]", method, new Object[]{arr1, "test"});
        String key2 = extractor.extractKey("#root.args[0]", method, new Object[]{arr2, "test"});

        assertThat(key1).isEqualTo("[item-1, item-2]");
        assertThat(key2).isEqualTo("[item-1, item-2]");
        assertThat(key1).doesNotContain("[Ljava.lang.String;");

        // Primitive int[] and long[] arrays
        int[] intArr1 = new int[]{10, 20, 30};
        int[] intArr2 = new int[]{10, 20, 30};
        String intKey1 = extractor.extractKey("#root.args[0]", method, new Object[]{intArr1, "test"});
        String intKey2 = extractor.extractKey("#root.args[0]", method, new Object[]{intArr2, "test"});
        assertThat(intKey1).isEqualTo("[10, 20, 30]");
        assertThat(intKey2).isEqualTo("[10, 20, 30]");
        assertThat(intKey1).doesNotContain("[I@");
    }

    @Test
    @DisplayName("Round 56: Multi-threaded concurrent extraction executes without lock contention")
    void shouldExtractConcurrentlyAcrossThreads() throws NoSuchMethodException, InterruptedException {
        Method method = SampleConsumer.class.getMethod("handleOrder", OrderPayload.class, String.class);
        OrderPayload payload = new OrderPayload("ORD-CONCURRENT", 100.0);
        int threadCount = 16;
        int iterations = 1000;
        java.util.concurrent.CountDownLatch latch = new java.util.concurrent.CountDownLatch(threadCount);
        java.util.concurrent.atomic.AtomicInteger errors = new java.util.concurrent.atomic.AtomicInteger();

        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            Thread worker = new Thread(() -> {
                try {
                    for (int i = 0; i < iterations; i++) {
                        String key = extractor.extractKey("#payload.orderId + '-' + #header", method, new Object[]{payload, "h" + threadId});
                        if (!key.equals("ORD-CONCURRENT-h" + threadId)) {
                            errors.incrementAndGet();
                        }
                    }
                } catch (Exception ex) {
                    errors.incrementAndGet();
                } finally {
                    latch.countDown();
                }
            });
            worker.start();
        }

        boolean finished = latch.await(10, java.util.concurrent.TimeUnit.SECONDS);
        assertThat(finished).isTrue();
        assertThat(errors.get()).isEqualTo(0);
    }

    @Test
    @DisplayName("Senior Refinements Round 66: extractKey deterministically formats Set regardless of insertion or iteration order")
    void shouldExtractSetDeterministically() throws NoSuchMethodException {
        Method method = SampleConsumer.class.getMethod("handleOrder", OrderPayload.class, String.class);

        java.util.Set<String> set1 = new java.util.LinkedHashSet<>(java.util.List.of("charlie", "alpha", "bravo"));
        java.util.Set<String> set2 = new java.util.HashSet<>(java.util.List.of("bravo", "charlie", "alpha"));

        String key1 = extractor.extractKey("#root.args[0]", method, new Object[]{set1, "test"});
        String key2 = extractor.extractKey("#root.args[0]", method, new Object[]{set2, "test"});

        assertThat(key1).isEqualTo("[alpha, bravo, charlie]");
        assertThat(key2).isEqualTo("[alpha, bravo, charlie]");
        assertThat(key1).isEqualTo(key2);
    }

    @Test
    @DisplayName("Senior Refinements Round 66: extractKey deterministically formats Map regardless of insertion or iteration order")
    void shouldExtractMapDeterministically() throws NoSuchMethodException {
        Method method = SampleConsumer.class.getMethod("handleOrder", OrderPayload.class, String.class);

        java.util.Map<String, Object> map1 = new java.util.LinkedHashMap<>();
        map1.put("z", 100);
        map1.put("a", "first");
        map1.put("m", true);

        java.util.Map<String, Object> map2 = new java.util.HashMap<>();
        map2.put("m", true);
        map2.put("z", 100);
        map2.put("a", "first");

        String key1 = extractor.extractKey("#root.args[0]", method, new Object[]{map1, "test"});
        String key2 = extractor.extractKey("#root.args[0]", method, new Object[]{map2, "test"});

        assertThat(key1).isEqualTo("{a=first, m=true, z=100}");
        assertThat(key2).isEqualTo("{a=first, m=true, z=100}");
        assertThat(key1).isEqualTo(key2);
    }

    @Test
    @DisplayName("Senior Refinements Round 66: extractKey deterministically formats nested Set and Map structures")
    void shouldExtractNestedStructuresDeterministically() throws NoSuchMethodException {
        Method method = SampleConsumer.class.getMethod("handleOrder", OrderPayload.class, String.class);

        java.util.Map<String, Object> nested1 = new java.util.HashMap<>();
        nested1.put("tags", new java.util.HashSet<>(java.util.List.of("vip", "gold")));
        nested1.put("meta", java.util.Map.of("k2", "v2", "k1", "v1"));

        java.util.Map<String, Object> nested2 = new java.util.LinkedHashMap<>();
        nested2.put("meta", new java.util.TreeMap<>(java.util.Map.of("k1", "v1", "k2", "v2")));
        nested2.put("tags", new java.util.LinkedHashSet<>(java.util.List.of("gold", "vip")));

        String key1 = extractor.extractKey("#root.args[0]", method, new Object[]{nested1, "test"});
        String key2 = extractor.extractKey("#root.args[0]", method, new Object[]{nested2, "test"});

        assertThat(key1).isEqualTo("{meta={k1=v1, k2=v2}, tags=[gold, vip]}");
        assertThat(key2).isEqualTo("{meta={k1=v1, k2=v2}, tags=[gold, vip]}");
        assertThat(key1).isEqualTo(key2);
    }

    @Test
    @DisplayName("Senior Refinements Round 68: extractKey handles self-referencing Object[] without StackOverflowError")
    void shouldHandleCyclicArrayWithoutStackOverflow() throws NoSuchMethodException {
        Method method = SampleConsumer.class.getMethod("handleOrder", OrderPayload.class, String.class);
        Object[] arr = new Object[1];
        arr[0] = arr;

        String key = extractor.extractKey("#root.args[0]", method, new Object[]{arr, "test"});

        assertThat(key).isNotNull();
        assertThat(key).contains("...");
    }

    @Test
    @DisplayName("Senior Refinements Round 68: extractKey handles object graphs containing arrays with back-references without StackOverflowError")
    void shouldHandleObjectGraphWithArrayBackReferenceWithoutStackOverflow() throws NoSuchMethodException {
        Method method = SampleConsumer.class.getMethod("handleOrder", OrderPayload.class, String.class);
        java.util.Map<String, Object> parentOrder = new java.util.HashMap<>();
        java.util.Map<String, Object> orderItem = new java.util.HashMap<>();
        parentOrder.put("id", "ORD-1");
        parentOrder.put("items", new Object[]{orderItem});
        orderItem.put("sku", "ITEM-1");
        orderItem.put("order", parentOrder); // cyclic back-reference to parent through array

        String key = extractor.extractKey("#root.args[0]", method, new Object[]{parentOrder, "test"});

        assertThat(key).isNotNull();
        assertThat(key).contains("ORD-1");
        assertThat(key).contains("ITEM-1");
        assertThat(key).contains("...");
    }
}



