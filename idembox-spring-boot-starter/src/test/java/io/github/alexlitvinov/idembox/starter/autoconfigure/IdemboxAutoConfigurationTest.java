package io.github.alexlitvinov.idembox.starter.autoconfigure;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.alexlitvinov.idembox.core.publisher.OutboxPublisher;
import io.github.alexlitvinov.idembox.core.repository.IdempotentRepository;
import io.github.alexlitvinov.idembox.core.repository.OutboxRepository;
import io.github.alexlitvinov.idembox.starter.aop.IdempotentConsumerAspect;
import io.github.alexlitvinov.idembox.starter.cleaner.IdempotencyCleaner;
import io.github.alexlitvinov.idembox.starter.cleaner.OutboxCleaner;
import io.github.alexlitvinov.idembox.starter.cleaner.IdemboxCleanerTaskRegistrar;
import io.github.alexlitvinov.idembox.starter.config.IdemboxProperties;
import java.util.List;
import io.github.alexlitvinov.idembox.starter.consumer.IdemboxDeadLetterPublishingRecoverer;
import io.github.alexlitvinov.idembox.starter.metrics.IdemboxMetrics;
import io.github.alexlitvinov.idembox.starter.poller.OutboxPoller;
import io.github.alexlitvinov.idembox.starter.publisher.DefaultOutboxPublisher;
import io.github.alexlitvinov.idembox.starter.repository.PostgresIdempotentRepository;
import io.github.alexlitvinov.idembox.starter.repository.PostgresOutboxRepository;
import java.time.Duration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.boot.autoconfigure.task.TaskSchedulingAutoConfiguration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.ScheduledAnnotationBeanPostProcessor;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class IdemboxAutoConfigurationTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(IdemboxAutoConfiguration.class));

    private interface StringProducerFactory extends org.springframework.kafka.core.ProducerFactory<String, String> {}

    private static abstract class StringKafkaTemplate extends KafkaTemplate<String, String> {
        protected StringKafkaTemplate() {
            super(mock(StringProducerFactory.class));
        }
    }

    @Configuration
    static class TestInfrastructureConfig {
        @Bean
        public javax.sql.DataSource dataSource() {
            return mock(javax.sql.DataSource.class);
        }

        @Bean
        public JdbcTemplate jdbcTemplate(javax.sql.DataSource dataSource) {
            JdbcTemplate jt = mock(JdbcTemplate.class);
            org.mockito.Mockito.when(jt.getDataSource()).thenReturn(dataSource);
            return jt;
        }

        @Bean
        public ObjectMapper objectMapper() {
            return new ObjectMapper();
        }

        @Bean
        public KafkaTemplate<String, String> kafkaTemplate() {
            return mock(StringKafkaTemplate.class);
        }

        @Bean
        public TransactionTemplate transactionTemplate() {
            return mock(TransactionTemplate.class);
        }
    }

    @Test
    @DisplayName("Should auto-configure all beans when infrastructure is present")
    void shouldConfigureAllBeans() {
        contextRunner.withUserConfiguration(TestInfrastructureConfig.class)
                .run(context -> {
                    assertThat(context).hasSingleBean(OutboxRepository.class);
                    assertThat(context).hasSingleBean(IdempotentRepository.class);
                    assertThat(context).hasSingleBean(OutboxPublisher.class);
                    assertThat(context).hasSingleBean(IdempotentConsumerAspect.class);
                    assertThat(context).hasSingleBean(IdempotencyCleaner.class);
                    assertThat(context).hasSingleBean(OutboxPoller.class);
                    assertThat(context).hasSingleBean(OutboxCleaner.class);

                    assertThat(context.getBean(OutboxRepository.class)).isInstanceOf(PostgresOutboxRepository.class);
                    assertThat(context.getBean(IdempotentRepository.class)).isInstanceOf(PostgresIdempotentRepository.class);
                    assertThat(context.getBean(OutboxPublisher.class)).isInstanceOf(DefaultOutboxPublisher.class);
                });
    }

    @Test
    @DisplayName("Should disable outbox beans when property is false")
    void shouldDisableOutboxWhenConfigured() {
        contextRunner.withUserConfiguration(TestInfrastructureConfig.class)
                .withPropertyValues("idembox.outbox.enabled=false")
                .run(context -> {
                    assertThat(context).doesNotHaveBean(OutboxPublisher.class);
                    assertThat(context).doesNotHaveBean(OutboxPoller.class);
                    assertThat(context).doesNotHaveBean(OutboxCleaner.class);
                    assertThat(context).hasSingleBean(IdempotentConsumerAspect.class);
                });
    }

    @Test
    @DisplayName("Should disable idempotency aspect when property is false")
    void shouldDisableIdempotencyWhenConfigured() {
        contextRunner.withUserConfiguration(TestInfrastructureConfig.class)
                .withPropertyValues("idembox.idempotency.enabled=false")
                .run(context -> {
                    assertThat(context).doesNotHaveBean(IdempotentConsumerAspect.class);
                    assertThat(context).hasSingleBean(OutboxPublisher.class);
                });
    }

    @Configuration
    static class MeterRegistryConfig extends TestInfrastructureConfig {
        @Bean
        public io.micrometer.core.instrument.MeterRegistry meterRegistry() {
            return new io.micrometer.core.instrument.simple.SimpleMeterRegistry();
        }
    }

    @Test
    @DisplayName("Fix 8: Should register idembox.outbox.messages.pending gauge when MeterRegistry is available")
    void shouldRegisterPendingMessagesGaugeWhenMeterRegistryPresent() {
        contextRunner.withUserConfiguration(MeterRegistryConfig.class)
                .run(context -> {
                    assertThat(context).hasSingleBean(io.micrometer.core.instrument.MeterRegistry.class);
                    io.micrometer.core.instrument.MeterRegistry registry = context.getBean(io.micrometer.core.instrument.MeterRegistry.class);
                    assertThat(registry.find("idembox.outbox.messages.pending").gauge()).isNotNull();
                });
    }

    @Test
    @DisplayName("Fix 3: Should disable only OutboxPoller on Web API nodes when poller.enabled=false while retaining OutboxPublisher and OutboxCleaner")
    void shouldDisableOnlyOutboxPollerWhenPollerEnabledIsFalse() {
        contextRunner.withUserConfiguration(TestInfrastructureConfig.class)
                .withPropertyValues("idembox.outbox.poller.enabled=false")
                .run(context -> {
                    assertThat(context).hasSingleBean(OutboxPublisher.class);
                    assertThat(context).hasSingleBean(OutboxRepository.class);
                    assertThat(context).doesNotHaveBean(OutboxPoller.class);
                    assertThat(context).hasSingleBean(OutboxCleaner.class);
                });
    }

    @Test
    @DisplayName("Senior Refinements Round 61: Should disable only OutboxCleaner when cleaner.enabled=false while retaining OutboxPoller")
    void shouldDisableOnlyOutboxCleanerWhenCleanerEnabledIsFalse() {
        contextRunner.withUserConfiguration(TestInfrastructureConfig.class)
                .withPropertyValues("idembox.outbox.cleaner.enabled=false")
                .run(context -> {
                    assertThat(context).hasSingleBean(OutboxPublisher.class);
                    assertThat(context).hasSingleBean(OutboxRepository.class);
                    assertThat(context).hasSingleBean(OutboxPoller.class);
                    assertThat(context).doesNotHaveBean(OutboxCleaner.class);
                });
    }

    @Configuration
    static class TransactionalProducerFactoryConfig extends TestInfrastructureConfig {
        @Bean
        public org.springframework.kafka.core.ProducerFactory<?, ?> producerFactory() {
            java.util.Map<String, Object> configs = new java.util.HashMap<>();
            configs.put(org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            configs.put(org.apache.kafka.clients.producer.ProducerConfig.TRANSACTIONAL_ID_CONFIG, "tx-custom");
            org.springframework.kafka.core.DefaultKafkaProducerFactory<?, ?> factory =
                    new org.springframework.kafka.core.DefaultKafkaProducerFactory<>(
                            configs,
                            org.apache.kafka.common.serialization.StringSerializer::new,
                            org.apache.kafka.common.serialization.StringSerializer::new
                    );
            factory.setTransactionIdPrefix("tx-");
            return factory;
        }
    }

    @Test
    @DisplayName("Transactional Kafka conflict: Should create isolated non-transactional KafkaTemplate for OutboxPoller")
    void shouldCreateNonTransactionalKafkaTemplateWhenProducerFactoryIsTransactional() {
        contextRunner.withUserConfiguration(TransactionalProducerFactoryConfig.class)
                .run(context -> {
                    assertThat(context).hasBean("idemboxOutboxKafkaOperations");
                    org.springframework.kafka.core.KafkaTemplate<?, ?> outboxKafka =
                            context.getBean("idemboxOutboxKafkaOperations", org.springframework.kafka.core.KafkaTemplate.class);
                    // Crucial: The Outbox KafkaTemplate must NOT be transactional!
                    assertThat(outboxKafka.isTransactional()).isFalse();

                    org.springframework.kafka.core.DefaultKafkaProducerFactory<?, ?> factory =
                            (org.springframework.kafka.core.DefaultKafkaProducerFactory<?, ?>) outboxKafka.getProducerFactory();
                    assertThat(factory.getConfigurationProperties())
                            .doesNotContainKey(org.apache.kafka.clients.producer.ProducerConfig.TRANSACTIONAL_ID_CONFIG);
                    assertThat(factory.getConfigurationProperties())
                            .containsEntry(org.apache.kafka.clients.producer.ProducerConfig.MAX_BLOCK_MS_CONFIG, 10000L);
                    assertThat(factory.getKeySerializerSupplier()).isNotNull();
                    assertThat(factory.getValueSerializerSupplier()).isNotNull();
                    assertThat(factory.getValueSerializerSupplier().get())
                            .isInstanceOf(io.github.alexlitvinov.idembox.starter.serializer.OutboxPayloadSerializer.class);

                    // Verify destroy() cleanly shuts down nonTxFactory
                    outboxKafka.destroy();
                });
    }

    @Configuration
    static class CustomJsonSerializerProducerConfig extends TestInfrastructureConfig {
        @Bean
        public org.springframework.kafka.core.ProducerFactory<?, ?> producerFactory() {
            java.util.Map<String, Object> configs = new java.util.HashMap<>();
            configs.put(org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            configs.put(org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, org.springframework.kafka.support.serializer.JsonSerializer.class);
            return new org.springframework.kafka.core.DefaultKafkaProducerFactory<>(
                    configs,
                    org.apache.kafka.common.serialization.StringSerializer::new,
                    org.springframework.kafka.support.serializer.JsonSerializer::new
            );
        }
    }

    @Test
    @DisplayName("Senior Refinements v13: Should force StringSerializer for idemboxOutboxKafkaOperations even if app configures JsonSerializer")
    void shouldForceStringSerializerForOutboxKafkaOperationsWhenAppHasJsonSerializer() {
        contextRunner.withUserConfiguration(CustomJsonSerializerProducerConfig.class)
                .run(context -> {
                    assertThat(context).hasBean("idemboxOutboxKafkaOperations");
                    org.springframework.kafka.core.KafkaTemplate<?, ?> outboxKafka =
                            context.getBean("idemboxOutboxKafkaOperations", org.springframework.kafka.core.KafkaTemplate.class);

                    org.springframework.kafka.core.DefaultKafkaProducerFactory<?, ?> factory =
                            (org.springframework.kafka.core.DefaultKafkaProducerFactory<?, ?>) outboxKafka.getProducerFactory();

                    assertThat(factory.getConfigurationProperties())
                            .containsEntry(org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                                    org.apache.kafka.common.serialization.StringSerializer.class)
                            .containsEntry(org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                                    io.github.alexlitvinov.idembox.starter.serializer.OutboxPayloadSerializer.class);
                    assertThat(factory.getKeySerializerSupplier().get())
                            .isInstanceOf(org.apache.kafka.common.serialization.StringSerializer.class);
                    assertThat(factory.getValueSerializerSupplier().get())
                            .isInstanceOf(io.github.alexlitvinov.idembox.starter.serializer.OutboxPayloadSerializer.class);
                });
    }

    @Test
    @DisplayName("Should configure idemboxTaskScheduler when no custom TaskScheduler bean exists")
    void shouldConfigureIdemboxTaskScheduler() {
        contextRunner.withUserConfiguration(TestInfrastructureConfig.class)
                .run(context -> {
                    assertThat(context).hasBean("idemboxTaskScheduler");
                    assertThat(context).hasSingleBean(org.springframework.scheduling.TaskScheduler.class);
                });
    }

    static class CustomIdemboxTaskSchedulerConfig extends TestInfrastructureConfig {
        @Bean("idemboxTaskScheduler")
        public org.springframework.scheduling.TaskScheduler customScheduler() {
            return mock(org.springframework.scheduling.TaskScheduler.class);
        }
    }

    @Test
    @DisplayName("Should back off default idemboxTaskScheduler when custom idemboxTaskScheduler bean exists")
    void shouldBackOffTaskSchedulerWhenCustomPresent() {
        contextRunner.withUserConfiguration(CustomIdemboxTaskSchedulerConfig.class)
                .run(context -> {
                    assertThat(context).hasBean("idemboxTaskScheduler");
                    assertThat(context.getBean("idemboxTaskScheduler")).isNotInstanceOf(org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler.class);
                });
    }

    @Test
    @DisplayName("High load backpressure: outboxAsyncExecutor should use CallerRunsPolicy")
    void outboxAsyncExecutorShouldUseCallerRunsPolicy() {
        contextRunner.withUserConfiguration(TestInfrastructureConfig.class)
                .run(context -> {
                    assertThat(context).hasBean("outboxAsyncExecutor");
                    org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor executor =
                            context.getBean("outboxAsyncExecutor", org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor.class);
                    assertThat(executor.getThreadPoolExecutor().getRejectedExecutionHandler())
                            .isInstanceOf(java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy.class);
                });
    }

    @Test
    @DisplayName("Senior Refinements v45: outboxAsyncExecutor rejects web server and kafka threads to prevent thread hijacking")
    void outboxAsyncExecutorShouldRejectWebAndKafkaThreads() {
        contextRunner.withUserConfiguration(TestInfrastructureConfig.class)
                .run(context -> {
                    org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor executor =
                            context.getBean("outboxAsyncExecutor", org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor.class);
                    java.util.concurrent.RejectedExecutionHandler handler = executor.getThreadPoolExecutor().getRejectedExecutionHandler();

                    // Test rejection on kafka thread
                    java.util.concurrent.atomic.AtomicReference<Throwable> kafkaEx = new java.util.concurrent.atomic.AtomicReference<>();
                    Thread kafkaThread = new Thread(() -> {
                        try {
                            handler.rejectedExecution(() -> {}, executor.getThreadPoolExecutor());
                        } catch (Throwable t) {
                            kafkaEx.set(t);
                        }
                    }, "kafka-producer-network-thread");
                    kafkaThread.start();
                    kafkaThread.join();
                    assertThat(kafkaEx.get()).isInstanceOf(java.util.concurrent.RejectedExecutionException.class)
                            .hasMessageContaining("Kafka network thread");

                    // Test rejection on tomcat/http web thread
                    java.util.concurrent.atomic.AtomicReference<Throwable> webEx = new java.util.concurrent.atomic.AtomicReference<>();
                    Thread webThread = new Thread(() -> {
                        try {
                            handler.rejectedExecution(() -> {}, executor.getThreadPoolExecutor());
                        } catch (Throwable t) {
                            webEx.set(t);
                        }
                    }, "http-nio-8080-exec-1");
                    webThread.start();
                    webThread.join();
                    assertThat(webEx.get()).isInstanceOf(java.util.concurrent.RejectedExecutionException.class)
                            .hasMessageContaining("Rejecting task execution on web/request thread");

                    // Test non-rejection on Spring @KafkaListener thread (standard worker thread)
                    java.util.concurrent.atomic.AtomicBoolean listenerExecuted = new java.util.concurrent.atomic.AtomicBoolean(false);
                    Thread listenerThread = new Thread(() ->
                            handler.rejectedExecution(() -> listenerExecuted.set(true), executor.getThreadPoolExecutor()),
                            "org.springframework.kafka.KafkaListenerEndpointContainer#0-0-C-1");
                    listenerThread.start();
                    listenerThread.join();
                    assertThat(listenerExecuted.get()).isTrue();

                    // Test non-rejection on application thread named 'web-order-processor'
                    java.util.concurrent.atomic.AtomicBoolean appExecuted = new java.util.concurrent.atomic.AtomicBoolean(false);
                    Thread appThread = new Thread(() ->
                            handler.rejectedExecution(() -> appExecuted.set(true), executor.getThreadPoolExecutor()),
                            "web-order-processor");
                    appThread.start();
                    appThread.join();
                    assertThat(appExecuted.get()).isTrue();
                });
    }

    @Configuration
    static class PlatformTxManagerConfig extends TestInfrastructureConfig {
        @Bean
        public org.springframework.transaction.PlatformTransactionManager platformTransactionManager() {
            return mock(org.springframework.transaction.PlatformTransactionManager.class);
        }
    }

    @Test
    @DisplayName("Senior Refinements v8: Should configure PostgresOutboxRepository with transactionTemplate when PlatformTransactionManager is present")
    void shouldConfigureClaimTransactionTemplateWhenTxManagerPresent() {
        contextRunner.withUserConfiguration(PlatformTxManagerConfig.class)
                .run(context -> {
                    assertThat(context).hasSingleBean(OutboxRepository.class);
                    PostgresOutboxRepository repo = (PostgresOutboxRepository) context.getBean(OutboxRepository.class);
                    assertThat(repo.getTransactionTemplate()).isNotNull();
                    assertThat(repo.getTransactionTemplate().getPropagationBehavior())
                            .isEqualTo(org.springframework.transaction.TransactionDefinition.PROPAGATION_REQUIRES_NEW);
                    assertThat(repo.getTransactionTemplate().getIsolationLevel())
                            .isEqualTo(org.springframework.transaction.TransactionDefinition.ISOLATION_READ_COMMITTED);
                    assertThat(repo.getTransactionTemplate().getTimeout()).isEqualTo(5);
                });
    }

    @Test
    @DisplayName("Senior Refinements v19: Should allow max.block.ms to match sendTimeout when sendTimeout exceeds 10s")
    void shouldAllowMaxBlockMsToMatchSendTimeoutWhenSendTimeoutIsLarge() {
        contextRunner.withUserConfiguration(CustomJsonSerializerProducerConfig.class)
                .withPropertyValues("idembox.outbox.send-timeout=30s")
                .run(context -> {
                    assertThat(context).hasBean("idemboxOutboxKafkaOperations");
                    org.springframework.kafka.core.KafkaTemplate<?, ?> outboxKafka =
                            context.getBean("idemboxOutboxKafkaOperations", org.springframework.kafka.core.KafkaTemplate.class);
                    org.springframework.kafka.core.DefaultKafkaProducerFactory<?, ?> factory =
                            (org.springframework.kafka.core.DefaultKafkaProducerFactory<?, ?>) outboxKafka.getProducerFactory();

                    assertThat(factory.getConfigurationProperties())
                            .containsEntry(org.apache.kafka.clients.producer.ProducerConfig.MAX_BLOCK_MS_CONFIG, 30_000L);
                });
    }

    @Test
    @DisplayName("Senior Refinements v19: Should use configured sendTimeout for max.block.ms when under 10s")
    void shouldUseConfiguredSendTimeoutWhenUnder10Seconds() {
        contextRunner.withUserConfiguration(CustomJsonSerializerProducerConfig.class)
                .withPropertyValues("idembox.outbox.send-timeout=4s")
                .run(context -> {
                    assertThat(context).hasBean("idemboxOutboxKafkaOperations");
                    org.springframework.kafka.core.KafkaTemplate<?, ?> outboxKafka =
                            context.getBean("idemboxOutboxKafkaOperations", org.springframework.kafka.core.KafkaTemplate.class);
                    org.springframework.kafka.core.DefaultKafkaProducerFactory<?, ?> factory =
                            (org.springframework.kafka.core.DefaultKafkaProducerFactory<?, ?>) outboxKafka.getProducerFactory();

                    assertThat(factory.getConfigurationProperties())
                            .containsEntry(org.apache.kafka.clients.producer.ProducerConfig.MAX_BLOCK_MS_CONFIG, 4000L);
                });
    }

    @Test
    @DisplayName("Senior Refinements v19: Should register ScheduledAnnotationBeanPostProcessor via @EnableScheduling on auto-configuration when explicitly enabled")
    void shouldRegisterScheduledAnnotationBeanPostProcessor() {
        contextRunner.withUserConfiguration(TestInfrastructureConfig.class)
                .withPropertyValues("idembox.scheduling.enabled=true")
                .run(context -> assertThat(context).hasBean(
                        org.springframework.scheduling.config.TaskManagementConfigUtils.SCHEDULED_ANNOTATION_PROCESSOR_BEAN_NAME));
    }

    @Test
    @DisplayName("Senior Refinements v23/v42: idemboxOutboxKafkaOperations enforces StringSerializer for key and OutboxPayloadSerializer for value")
    void shouldEnforceStringSerializerOnOutboxKafkaOperations() {
        contextRunner.withUserConfiguration(CustomJsonSerializerProducerConfig.class)
                .run(context -> {
                    assertThat(context).hasBean("idemboxOutboxKafkaOperations");
                    org.springframework.kafka.core.KafkaTemplate<?, ?> outboxKafka =
                            context.getBean("idemboxOutboxKafkaOperations", org.springframework.kafka.core.KafkaTemplate.class);
                    org.springframework.kafka.core.DefaultKafkaProducerFactory<?, ?> factory =
                            (org.springframework.kafka.core.DefaultKafkaProducerFactory<?, ?>) outboxKafka.getProducerFactory();

                    assertThat(factory.getConfigurationProperties())
                            .containsEntry(org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                                    org.apache.kafka.common.serialization.StringSerializer.class)
                            .containsEntry(org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                                    io.github.alexlitvinov.idembox.starter.serializer.OutboxPayloadSerializer.class);
                });
    }

    @Configuration
    static class MultipleTransactionManagersConfig extends TestInfrastructureConfig {
        @Bean("txManager1")
        public PlatformTransactionManager txManager1() {
            return mock(PlatformTransactionManager.class);
        }

        @Bean("txManager2")
        public PlatformTransactionManager txManager2() {
            return mock(PlatformTransactionManager.class);
        }
    }

    @Test
    @DisplayName("Senior Refinements v27: Multiple PlatformTransactionManager beans without @Primary should resolve to DataSourceTransactionManager without NoUniqueBeanDefinitionException")
    void shouldResolveTransactionManagerWhenMultipleManagersPresentWithoutPrimary() {
        contextRunner.withUserConfiguration(MultipleTransactionManagersConfig.class)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).hasSingleBean(IdempotentRepository.class);
                    assertThat(context).hasSingleBean(OutboxRepository.class);
                });
    }

    @Test
    @DisplayName("Senior Refinements v30: Supports hierarchical poller rate property idembox.outbox.poller.rate-ms")
    void shouldBindHierarchicalPollerRateProperty() {
        contextRunner.withUserConfiguration(TestInfrastructureConfig.class)
                .withPropertyValues("idembox.outbox.poller.rate-ms=350")
                .run(context -> {
                    IdemboxProperties props = context.getBean(IdemboxProperties.class);
                    assertThat(props.getOutbox().getPoller().getRateMs()).isEqualTo(350L);
                    assertThat(props.getOutbox().getPollerRateMs()).isEqualTo(350L);
                });
    }

    @Test
    @DisplayName("Senior Refinements v30: Supports legacy poller rate property idembox.outbox.poller-rate-ms")
    void shouldBindLegacyPollerRateProperty() {
        contextRunner.withUserConfiguration(TestInfrastructureConfig.class)
                .withPropertyValues("idembox.outbox.poller-rate-ms=420")
                .run(context -> {
                    IdemboxProperties props = context.getBean(IdemboxProperties.class);
                    assertThat(props.getOutbox().getPoller().getRateMs()).isEqualTo(420L);
                    assertThat(props.getOutbox().getPollerRateMs()).isEqualTo(420L);
                });
    }

    @Configuration
    static class GlobalTaskSchedulerConfig extends TestInfrastructureConfig {
        @Bean("taskScheduler")
        public org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler taskScheduler() {
            org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler s = new org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler();
            s.setPoolSize(1);
            s.setThreadNamePrefix("scheduling-");
            s.initialize();
            return s;
        }
    }

    @Test
    @DisplayName("Senior Refinements v37: IdemboxTaskScheduler is created and isolated even when standard taskScheduler (poolSize=1) is present")
    void shouldIsolateIdemboxTaskSchedulerEvenWhenGlobalTaskSchedulerExists() {
        contextRunner.withUserConfiguration(GlobalTaskSchedulerConfig.class)
                .run(context -> {
                    assertThat(context).hasBean("taskScheduler");
                    assertThat(context).hasBean("idemboxTaskScheduler");
                    org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler idemboxScheduler =
                            context.getBean("idemboxTaskScheduler", org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler.class);
                    assertThat(idemboxScheduler.getThreadNamePrefix()).isEqualTo("idembox-scheduler-");
                    assertThat(idemboxScheduler.getScheduledThreadPoolExecutor().getCorePoolSize()).isEqualTo(4);
                });
    }

    @Configuration
    static class DatabaseInitializerTestConfig extends TestInfrastructureConfig {
        @Bean
        @Override
        public javax.sql.DataSource dataSource() {
            try {
                javax.sql.DataSource ds = mock(javax.sql.DataSource.class);
                java.sql.Connection conn = mock(java.sql.Connection.class);
                java.sql.DatabaseMetaData meta = mock(java.sql.DatabaseMetaData.class);
                java.sql.Statement stmt = mock(java.sql.Statement.class);
                when(ds.getConnection()).thenReturn(conn);
                when(conn.getMetaData()).thenReturn(meta);
                when(conn.createStatement()).thenReturn(stmt);
                return ds;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Test
    @DisplayName("Senior Refinements v37: Configures idemboxDatabaseInitializer when idembox.initialize-schema=true")
    void shouldConfigureDatabaseInitializerWhenPropertyEnabled() {
        contextRunner.withUserConfiguration(DatabaseInitializerTestConfig.class)
                .withPropertyValues("idembox.initialize-schema=true")
                .run(context -> assertThat(context).hasBean("idemboxDatabaseInitializer"));

        contextRunner.withUserConfiguration(TestInfrastructureConfig.class)
                .run(context -> assertThat(context).doesNotHaveBean("idemboxDatabaseInitializer"));
    }

    @Test
    @DisplayName("Senior Refinements v38: Should use NoOp IdemboxMetrics when idembox.metrics.enabled=false")
    void shouldUseNoOpMetricsWhenMetricsDisabled() {
        contextRunner.withUserConfiguration(TestInfrastructureConfig.class)
                .withPropertyValues("idembox.metrics.enabled=false")
                .run(context -> {
                    assertThat(context).hasSingleBean(IdemboxMetrics.class);
                    IdemboxMetrics metrics = context.getBean(IdemboxMetrics.class);
                    // Safe no-op invocation
                    metrics.recordOutboxEnqueued("any-topic");
                    metrics.recordIdempotentDuplicate("any-group");
                });
    }

    @Test
    @DisplayName("Senior Refinements v38: Should configure multi-tenancy table schema and table prefix on repositories")
    void shouldConfigureMultiTenancyTableNamesOnRepositories() {
        contextRunner.withUserConfiguration(TestInfrastructureConfig.class)
                .withPropertyValues(
                        "idembox.table-schema=custom_schema",
                        "idembox.table-prefix=tenant1_"
                )
                .run(context -> {
                    assertThat(context).hasBean("outboxRepository");
                    assertThat(context).hasBean("idempotentRepository");

                    io.github.alexlitvinov.idembox.starter.repository.PostgresOutboxRepository outboxRepo =
                            (io.github.alexlitvinov.idembox.starter.repository.PostgresOutboxRepository) context.getBean("outboxRepository");
                    assertThat(outboxRepo.getTableName()).isEqualTo("custom_schema.tenant1_outbox_messages");

                    io.github.alexlitvinov.idembox.starter.repository.PostgresIdempotentRepository idempotentRepo =
                            (io.github.alexlitvinov.idembox.starter.repository.PostgresIdempotentRepository) context.getBean("idempotentRepository");
                    assertThat(idempotentRepo.getTableName()).isEqualTo("custom_schema.tenant1_idempotent_records");
                });
    }

    static class MultipleTxManagersConfig extends TestInfrastructureConfig {
        @Bean
        @org.springframework.context.annotation.Primary
        public org.springframework.transaction.PlatformTransactionManager primaryTransactionManager() {
            return mock(org.springframework.transaction.PlatformTransactionManager.class);
        }

        @Bean
        public org.springframework.transaction.PlatformTransactionManager secondaryBatchTransactionManager() {
            return mock(org.springframework.transaction.PlatformTransactionManager.class);
        }
    }

    @Test
    @DisplayName("Senior Refinements v47: resolveTransactionManager prioritizes @Primary transaction manager when multiple exist")
    void shouldPrioritizePrimaryTransactionManagerWhenMultipleExist() {
        contextRunner.withUserConfiguration(MultipleTxManagersConfig.class)
                .run(context -> {
                    assertThat(context).hasBean("idempotentRepository");
                    PostgresIdempotentRepository repo = (PostgresIdempotentRepository) context.getBean("idempotentRepository");
                    org.springframework.transaction.PlatformTransactionManager primaryTm =
                            context.getBean("primaryTransactionManager", org.springframework.transaction.PlatformTransactionManager.class);
                    assertThat(repo.getRequiresNewTransactionTemplate().getTransactionManager()).isSameAs(primaryTm);
                });
    }

    static class IncompatibleDataSourceTxManagerConfig extends TestInfrastructureConfig {
        @Bean
        public org.springframework.jdbc.datasource.DataSourceTransactionManager foreignAuditTxManager() {
            javax.sql.DataSource foreignDs = mock(javax.sql.DataSource.class);
            return new org.springframework.jdbc.datasource.DataSourceTransactionManager(foreignDs);
        }
    }

    @Test
    @DisplayName("Senior Refinements Round 52: resolveTransactionManager rejects txManager from mismatched DataSource and creates dedicated txManager")
    void shouldCreateDedicatedTxManagerWhenExistingBelongsToDifferentDataSource() {
        contextRunner.withUserConfiguration(IncompatibleDataSourceTxManagerConfig.class)
                .run(context -> {
                    assertThat(context).hasBean("idempotentRepository");
                    PostgresIdempotentRepository repo = (PostgresIdempotentRepository) context.getBean("idempotentRepository");
                    org.springframework.transaction.PlatformTransactionManager tm = repo.getRequiresNewTransactionTemplate().getTransactionManager();
                    assertThat(tm).isInstanceOf(org.springframework.jdbc.datasource.DataSourceTransactionManager.class);
                    org.springframework.jdbc.datasource.DataSourceTransactionManager dataSourceTxManager =
                            (org.springframework.jdbc.datasource.DataSourceTransactionManager) tm;
                    javax.sql.DataSource appDs = context.getBean(javax.sql.DataSource.class);
                    assertThat(java.util.Objects.requireNonNull(dataSourceTxManager).getDataSource()).isSameAs(appDs);
                });
    }

    @Test
    @DisplayName("Round 56: idemboxTaskScheduler should enable graceful shutdown with 30s timeout and alias idemboxTaskExecutor")
    void idemboxTaskSchedulerShouldConfigureGracefulShutdown() {
        contextRunner.withUserConfiguration(TestInfrastructureConfig.class)
                .run(context -> {
                    assertThat(context).hasBean("idemboxTaskScheduler");
                    org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler scheduler =
                            context.getBean("idemboxTaskScheduler", org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler.class);
                    assertThat((Boolean) org.springframework.test.util.ReflectionTestUtils.getField(scheduler, "waitForTasksToCompleteOnShutdown")).isTrue();
                    assertThat((Long) org.springframework.test.util.ReflectionTestUtils.getField(scheduler, "awaitTerminationMillis")).isEqualTo(30000L);

                    assertThat(context).hasBean("idemboxTaskExecutor");
                    assertThat(context.getBean("idemboxTaskExecutor"))
                            .isSameAs(context.getBean("outboxAsyncExecutor"));
                });
    }

    @Configuration
    @EnableScheduling
    static class UserAppSchedulingConfig extends TestInfrastructureConfig {
    }

    @Test
    @DisplayName("TaskScheduling coexistence: TaskSchedulingAutoConfiguration creates standard taskScheduler before IdemboxAutoConfiguration")
    void shouldAllowTaskSchedulingAutoConfigurationToCreateDefaultTaskScheduler() {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(
                        TaskSchedulingAutoConfiguration.class,
                        IdemboxAutoConfiguration.class
                ))
                .withUserConfiguration(UserAppSchedulingConfig.class)
                .run(context -> {
                    assertThat(context).hasBean("taskScheduler");
                    assertThat(context).hasBean("idemboxTaskScheduler");
                    assertThat(context.getBean("taskScheduler"))
                            .isNotSameAs(context.getBean("idemboxTaskScheduler"));
                });
    }


    @Test
    @DisplayName("Scheduling condition: enable-scheduling=false disables @EnableScheduling and ScheduledAnnotationBeanPostProcessor")
    void shouldDisableSchedulingWhenConfigured() {
        contextRunner.withUserConfiguration(TestInfrastructureConfig.class)
                .withPropertyValues("idembox.enable-scheduling=false")
                .run(context -> assertThat(context).doesNotHaveBean(ScheduledAnnotationBeanPostProcessor.class));
    }

    @Test
    @DisplayName("Scheduling condition: scheduling.enabled=false disables @EnableScheduling and ScheduledAnnotationBeanPostProcessor")
    void shouldDisableSchedulingWhenSchedulingEnabledFalse() {
        contextRunner.withUserConfiguration(TestInfrastructureConfig.class)
                .withPropertyValues("idembox.scheduling.enabled=false")
                .run(context -> assertThat(context).doesNotHaveBean(ScheduledAnnotationBeanPostProcessor.class));
    }

    @Test
    @DisplayName("Scheduling condition: by default @EnableScheduling is disabled to prevent leaking into host app")
    void shouldNotEnableSchedulingByDefault() {
        contextRunner.withUserConfiguration(TestInfrastructureConfig.class)
                .run(context -> {
                    assertThat(context).doesNotHaveBean(ScheduledAnnotationBeanPostProcessor.class);
                    assertThat(context).hasSingleBean(io.github.alexlitvinov.idembox.starter.cleaner.IdemboxCleanerTaskRegistrar.class);
                });
    }

    @Test
    @DisplayName("Scheduling condition: when scheduling.enabled=true @EnableScheduling registers ScheduledAnnotationBeanPostProcessor")
    void shouldEnableSchedulingWhenPropertyIsTrue() {
        contextRunner.withUserConfiguration(TestInfrastructureConfig.class)
                .withPropertyValues("idembox.scheduling.enabled=true")
                .run(context -> assertThat(context).hasSingleBean(ScheduledAnnotationBeanPostProcessor.class));
    }

    @Test
    @DisplayName("Issue 4: IdemboxProperties binds scheduling.enabled and keeps enable-scheduling in sync")
    void shouldBindSchedulingProperties() {
        contextRunner.withUserConfiguration(TestInfrastructureConfig.class)
                .withPropertyValues("idembox.scheduling.enabled=true")
                .run(context -> {
                    IdemboxProperties props = context.getBean(IdemboxProperties.class);
                    assertThat(props.getScheduling().isEnabled()).isTrue();
                    assertThat(props.isEnableScheduling()).isTrue();
                });

        contextRunner.withUserConfiguration(TestInfrastructureConfig.class)
                .withPropertyValues("idembox.enable-scheduling=true")
                .run(context -> {
                    IdemboxProperties props = context.getBean(IdemboxProperties.class);
                    assertThat(props.getScheduling().isEnabled()).isTrue();
                    assertThat(props.isEnableScheduling()).isTrue();
                });

        contextRunner.withUserConfiguration(TestInfrastructureConfig.class)
                .run(context -> {
                    IdemboxProperties props = context.getBean(IdemboxProperties.class);
                    assertThat(props.getScheduling().isEnabled()).isFalse();
                    assertThat(props.isEnableScheduling()).isFalse();
                });
    }

    @Test
    @DisplayName("Senior Refinements Round 64: idemboxDatabaseInitializer should be instantiated with templating support when initialize-schema=true")
    void shouldConfigureIdemboxDatabaseInitializer() {
        contextRunner.withUserConfiguration(DatabaseInitializerTestConfig.class)
                .withPropertyValues(
                        "idembox.initialize-schema=true",
                        "idembox.table-schema=custom_schema",
                        "idembox.table-prefix=prefix_"
                )
                .run(context -> {
                    assertThat(context).hasBean("idemboxDatabaseInitializer");
                    assertThat(context.getBean("idemboxDatabaseInitializer"))
                            .isInstanceOf(IdemboxDatabaseInitializer.class);
                });
    }

    @Test
    @DisplayName("Issue 1: idemboxDatabaseInitializer splits comma-separated schema-location for DatabaseInitializationSettings")
    void shouldSplitCommaSeparatedSchemaLocationInSettings() {
        contextRunner.withUserConfiguration(DatabaseInitializerTestConfig.class)
                .withPropertyValues(
                        "idembox.initialize-schema=true",
                        "idembox.schema-location=classpath:db/migration/idembox/schema-postgresql.sql , classpath:db/migration/idembox/schema-postgresql.sql"
                )
                .run(context -> {
                    assertThat(context).hasBean("idemboxDatabaseInitializer");
                    IdemboxDatabaseInitializer initializer =
                            context.getBean("idemboxDatabaseInitializer", IdemboxDatabaseInitializer.class);
                    org.springframework.boot.sql.init.DatabaseInitializationSettings settings =
                            (org.springframework.boot.sql.init.DatabaseInitializationSettings)
                                    org.springframework.test.util.ReflectionTestUtils.getField(initializer, org.springframework.boot.jdbc.init.DataSourceScriptDatabaseInitializer.class, "settings");
                    assertThat(settings).isNotNull();
                    assertThat(settings.getSchemaLocations())
                            .containsExactly("classpath:db/migration/idembox/schema-postgresql.sql", "classpath:db/migration/idembox/schema-postgresql.sql");
                });
    }

    @Test
    @DisplayName("Issue 4: IdemboxCleanerTaskRegistrar programmatically schedules OutboxPoller when @EnableScheduling is absent")
    void shouldProgrammaticallyScheduleOutboxPoller() {
        contextRunner.withUserConfiguration(TestInfrastructureConfig.class)
                .withPropertyValues("idembox.outbox.poller.rate-ms=300")
                .run(context -> {
                    assertThat(context).doesNotHaveBean(ScheduledAnnotationBeanPostProcessor.class);
                    assertThat(context).hasSingleBean(IdemboxCleanerTaskRegistrar.class);
                    IdemboxCleanerTaskRegistrar registrar = context.getBean(IdemboxCleanerTaskRegistrar.class);
                    List<?> tasks = (List<?>) org.springframework.test.util.ReflectionTestUtils.getField(registrar, "scheduledTasks");
                    assertThat(tasks).isNotNull().isNotEmpty();
                });
    }

    @Test
    @DisplayName("Senior Refinements Round 65: idemboxOutboxKafkaOperations configures DELIVERY_TIMEOUT_MS_CONFIG aligning with sendTimeout/kafkaDeliveryTimeout")
    void shouldConfigureDeliveryTimeoutMsOnOutboxKafkaOperations() {
        contextRunner.withUserConfiguration(CustomJsonSerializerProducerConfig.class)
                .withPropertyValues("idembox.outbox.send-timeout=15s")
                .run(context -> {
                    assertThat(context).hasBean("idemboxOutboxKafkaOperations");
                    org.springframework.kafka.core.KafkaTemplate<?, ?> outboxKafka =
                            context.getBean("idemboxOutboxKafkaOperations", org.springframework.kafka.core.KafkaTemplate.class);
                    org.springframework.kafka.core.DefaultKafkaProducerFactory<?, ?> factory =
                            (org.springframework.kafka.core.DefaultKafkaProducerFactory<?, ?>) outboxKafka.getProducerFactory();

                    assertThat(factory.getConfigurationProperties())
                            .containsEntry(org.apache.kafka.clients.producer.ProducerConfig.MAX_BLOCK_MS_CONFIG, 15_000L);
                    assertThat(factory.getConfigurationProperties())
                            .containsEntry(org.apache.kafka.clients.producer.ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120_000);
                });
    }

    @Configuration
    static class CustomIdemboxTaskExecutorConfig extends TestInfrastructureConfig {
        @Bean("idemboxTaskExecutor")
        public org.springframework.core.task.TaskExecutor customExecutor() {
            org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor executor = new org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor();
            executor.setThreadNamePrefix("custom-test-exec-");
            executor.initialize();
            return executor;
        }
    }

    @Test
    @DisplayName("Senior Refinements Round 67: outboxPoller injects custom idemboxTaskExecutor bean")
    void outboxPollerShouldInjectCustomIdemboxTaskExecutor() {
        contextRunner.withUserConfiguration(CustomIdemboxTaskExecutorConfig.class)
                .run(context -> {
                    assertThat(context).hasBean("idemboxTaskExecutor");
                    org.springframework.core.task.TaskExecutor customExec =
                            context.getBean("idemboxTaskExecutor", org.springframework.core.task.TaskExecutor.class);
                    assertThat(context).hasSingleBean(OutboxPoller.class);
                    OutboxPoller poller = context.getBean(OutboxPoller.class);
                    Object pollerExec = org.springframework.test.util.ReflectionTestUtils.getField(poller, "outboxAsyncExecutor");
                    assertThat(pollerExec).isSameAs(customExec);
                });
    }

    @Test
    @DisplayName("Senior Refinements Round 69: idembox.create-schema property binds correctly")
    void shouldBindCreateSchemaProperty() {
        contextRunner.withUserConfiguration(TestInfrastructureConfig.class)
                .withPropertyValues("idembox.create-schema=true")
                .run(context -> {
                    IdemboxProperties props = context.getBean(IdemboxProperties.class);
                    assertThat(props.isCreateSchema()).isTrue();
                });

        contextRunner.withUserConfiguration(TestInfrastructureConfig.class)
                .run(context -> {
                    IdemboxProperties props = context.getBean(IdemboxProperties.class);
                    assertThat(props.isCreateSchema()).isFalse();
                });
    }

    @Test
    @DisplayName("Senior Refinements Round 69: idembox.idempotency.in-flight-wait-timeout clamps to MAX_IN_FLIGHT_WAIT_TIMEOUT")
    void shouldClampInFlightWaitTimeoutFromProperties() {
        contextRunner.withUserConfiguration(TestInfrastructureConfig.class)
                .withPropertyValues("idembox.idempotency.in-flight-wait-timeout=30s")
                .run(context -> {
                    IdemboxProperties props = context.getBean(IdemboxProperties.class);
                    assertThat(props.getIdempotency().getInFlightWaitTimeout())
                            .isEqualTo(IdemboxProperties.IdempotencyProperties.MAX_IN_FLIGHT_WAIT_TIMEOUT);
                });
    }

    @Configuration
    static class UnqualifiedKafkaOperationsConsumerConfig extends TransactionalProducerFactoryConfig {
        @Bean
        public String userConsumerDependingOnKafkaOperations(
                org.springframework.kafka.core.KafkaOperations<?, ?> customNamedKafkaOperations) {
            return customNamedKafkaOperations != null ? "injected-ok" : "null";
        }
    }

    @Test
    @DisplayName("Senior Refinements Round 75: idemboxOutboxKafkaOperations has autowireCandidate=false preventing NoUniqueBeanDefinitionException while OutboxPoller still uses it")
    void shouldNotCauseNoUniqueBeanDefinitionExceptionWhenInjectingUnqualifiedKafkaOperations() {
        contextRunner.withUserConfiguration(UnqualifiedKafkaOperationsConsumerConfig.class)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context.getBean("userConsumerDependingOnKafkaOperations")).isEqualTo("injected-ok");

                    org.springframework.kafka.core.KafkaOperations<?, ?> outboxOps =
                            context.getBean("idemboxOutboxKafkaOperations", org.springframework.kafka.core.KafkaOperations.class);
                    org.springframework.kafka.core.KafkaOperations<?, ?> defaultOps =
                            context.getBean("kafkaTemplate", org.springframework.kafka.core.KafkaOperations.class);
                    assertThat(outboxOps).isNotSameAs(defaultOps);

                    OutboxPoller poller = context.getBean(OutboxPoller.class);
                    Object pollerOps = org.springframework.test.util.ReflectionTestUtils.getField(poller, "kafkaOperations");
                    assertThat(pollerOps).isSameAs(outboxOps);
                });
    }

    @Configuration
    static class StringProducerPropertiesAndMultipleKafkaTemplatesConfig {
        @Bean
        public JdbcTemplate jdbcTemplate() {
            return mock(JdbcTemplate.class);
        }

        @Bean
        public ObjectMapper objectMapper() {
            return new ObjectMapper();
        }

        @Bean
        public PlatformTransactionManager transactionManager() {
            return mock(PlatformTransactionManager.class);
        }

        @Bean
        public org.springframework.kafka.core.ProducerFactory<?, ?> producerFactory() {
            java.util.Map<String, Object> configs = new java.util.HashMap<>();
            configs.put(org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            // String properties as populated by Spring Boot spring.kafka.producer.properties.*
            configs.put(org.apache.kafka.clients.producer.ProducerConfig.LINGER_MS_CONFIG, "50");
            configs.put(org.apache.kafka.clients.producer.ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "15000");
            return new org.springframework.kafka.core.DefaultKafkaProducerFactory<>(
                    configs,
                    org.apache.kafka.common.serialization.StringSerializer::new,
                    org.apache.kafka.common.serialization.StringSerializer::new
            );
        }

        @Bean(name = "kafkaTemplate")
        public org.springframework.kafka.core.KafkaOperations<String, String> kafkaTemplate() {
            return mock(StringKafkaTemplate.class);
        }

        @Bean(name = "secondaryKafkaTemplate")
        public org.springframework.kafka.core.KafkaOperations<String, String> secondaryKafkaTemplate() {
            return mock(StringKafkaTemplate.class);
        }
    }

    @Test
    @DisplayName("Senior Refinements Round 76: Handles String linger.ms/request.timeout.ms configs and multiple KafkaOperations beans without ClassCastException or NoUniqueBeanDefinitionException")
    void shouldHandleStringProducerConfigsAndMultipleKafkaOperationsBeans() {
        contextRunner.withUserConfiguration(StringProducerPropertiesAndMultipleKafkaTemplatesConfig.class)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).hasSingleBean(OutboxPoller.class);
                    org.springframework.kafka.core.KafkaTemplate<?, ?> outboxKafka =
                            context.getBean("idemboxOutboxKafkaOperations", org.springframework.kafka.core.KafkaTemplate.class);
                    org.springframework.kafka.core.DefaultKafkaProducerFactory<?, ?> factory =
                            (org.springframework.kafka.core.DefaultKafkaProducerFactory<?, ?>) outboxKafka.getProducerFactory();
                    // Default kafkaDeliveryTimeout is 2m (120000ms) >= 50 + 15000 (15050ms)
                    assertThat(factory.getConfigurationProperties())
                            .containsEntry(org.apache.kafka.clients.producer.ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120_000);
                });
    }

    @Configuration
    static class ProducerFactoryOnlyWithoutDefaultKafkaTemplateConfig {
        @Bean
        public JdbcTemplate jdbcTemplate() {
            return mock(JdbcTemplate.class);
        }

        @Bean
        public ObjectMapper objectMapper() {
            return new ObjectMapper();
        }

        @Bean
        public PlatformTransactionManager transactionManager() {
            return mock(PlatformTransactionManager.class);
        }

        @Bean
        public org.springframework.kafka.core.ProducerFactory<?, ?> producerFactory() {
            java.util.Map<String, Object> configs = new java.util.HashMap<>();
            configs.put(org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            return new org.springframework.kafka.core.DefaultKafkaProducerFactory<>(
                    configs,
                    org.apache.kafka.common.serialization.StringSerializer::new,
                    org.apache.kafka.common.serialization.StringSerializer::new
            );
        }
    }

    @Test
    @DisplayName("Senior Refinements Round 76: Creates OutboxPoller even when only ProducerFactory is present and idemboxOutboxKafkaOperations has autowireCandidate=false")
    void shouldCreateOutboxPollerWhenOnlyProducerFactoryIsPresent() {
        contextRunner.withUserConfiguration(ProducerFactoryOnlyWithoutDefaultKafkaTemplateConfig.class)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).hasBean("idemboxOutboxKafkaOperations");
                    assertThat(context).hasSingleBean(OutboxPoller.class);
                });
    }

    @Configuration
    static class DedicatedDataSourceConfig {
        @Bean
        public javax.sql.DataSource dataSource() {
            return mock(javax.sql.DataSource.class);
        }

        @Bean(name = "idemboxDataSource")
        public javax.sql.DataSource idemboxDataSource() {
            return mock(javax.sql.DataSource.class);
        }

        @Bean
        public JdbcTemplate jdbcTemplate(javax.sql.DataSource dataSource) {
            JdbcTemplate jt = mock(JdbcTemplate.class);
            when(jt.getDataSource()).thenReturn(dataSource);
            return jt;
        }

        @Bean
        public ObjectMapper objectMapper() {
            return new ObjectMapper();
        }

        @Bean
        public KafkaTemplate<String, String> kafkaTemplate() {
            return mock(StringKafkaTemplate.class);
        }
    }

    @Test
    @DisplayName("Should use dedicated idemboxDataSource for repositories when present")
    void shouldUseDedicatedDataSourceWhenPresent() {
        contextRunner.withUserConfiguration(DedicatedDataSourceConfig.class)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).hasSingleBean(IdempotentRepository.class);
                    assertThat(context).hasSingleBean(OutboxRepository.class);

                    javax.sql.DataSource dedicatedDs = context.getBean("idemboxDataSource", javax.sql.DataSource.class);

                    IdempotentRepository idRepo = context.getBean(IdempotentRepository.class);
                    assertThat(idRepo).isInstanceOf(PostgresIdempotentRepository.class);
                    PostgresIdempotentRepository pgIdRepo = (PostgresIdempotentRepository) idRepo;
                    assertThat(pgIdRepo.getJdbcTemplate().getDataSource()).isSameAs(dedicatedDs);

                    OutboxRepository outboxRepo = context.getBean(OutboxRepository.class);
                    assertThat(outboxRepo).isInstanceOf(PostgresOutboxRepository.class);
                    PostgresOutboxRepository pgOutboxRepo = (PostgresOutboxRepository) outboxRepo;
                    assertThat(pgOutboxRepo.getJdbcTemplate().getDataSource()).isSameAs(dedicatedDs);
                });
    }

    @Test
    @DisplayName("Issue 5: IdemboxDeadLetterPublishingRecoverer uses configured idempotency default-ttl for terminalFailureRetention")
    void shouldConfigureDeadLetterPublishingRecovererWithConfiguredRetention() {
        contextRunner.withUserConfiguration(TestInfrastructureConfig.class)
                .withPropertyValues("idembox.idempotency.default-ttl=30d")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).hasSingleBean(IdemboxDeadLetterPublishingRecoverer.class);
                    IdemboxDeadLetterPublishingRecoverer recoverer =
                            context.getBean(IdemboxDeadLetterPublishingRecoverer.class);
                    assertThat(recoverer.getTerminalFailureRetention()).isEqualTo(Duration.ofDays(30));
                });
    }

    @Configuration
    static class InfrastructureWithoutObjectMapperConfig {
        @Bean
        public javax.sql.DataSource dataSource() {
            return mock(javax.sql.DataSource.class);
        }

        @Bean
        public JdbcTemplate jdbcTemplate(javax.sql.DataSource dataSource) {
            JdbcTemplate jt = mock(JdbcTemplate.class);
            when(jt.getDataSource()).thenReturn(dataSource);
            return jt;
        }

        @Bean
        public KafkaTemplate<String, String> kafkaTemplate() {
            return mock(StringKafkaTemplate.class);
        }

        @Bean
        public TransactionTemplate transactionTemplate() {
            return mock(TransactionTemplate.class);
        }
    }

    @Test
    @DisplayName("Should provide fallback ObjectMapper bean when none is defined")
    void shouldProvideFallbackObjectMapperWhenMissing() {
        contextRunner.withUserConfiguration(InfrastructureWithoutObjectMapperConfig.class)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).hasSingleBean(ObjectMapper.class);
                    assertThat(context).hasSingleBean(OutboxRepository.class);
                    assertThat(context).hasSingleBean(OutboxPublisher.class);
                });
    }

    @Test
    @DisplayName("Should respect user-provided ObjectMapper bean when defined")
    void shouldRespectUserProvidedObjectMapper() {
        ObjectMapper customMapper = new ObjectMapper();
        contextRunner.withUserConfiguration(InfrastructureWithoutObjectMapperConfig.class)
                .withBean("customObjectMapper", ObjectMapper.class, () -> customMapper)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).hasSingleBean(ObjectMapper.class);
                    assertThat(context.getBean(ObjectMapper.class)).isSameAs(customMapper);
                });
    }

    @Test
    @DisplayName("Should wire Tracer into OutboxPublisher when Tracer bean is present in context")
    void shouldWireTracerWhenTracerBeanPresent() {
        io.micrometer.tracing.Tracer mockTracer = mock(io.micrometer.tracing.Tracer.class);
        contextRunner.withUserConfiguration(TestInfrastructureConfig.class)
                .withBean("customTracer", io.micrometer.tracing.Tracer.class, () -> mockTracer)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).hasSingleBean(OutboxPublisher.class);
                    OutboxPublisher publisher = context.getBean(OutboxPublisher.class);
                    assertThat(publisher).isInstanceOf(DefaultOutboxPublisher.class);
                    assertThat(org.springframework.test.util.ReflectionTestUtils.getField(publisher, "tracer"))
                            .isSameAs(mockTracer);
                });
    }

    @Test
    @DisplayName("Should create OutboxPublisher with null Tracer when Tracer bean is absent")
    void shouldCreateOutboxPublisherWithNullTracerWhenAbsent() {
        contextRunner.withUserConfiguration(TestInfrastructureConfig.class)
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).hasSingleBean(OutboxPublisher.class);
                    OutboxPublisher publisher = context.getBean(OutboxPublisher.class);
                    assertThat(publisher).isInstanceOf(DefaultOutboxPublisher.class);
                    assertThat(org.springframework.test.util.ReflectionTestUtils.getField(publisher, "tracer"))
                            .isNull();
                });
    }
}
