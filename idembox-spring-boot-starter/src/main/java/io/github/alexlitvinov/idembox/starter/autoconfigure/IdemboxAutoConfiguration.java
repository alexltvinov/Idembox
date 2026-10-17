package io.github.alexlitvinov.idembox.starter.autoconfigure;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.alexlitvinov.idembox.core.publisher.OutboxPublisher;
import io.github.alexlitvinov.idembox.core.repository.IdempotentRepository;
import io.github.alexlitvinov.idembox.core.repository.OutboxRepository;
import io.github.alexlitvinov.idembox.core.spel.SpelKeyExtractor;
import io.github.alexlitvinov.idembox.starter.aop.IdempotentConsumerAspect;
import io.github.alexlitvinov.idembox.starter.aop.IdempotentStartupValidator;
import io.github.alexlitvinov.idembox.starter.aop.IdempotentTransactionSyncAspect;
import io.github.alexlitvinov.idembox.starter.cleaner.IdempotencyCleaner;
import io.github.alexlitvinov.idembox.starter.cleaner.OutboxCleaner;
import io.github.alexlitvinov.idembox.starter.cleaner.IdemboxCleanerTaskRegistrar;
import io.github.alexlitvinov.idembox.starter.config.IdemboxProperties;
import io.github.alexlitvinov.idembox.starter.consumer.IdemboxDeadLetterPublishingRecoverer;
import io.github.alexlitvinov.idembox.starter.metrics.IdemboxMetrics;
import io.github.alexlitvinov.idembox.starter.poller.OutboxPoller;
import io.github.alexlitvinov.idembox.starter.publisher.DefaultOutboxPublisher;
import io.github.alexlitvinov.idembox.starter.repository.PostgresIdempotentRepository;
import io.github.alexlitvinov.idembox.starter.repository.PostgresOutboxRepository;
import io.github.alexlitvinov.idembox.starter.util.ThreadTypeClassifier;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import java.util.List;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.context.annotation.Conditional;
import org.springframework.core.type.AnnotatedTypeMetadata;
import org.springframework.lang.NonNull;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.JdbcTemplateAutoConfiguration;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.ClassUtils;
import org.springframework.core.io.ResourceLoader;
import org.springframework.core.task.TaskExecutor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;

import org.springframework.boot.autoconfigure.jackson.JacksonAutoConfiguration;
import org.springframework.boot.autoconfigure.task.TaskSchedulingAutoConfiguration;

@AutoConfiguration
@AutoConfigureAfter({
        DataSourceAutoConfiguration.class,
        JdbcTemplateAutoConfiguration.class,
        KafkaAutoConfiguration.class,
        TaskSchedulingAutoConfiguration.class,
        org.springframework.boot.autoconfigure.transaction.TransactionAutoConfiguration.class,
        JacksonAutoConfiguration.class
})
@EnableConfigurationProperties(IdemboxProperties.class)
@ConditionalOnClass(JdbcTemplate.class)
public class IdemboxAutoConfiguration {

    @Configuration(proxyBeanMethods = false)
    @org.springframework.context.annotation.Conditional(SchedulingEnabledCondition.class)
    @EnableScheduling
    static class IdemboxSchedulingConfiguration {
    }

    static class SchedulingEnabledCondition implements Condition {
        @Override
        public boolean matches(
                @NonNull ConditionContext context,
                @NonNull AnnotatedTypeMetadata metadata) {
            org.springframework.core.env.Environment env = context.getEnvironment();
            String prop1 = env.getProperty("idembox.scheduling.enabled");
            if (prop1 != null) {
                return "true".equalsIgnoreCase(prop1.trim());
            }
            String prop2 = env.getProperty("idembox.enable-scheduling");
            if (prop2 != null) {
                return "true".equalsIgnoreCase(prop2.trim());
            }
            return false;
        }
    }

    @Bean(name = {"outboxAsyncExecutor", "idemboxTaskExecutor"})
    @ConditionalOnMissingBean(name = {"outboxAsyncExecutor", "idemboxTaskExecutor"})
    public TaskExecutor outboxAsyncExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(2);
        executor.setMaxPoolSize(10);
        executor.setQueueCapacity(500);
        executor.setThreadNamePrefix("outbox-async-");
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy() {
            @Override
            public void rejectedExecution(Runnable r, ThreadPoolExecutor e) {
                Thread current = Thread.currentThread();
                if (ThreadTypeClassifier.isKafkaNetworkThread(current)) {
                    throw new RejectedExecutionException("Rejecting task execution on Kafka network thread to prevent I/O starvation: " + current.getName());
                }
                if (ThreadTypeClassifier.isWebContainerThread(current) || ThreadTypeClassifier.isEventLoopOrNonBlockingThread(current)) {
                    throw new RejectedExecutionException("Rejecting task execution on web/request thread to prevent thread hijacking: " + current.getName());
                }
                super.rejectedExecution(r, e);
            }
        });
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(30);
        executor.initialize();
        return executor;
    }

    @Bean(name = "idemboxTaskScheduler")
    @ConditionalOnMissingBean(name = "idemboxTaskScheduler")
    public org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler idemboxTaskScheduler(
            IdemboxProperties properties) {
        org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler scheduler = new org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler();
        int poolSize = properties.getSchedulerPoolSize() > 0 ? properties.getSchedulerPoolSize() : 4;
        scheduler.setPoolSize(poolSize);
        scheduler.setThreadNamePrefix("idembox-scheduler-");
        scheduler.setWaitForTasksToCompleteOnShutdown(true);
        scheduler.setAwaitTerminationSeconds(30);
        scheduler.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        scheduler.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
        scheduler.initialize();
        return scheduler;
    }

    @Bean
    @ConditionalOnMissingBean(IdemboxCleanerTaskRegistrar.class)
    @ConditionalOnBean(name = "idemboxTaskScheduler")
    public IdemboxCleanerTaskRegistrar idemboxCleanerTaskRegistrar(
            @Qualifier("idemboxTaskScheduler") org.springframework.scheduling.TaskScheduler scheduler,
            ObjectProvider<OutboxCleaner> outboxCleanerProvider,
            ObjectProvider<IdempotencyCleaner> idempotencyCleanerProvider,
            ObjectProvider<OutboxPoller> outboxPollerProvider,
            IdemboxProperties properties,
            org.springframework.context.ApplicationContext applicationContext,
            org.springframework.core.env.Environment environment) {
        return new IdemboxCleanerTaskRegistrar(
                scheduler, outboxCleanerProvider, idempotencyCleanerProvider, outboxPollerProvider, properties, applicationContext, environment);
    }


    static class JdbcInfrastructureCondition implements Condition {
        @Override
        public boolean matches(
                @NonNull ConditionContext context,
                @NonNull AnnotatedTypeMetadata metadata) {
            ConfigurableListableBeanFactory beanFactory = context.getBeanFactory();
            if (beanFactory == null) {
                return false;
            }
            if (beanFactory.getBeanNamesForType(JdbcTemplate.class, true, false).length > 0) {
                return true;
            }
            return beanFactory.getBeanNamesForType(javax.sql.DataSource.class, true, false).length > 0;
        }
    }

    static JdbcTemplate resolveInfrastructureJdbcTemplate(
            ObjectProvider<JdbcTemplate> dedicatedJdbcTemplateProvider,
            ObjectProvider<javax.sql.DataSource> dedicatedDataSourceProvider,
            ObjectProvider<JdbcTemplate> defaultJdbcTemplateProvider,
            ObjectProvider<javax.sql.DataSource> defaultDataSourceProvider) {
        JdbcTemplate template = dedicatedJdbcTemplateProvider != null ? dedicatedJdbcTemplateProvider.getIfAvailable() : null;
        if (template != null) {
            return template;
        }
        javax.sql.DataSource ds = dedicatedDataSourceProvider != null ? dedicatedDataSourceProvider.getIfAvailable() : null;
        if (ds != null) {
            return new JdbcTemplate(ds);
        }
        template = defaultJdbcTemplateProvider != null ? defaultJdbcTemplateProvider.getIfAvailable() : null;
        if (template != null) {
            return template;
        }
        ds = defaultDataSourceProvider != null ? defaultDataSourceProvider.getIfAvailable() : null;
        if (ds != null) {
            return new JdbcTemplate(ds);
        }
        return null;
    }

    @Bean(name = "idemboxDatabaseInitializer")
    @ConditionalOnMissingBean(name = "idemboxDatabaseInitializer")
    @ConditionalOnProperty(prefix = "idembox", name = "initialize-schema", havingValue = "true")
    @Conditional(JdbcInfrastructureCondition.class)
    public IdemboxDatabaseInitializer idemboxDatabaseInitializer(
            @Qualifier("idemboxDataSource") ObjectProvider<javax.sql.DataSource> dedicatedDataSourceProvider,
            ObjectProvider<javax.sql.DataSource> defaultDataSourceProvider,
            @Qualifier("idemboxJdbcTemplate") ObjectProvider<JdbcTemplate> dedicatedJdbcTemplateProvider,
            ObjectProvider<JdbcTemplate> defaultJdbcTemplateProvider,
            IdemboxProperties properties,
            ResourceLoader resourceLoader) {
        javax.sql.DataSource dataSource = dedicatedDataSourceProvider != null ? dedicatedDataSourceProvider.getIfAvailable() : null;
        if (dataSource == null && defaultDataSourceProvider != null) {
            dataSource = defaultDataSourceProvider.getIfAvailable();
        }
        if (dataSource == null) {
            JdbcTemplate jt = dedicatedJdbcTemplateProvider != null ? dedicatedJdbcTemplateProvider.getIfAvailable() : null;
            if (jt == null && defaultJdbcTemplateProvider != null) {
                jt = defaultJdbcTemplateProvider.getIfAvailable();
            }
            if (jt != null) {
                dataSource = jt.getDataSource();
            }
        }
        if (dataSource == null) {
            throw new IllegalStateException("No DataSource available to initialize Idembox database schema");
        }
        org.springframework.boot.sql.init.DatabaseInitializationSettings settings =
                new org.springframework.boot.sql.init.DatabaseInitializationSettings();
        List<String> locations = properties.getSchemaLocation() != null && !properties.getSchemaLocation().isBlank()
                ? java.util.Arrays.stream(properties.getSchemaLocation().split(","))
                        .map(String::trim)
                        .filter(s -> !s.isEmpty())
                        .toList()
                : List.of("classpath:db/migration/idembox/schema-postgresql.sql");
        settings.setSchemaLocations(locations);
        settings.setMode(org.springframework.boot.sql.init.DatabaseInitializationMode.ALWAYS);
        settings.setContinueOnError(false);
        return new IdemboxDatabaseInitializer(dataSource, settings, properties, resourceLoader);
    }

    @Bean
    @ConditionalOnMissingBean
    public SpelKeyExtractor spelKeyExtractor() {
        return new SpelKeyExtractor();
    }

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "idembox.metrics", name = "enabled", havingValue = "true", matchIfMissing = true)
    public IdemboxMetrics idemboxMetrics(ObjectProvider<MeterRegistry> meterRegistryProvider) {
        MeterRegistry registry = meterRegistryProvider.getIfAvailable();
        return new IdemboxMetrics(registry);
    }

    @Bean
    @ConditionalOnMissingBean
    public IdemboxMetrics noOpIdemboxMetrics() {
        return IdemboxMetrics.noOp();
    }

    @Bean
    @ConditionalOnMissingBean(ObjectMapper.class)
    public ObjectMapper objectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.findAndRegisterModules();
        return mapper;
    }

    // ==========================================================
    // Idempotency Components (Independent of Kafka)
    // ==========================================================

    static boolean isCompatibleDataSource(PlatformTransactionManager tm, javax.sql.DataSource targetDs) {
        if (tm == null) {
            return false;
        }
        if (tm.getClass().getName().contains("Kafka")) {
            return false;
        }
        if (targetDs == null) {
            return true;
        }
        if (tm instanceof org.springframework.jdbc.datasource.DataSourceTransactionManager dataSourceTxManager) {
            return dataSourceTxManager.getDataSource() == null || dataSourceTxManager.getDataSource() == targetDs;
        }
        try {
            java.lang.reflect.Method getDsMethod = tm.getClass().getMethod("getDataSource");
            Object ds = getDsMethod.invoke(tm);
            if (ds instanceof javax.sql.DataSource tmDs) {
                return tmDs == targetDs;
            }
        } catch (Throwable ignored) {
        }
        return true;
    }

    static PlatformTransactionManager resolveTransactionManager(
            JdbcTemplate jdbcTemplate,
            ObjectProvider<PlatformTransactionManager> txManagerProvider) {
        javax.sql.DataSource targetDs = (jdbcTemplate != null) ? jdbcTemplate.getDataSource() : null;
        PlatformTransactionManager txManager = null;
        try {
            PlatformTransactionManager candidate = txManagerProvider.getIfAvailable();
            if (isCompatibleDataSource(candidate, targetDs)) {
                txManager = candidate;
            }
        } catch (Exception ignored) {
        }
        if (txManager == null) {
            try {
                txManager = txManagerProvider.orderedStream()
                        .filter(tm -> isCompatibleDataSource(tm, targetDs))
                        .findFirst()
                        .orElse(null);
            } catch (Exception ignored) {
            }
        }
        if (txManager == null && targetDs != null) {
            txManager = new org.springframework.jdbc.datasource.DataSourceTransactionManager(targetDs);
        }
        return txManager;
    }

    @Bean
    @ConditionalOnMissingBean(IdempotentRepository.class)
    @Conditional(JdbcInfrastructureCondition.class)
    public IdempotentRepository idempotentRepository(
            @Qualifier("idemboxJdbcTemplate") ObjectProvider<JdbcTemplate> dedicatedJdbcTemplateProvider,
            @Qualifier("idemboxDataSource") ObjectProvider<javax.sql.DataSource> dedicatedDataSourceProvider,
            ObjectProvider<JdbcTemplate> defaultJdbcTemplateProvider,
            ObjectProvider<javax.sql.DataSource> defaultDataSourceProvider,
            IdemboxProperties properties,
            ObjectProvider<PlatformTransactionManager> txManagerProvider) {
        JdbcTemplate jdbcTemplate = resolveInfrastructureJdbcTemplate(
                dedicatedJdbcTemplateProvider,
                dedicatedDataSourceProvider,
                defaultJdbcTemplateProvider,
                defaultDataSourceProvider);
        if (jdbcTemplate == null) {
            throw new IllegalStateException("Neither JdbcTemplate nor DataSource bean found in application context to initialize IdempotentRepository");
        }
        PlatformTransactionManager txManager = resolveTransactionManager(jdbcTemplate, txManagerProvider);
        return new PostgresIdempotentRepository(jdbcTemplate, txManager, properties.resolveIdempotentTableName());
    }

    @Bean
    @org.springframework.core.annotation.Order(org.springframework.core.Ordered.HIGHEST_PRECEDENCE)
    @ConditionalOnMissingBean(IdempotentConsumerAspect.class)
    @ConditionalOnProperty(prefix = "idembox.idempotency", name = "enabled", havingValue = "true", matchIfMissing = true)
    public IdempotentConsumerAspect idempotentConsumerAspect(
            IdempotentRepository idempotentRepository,
            SpelKeyExtractor spelKeyExtractor,
            IdemboxProperties properties,
            IdemboxMetrics idemboxMetrics,
            @Qualifier("idemboxTaskExecutor") ObjectProvider<Executor> taskExecutorProvider,
            @Qualifier("outboxAsyncExecutor") ObjectProvider<Executor> outboxExecutorProvider) {
        Executor executor = taskExecutorProvider.getIfAvailable(outboxExecutorProvider::getIfAvailable);
        return new IdempotentConsumerAspect(idempotentRepository, spelKeyExtractor, properties, idemboxMetrics, executor);
    }

    @Bean
    @ConditionalOnMissingBean(IdempotentTransactionSyncAspect.class)
    @ConditionalOnProperty(prefix = "idembox.idempotency", name = "enabled", havingValue = "true", matchIfMissing = true)
    public IdempotentTransactionSyncAspect idempotentTransactionSyncAspect(
            IdempotentRepository idempotentRepository,
            IdemboxMetrics idemboxMetrics) {
        return new IdempotentTransactionSyncAspect(idempotentRepository, idemboxMetrics);
    }

    @Bean
    @ConditionalOnMissingBean(IdempotencyCleaner.class)
    @ConditionalOnBean(IdempotentRepository.class)
    @ConditionalOnProperty(prefix = "idembox.idempotency", name = "enabled", havingValue = "true", matchIfMissing = true)
    public IdempotencyCleaner idempotencyCleaner(
            IdempotentRepository idempotentRepository,
            IdemboxProperties properties) {
        return new IdempotencyCleaner(idempotentRepository, properties);
    }

    @Bean
    @ConditionalOnMissingBean(IdempotentStartupValidator.class)
    @ConditionalOnProperty(prefix = "idembox.idempotency", name = "enabled", havingValue = "true", matchIfMissing = true)
    public IdempotentStartupValidator idempotentStartupValidator(IdemboxProperties properties) {
        return new IdempotentStartupValidator(properties);
    }

    // ==========================================================
    // Consumer DLT Components (Require Kafka, DLT recoverer, and IdempotentRepository)
    // ==========================================================

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass({KafkaOperations.class, DeadLetterPublishingRecoverer.class})
    @ConditionalOnProperty(prefix = "idembox.idempotency", name = "enabled", havingValue = "true", matchIfMissing = true)
    static class ConsumerDltConfiguration {

        @Bean
        @ConditionalOnMissingBean(DeadLetterPublishingRecoverer.class)
        public IdemboxDeadLetterPublishingRecoverer idemboxDeadLetterPublishingRecoverer(
                @Qualifier("idemboxOutboxKafkaOperations") ObjectProvider<KafkaOperations<?, ?>> outboxOperationsProvider,
                ObjectProvider<KafkaOperations<?, ?>> defaultOperationsProvider,
                ObjectProvider<IdempotentRepository> idempotentRepositoryProvider,
                ObjectProvider<ObjectMapper> objectMapperProvider,
                IdemboxProperties properties,
                org.springframework.context.ApplicationContext applicationContext) {
            KafkaOperations<?, ?> operations = outboxOperationsProvider.getIfUnique();
            if (operations == null && applicationContext != null && applicationContext.containsBean("idemboxOutboxKafkaOperations")) {
                operations = applicationContext.getBean("idemboxOutboxKafkaOperations", KafkaOperations.class);
            }
            if (operations == null) {
                operations = OutboxConfiguration.resolveDefaultKafkaOperations(defaultOperationsProvider, applicationContext);
            }
            List<String> keyFields = (properties != null && properties.getIdempotency() != null)
                    ? properties.getIdempotency().getKeyFields()
                    : null;
            IdemboxProperties.DltProperties dltProps = (properties != null)
                    ? properties.getDlt()
                    : null;
            java.time.Duration retention = (properties != null && properties.getIdempotency() != null)
                    ? io.github.alexlitvinov.idembox.core.spel.DurationParser.parse(
                            properties.getIdempotency().getDefaultTtl(), java.time.Duration.ofDays(7))
                    : java.time.Duration.ofDays(7);
            return new IdemboxDeadLetterPublishingRecoverer(
                    operations,
                    idempotentRepositoryProvider,
                    null,
                    objectMapperProvider.getIfAvailable(),
                    retention,
                    keyFields,
                    dltProps
            );
        }
    }

    // ==========================================================
    // Outbox Components (Require Kafka and Outbox enabled)
    // ==========================================================

    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(KafkaOperations.class)
    @ConditionalOnProperty(prefix = "idembox.outbox", name = "enabled", havingValue = "true", matchIfMissing = true)
    static class OutboxConfiguration {

        @Bean
        @ConditionalOnMissingBean(OutboxRepository.class)
        @Conditional(JdbcInfrastructureCondition.class)
        public OutboxRepository outboxRepository(
                @Qualifier("idemboxJdbcTemplate") ObjectProvider<JdbcTemplate> dedicatedJdbcTemplateProvider,
                @Qualifier("idemboxDataSource") ObjectProvider<javax.sql.DataSource> dedicatedDataSourceProvider,
                ObjectProvider<JdbcTemplate> defaultJdbcTemplateProvider,
                ObjectProvider<javax.sql.DataSource> defaultDataSourceProvider,
                ObjectProvider<ObjectMapper> objectMapperProvider,
                IdemboxMetrics idemboxMetrics,
                ObjectProvider<PlatformTransactionManager> txManagerProvider,
                IdemboxProperties properties) {
            JdbcTemplate jdbcTemplate = resolveInfrastructureJdbcTemplate(
                    dedicatedJdbcTemplateProvider,
                    dedicatedDataSourceProvider,
                    defaultJdbcTemplateProvider,
                    defaultDataSourceProvider);
            if (jdbcTemplate == null) {
                throw new IllegalStateException("Neither JdbcTemplate nor DataSource bean found in application context to initialize OutboxRepository");
            }
            PlatformTransactionManager txManager = resolveTransactionManager(jdbcTemplate, txManagerProvider);
            ObjectMapper om = objectMapperProvider != null ? objectMapperProvider.getIfAvailable() : null;
            if (om == null) {
                om = new ObjectMapper();
                om.findAndRegisterModules();
            }
            PostgresOutboxRepository repository = new PostgresOutboxRepository(
                    jdbcTemplate, om, txManager, idemboxMetrics, properties.resolveOutboxTableName());
            idemboxMetrics.registerPendingMessagesGauge(repository::getPendingMessagesCount);
            return repository;
        }

        @Bean
        @ConditionalOnMissingBean(OutboxPublisher.class)
        public OutboxPublisher outboxPublisher(
                OutboxRepository outboxRepository,
                ObjectProvider<ObjectMapper> objectMapperProvider,
                IdemboxProperties properties,
                ApplicationEventPublisher eventPublisher,
                IdemboxMetrics idemboxMetrics,
                ApplicationContext applicationContext) {
            ObjectMapper om = objectMapperProvider != null ? objectMapperProvider.getIfAvailable() : null;
            if (om == null) {
                om = new ObjectMapper();
                om.findAndRegisterModules();
            }
            if (ClassUtils.isPresent("io.micrometer.tracing.Tracer", applicationContext.getClassLoader())) {
                return TracingHelper.createPublisher(
                        outboxRepository,
                        om,
                        properties,
                        eventPublisher,
                        idemboxMetrics,
                        applicationContext
                );
            }
            return new DefaultOutboxPublisher(
                    outboxRepository,
                    om,
                    properties,
                    eventPublisher,
                    idemboxMetrics
            );
        }

        private static class TracingHelper {
            static OutboxPublisher createPublisher(
                    OutboxRepository outboxRepository,
                    ObjectMapper om,
                    IdemboxProperties properties,
                    ApplicationEventPublisher eventPublisher,
                    IdemboxMetrics idemboxMetrics,
                    ApplicationContext context) {
                io.micrometer.tracing.Tracer tracer = null;
                try {
                    tracer = context.getBeanProvider(io.micrometer.tracing.Tracer.class).getIfAvailable();
                } catch (Throwable ignored) {
                }
                return new DefaultOutboxPublisher(
                        outboxRepository,
                        om,
                        properties,
                        eventPublisher,
                        idemboxMetrics,
                        tracer
                );
            }
        }

        static int parseIntConfig(Object value, int defaultValue) {
            if (value instanceof Number number) {
                return number.intValue();
            }
            if (value instanceof CharSequence cs) {
                try {
                    return Integer.parseInt(cs.toString().trim());
                } catch (NumberFormatException ignored) {
                    return defaultValue;
                }
            }
            return defaultValue;
        }

        static KafkaOperations<?, ?> resolveDefaultKafkaOperations(
                ObjectProvider<KafkaOperations<?, ?>> provider,
                org.springframework.context.ApplicationContext applicationContext) {
            if (provider != null) {
                KafkaOperations<?, ?> unique = provider.getIfUnique();
                if (unique != null) {
                    return unique;
                }
            }
            if (applicationContext != null && applicationContext.containsBean("kafkaTemplate")) {
                try {
                    return applicationContext.getBean("kafkaTemplate", KafkaOperations.class);
                } catch (Exception ignored) {
                }
            }
            if (provider != null) {
                try {
                    return provider.orderedStream().findFirst().orElse(null);
                } catch (Exception ignored) {
                }
            }
            return null;
        }

        static class OnKafkaOperationsOrProducerFactoryCondition implements Condition {
            @Override
            public boolean matches(
                    @NonNull ConditionContext context,
                    @NonNull AnnotatedTypeMetadata metadata) {
                ConfigurableListableBeanFactory beanFactory = context.getBeanFactory();
                if (beanFactory == null) {
                    return false;
                }
                if (beanFactory.getBeanNamesForType(KafkaOperations.class, true, false).length > 0) {
                    return true;
                }
                if (beanFactory.getBeanNamesForType(org.springframework.kafka.core.ProducerFactory.class, true, false).length > 0) {
                    return true;
                }
                return beanFactory.containsBean("idemboxOutboxKafkaOperations");
            }
        }

        @Bean(name = "idemboxOutboxKafkaOperations", autowireCandidate = false)
        @ConditionalOnMissingBean(name = "idemboxOutboxKafkaOperations")
        @ConditionalOnBean(org.springframework.kafka.core.ProducerFactory.class)
        public KafkaOperations<?, ?> idemboxOutboxKafkaOperations(
                org.springframework.kafka.core.ProducerFactory<?, ?> producerFactory,
                ObjectProvider<KafkaOperations<?, ?>> defaultKafkaOperations,
                IdemboxProperties properties,
                org.springframework.context.ApplicationContext applicationContext,
                ObjectProvider<ObjectMapper> objectMapperProvider) {
            if (producerFactory instanceof org.springframework.kafka.core.DefaultKafkaProducerFactory<?, ?> defaultProducerFactory) {
                // When spring.kafka.producer.transaction-id-prefix is configured, Spring Boot creates a transactional producer factory.
                // OutboxPoller requires non-transactional Kafka publishing to avoid holding or creating 2PC transactions.
                // Outbox messages are serialized to JSON String or raw byte[] (Base64 decoded). If the application configures
                // JsonSerializer, Kafka will double-serialize Strings (e.g. "\"{\\\"id\\\":...}\""). We use OutboxPayloadSerializer
                // which seamlessly supports both String and byte[] payloads without ClassCastException.
                java.util.Map<String, Object> configs = new java.util.HashMap<>(defaultProducerFactory.getConfigurationProperties());
                configs.remove(org.apache.kafka.clients.producer.ProducerConfig.TRANSACTIONAL_ID_CONFIG);
                long sendTimeoutMs = Math.max(1000L, properties.getOutbox().getSendTimeout().toMillis());
                configs.put(org.apache.kafka.clients.producer.ProducerConfig.MAX_BLOCK_MS_CONFIG, sendTimeoutMs);
                configs.put(org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);
                configs.put(org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, io.github.alexlitvinov.idembox.starter.serializer.OutboxPayloadSerializer.class);

                ObjectMapper om = objectMapperProvider.getIfAvailable();
                if (om != null) {
                    configs.put(io.github.alexlitvinov.idembox.starter.serializer.OutboxPayloadSerializer.OBJECT_MAPPER_CONFIG, om);
                }

                long deliveryTimeoutMs = properties.getOutbox().getKafkaDeliveryTimeout() != null
                        ? properties.getOutbox().getKafkaDeliveryTimeout().toMillis()
                        : sendTimeoutMs;
                if (!configs.containsKey(org.apache.kafka.clients.producer.ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG)) {
                    int safeDeliveryTimeout = (int) Math.min(Integer.MAX_VALUE, Math.max(sendTimeoutMs, deliveryTimeoutMs));
                    configs.put(org.apache.kafka.clients.producer.ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, safeDeliveryTimeout);

                    int lingerMs = parseIntConfig(
                            configs.get(org.apache.kafka.clients.producer.ProducerConfig.LINGER_MS_CONFIG), 0);
                    int maxRequestTimeoutMs = Math.max(500, safeDeliveryTimeout - Math.max(0, lingerMs));
                    Object existingReqTimeout = configs.get(org.apache.kafka.clients.producer.ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG);
                    if (existingReqTimeout == null
                            || parseIntConfig(existingReqTimeout, Integer.MAX_VALUE) > maxRequestTimeoutMs) {
                        configs.put(org.apache.kafka.clients.producer.ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, maxRequestTimeoutMs);
                    }
                }

                org.springframework.kafka.core.DefaultKafkaProducerFactory<String, Object> nonTxFactory =
                        new org.springframework.kafka.core.DefaultKafkaProducerFactory<>(
                                configs,
                                org.apache.kafka.common.serialization.StringSerializer::new,
                                () -> new io.github.alexlitvinov.idembox.starter.serializer.OutboxPayloadSerializer(om)
                        );
                return new org.springframework.kafka.core.KafkaTemplate<>(nonTxFactory) {
                    @Override
                    public void destroy() {
                        super.destroy();
                        nonTxFactory.destroy();
                    }
                };
            }
            KafkaOperations<?, ?> fallback = resolveDefaultKafkaOperations(defaultKafkaOperations, applicationContext);
            if (fallback != null) {
                return fallback;
            }
            return createKafkaTemplate(producerFactory);
        }

        private static <K, V> org.springframework.kafka.core.KafkaTemplate<K, V> createKafkaTemplate(
                org.springframework.kafka.core.ProducerFactory<K, V> producerFactory) {
            return new org.springframework.kafka.core.KafkaTemplate<>(producerFactory);
        }

        @Bean
        @ConditionalOnMissingBean(OutboxPoller.class)
        @org.springframework.context.annotation.Conditional(OnKafkaOperationsOrProducerFactoryCondition.class)
        @ConditionalOnProperty(prefix = "idembox.outbox.poller", name = "enabled", havingValue = "true", matchIfMissing = true)
        public OutboxPoller outboxPoller(
                OutboxRepository outboxRepository,
                @org.springframework.beans.factory.annotation.Qualifier("idemboxOutboxKafkaOperations")
                ObjectProvider<KafkaOperations<?, ?>> outboxOperationsProvider,
                ObjectProvider<KafkaOperations<?, ?>> defaultOperationsProvider,
                IdemboxProperties properties,
                IdemboxMetrics idemboxMetrics,
                @org.springframework.beans.factory.annotation.Qualifier("idemboxTaskExecutor")
                ObjectProvider<TaskExecutor> taskExecutorProvider,
                @org.springframework.beans.factory.annotation.Qualifier("outboxAsyncExecutor")
                ObjectProvider<TaskExecutor> outboxAsyncExecutorProvider,
                org.springframework.context.ApplicationContext applicationContext,
                ObjectProvider<ObjectMapper> objectMapperProvider) {
            KafkaOperations<?, ?> operations = outboxOperationsProvider.getIfUnique();
            if (operations == null && applicationContext != null && applicationContext.containsBean("idemboxOutboxKafkaOperations")) {
                operations = applicationContext.getBean("idemboxOutboxKafkaOperations", KafkaOperations.class);
            }
            if (operations == null) {
                operations = resolveDefaultKafkaOperations(defaultOperationsProvider, applicationContext);
            }
            TaskExecutor executor = taskExecutorProvider.getIfAvailable(outboxAsyncExecutorProvider::getIfAvailable);
            ObjectMapper mapper = objectMapperProvider.getIfAvailable();
            return new OutboxPoller(outboxRepository, operations, properties, idemboxMetrics, executor, mapper);
        }

        @Bean
        @ConditionalOnMissingBean(OutboxCleaner.class)
        @ConditionalOnBean(OutboxRepository.class)
        @ConditionalOnProperty(prefix = "idembox.outbox.cleaner", name = "enabled", havingValue = "true", matchIfMissing = true)
        public OutboxCleaner outboxCleaner(
                OutboxRepository outboxRepository,
                IdemboxProperties properties) {
            return new OutboxCleaner(outboxRepository, properties);
        }
    }
}
