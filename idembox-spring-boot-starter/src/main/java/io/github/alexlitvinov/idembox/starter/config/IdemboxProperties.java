package io.github.alexlitvinov.idembox.starter.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.core.Ordered;
import org.springframework.util.unit.DataSize;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

@Validated
@ConfigurationProperties(prefix = "idembox")
public class IdemboxProperties {

    @Valid
    @NotNull
    private OutboxProperties outbox = new OutboxProperties();

    @Valid
    @NotNull
    private IdempotencyProperties idempotency = new IdempotencyProperties();

    private boolean initializeSchema = false;

    private String schemaLocation = "classpath:db/migration/idembox/schema-postgresql.sql";

    private String tableSchema;

    private boolean createSchema = false;

    private String tablePrefix;

    @Valid
    @NotNull
    private MetricsProperties metrics = new MetricsProperties();

    @Valid
    @NotNull
    private DltProperties dlt = new DltProperties();

    @Positive
    @Min(1)
    private int schedulerPoolSize = 4;

    @Valid
    @NotNull
    private SchedulingProperties scheduling = new SchedulingProperties();

    private boolean enableScheduling = false;

    public OutboxProperties getOutbox() {
        return outbox;
    }

    public void setOutbox(OutboxProperties outbox) {
        this.outbox = outbox;
    }

    public IdempotencyProperties getIdempotency() {
        return idempotency;
    }

    public void setIdempotency(IdempotencyProperties idempotency) {
        this.idempotency = idempotency;
    }

    public boolean isInitializeSchema() {
        return initializeSchema;
    }

    public void setInitializeSchema(boolean initializeSchema) {
        this.initializeSchema = initializeSchema;
    }

    public String getSchemaLocation() {
        return schemaLocation;
    }

    public void setSchemaLocation(String schemaLocation) {
        this.schemaLocation = schemaLocation;
    }

    public int getSchedulerPoolSize() {
        return schedulerPoolSize;
    }

    public void setSchedulerPoolSize(int schedulerPoolSize) {
        this.schedulerPoolSize = schedulerPoolSize;
    }

    public SchedulingProperties getScheduling() {
        return scheduling;
    }

    public void setScheduling(SchedulingProperties scheduling) {
        this.scheduling = scheduling;
    }

    public boolean isEnableScheduling() {
        return (scheduling != null && scheduling.isEnabled()) || enableScheduling;
    }

    public void setEnableScheduling(boolean enableScheduling) {
        this.enableScheduling = enableScheduling;
        if (this.scheduling != null) {
            this.scheduling.setEnabled(enableScheduling);
        }
    }

    public String getTableSchema() {
        return tableSchema;
    }

    public void setTableSchema(String tableSchema) {
        this.tableSchema = tableSchema;
    }

    public boolean isCreateSchema() {
        return createSchema;
    }

    public void setCreateSchema(boolean createSchema) {
        this.createSchema = createSchema;
    }

    public String getTablePrefix() {
        return tablePrefix;
    }

    public void setTablePrefix(String tablePrefix) {
        this.tablePrefix = tablePrefix;
    }

    public MetricsProperties getMetrics() {
        return metrics;
    }

    public void setMetrics(MetricsProperties metrics) {
        this.metrics = metrics;
    }

    public DltProperties getDlt() {
        return dlt;
    }

    public void setDlt(DltProperties dlt) {
        this.dlt = dlt;
    }

    public String resolveOutboxTableName() {
        return io.github.alexlitvinov.idembox.starter.util.TableUtils.resolveTableName(tableSchema, tablePrefix, "outbox_messages");
    }

    public String resolveIdempotentTableName() {
        return io.github.alexlitvinov.idembox.starter.util.TableUtils.resolveTableName(tableSchema, tablePrefix, "idempotent_records");
    }

    public static class OutboxProperties {
        private boolean enabled = true;

        @Positive
        @Min(1)
        private long pollerRateMs = 500;

        @Positive
        @Min(1)
        private int batchSize = 50;

        private boolean immediateFlush = true;

        @NotNull
        private Duration retention = Duration.ofDays(7);

        @NotNull
        private Duration failedRetention = Duration.ofDays(30);

        @NotNull
        private Duration sendTimeout = Duration.ofSeconds(10);

        @Min(0)
        private int maxRetries = 5;

        @NotNull
        private Duration initialBackoff = Duration.ofSeconds(1);

        @NotNull
        private Duration maxBackoff = Duration.ofMinutes(10);

        @DecimalMin("1.0")
        private double backoffMultiplier = 2.0;

        @NotNull
        private Duration inProgressLockDuration = Duration.ofMinutes(5);

        @Positive
        @Min(1)
        private int maxDrainBatches = 10;

        @NotNull
        private Duration brokerCooldown = Duration.ofSeconds(10);

        @Valid
        @NotNull
        private PollerProperties poller = new PollerProperties();

        @Valid
        @NotNull
        private CleanerProperties cleaner = new CleanerProperties();

        @NotBlank
        private String cleanupCron = "0 0 * * * *";

        @NotNull
        private DataSize maxPayloadSize = DataSize.ofMegabytes(1);

        @NotNull
        private DataSize maxHeaderSize = DataSize.ofKilobytes(32);

        private boolean preserveCreationTimestamp = false;
        private boolean addTypeIdHeader = true;

        @NotNull
        private Duration kafkaDeliveryTimeout = Duration.ofMinutes(2);

        @NotNull
        private Duration maxOutageDuration = Duration.ofHours(24);

        @SuppressWarnings("BooleanMethodIsAlwaysInverted")
        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public long getPollerRateMs() {
            return poller != null ? poller.getRateMs() : pollerRateMs;
        }

        public void setPollerRateMs(long pollerRateMs) {
            this.pollerRateMs = pollerRateMs;
            if (this.poller != null) {
                this.poller.setRateMs(pollerRateMs);
            }
        }

        public int getBatchSize() {
            return batchSize;
        }

        public void setBatchSize(int batchSize) {
            this.batchSize = batchSize;
        }

        public boolean isImmediateFlush() {
            return immediateFlush;
        }

        public void setImmediateFlush(boolean immediateFlush) {
            this.immediateFlush = immediateFlush;
        }

        public Duration getRetention() {
            return retention;
        }

        public void setRetention(Duration retention) {
            this.retention = retention;
        }

        public Duration getFailedRetention() {
            return failedRetention;
        }

        public void setFailedRetention(Duration failedRetention) {
            this.failedRetention = failedRetention;
        }

        public Duration getSendTimeout() {
            return sendTimeout;
        }

        public void setSendTimeout(Duration sendTimeout) {
            this.sendTimeout = sendTimeout;
        }

        public int getMaxRetries() {
            return maxRetries;
        }

        public void setMaxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
        }

        public Duration getInitialBackoff() {
            return initialBackoff;
        }

        public void setInitialBackoff(Duration initialBackoff) {
            this.initialBackoff = initialBackoff;
        }

        public Duration getMaxBackoff() {
            return maxBackoff;
        }

        public void setMaxBackoff(Duration maxBackoff) {
            this.maxBackoff = maxBackoff;
        }

        public double getBackoffMultiplier() {
            return backoffMultiplier;
        }

        public void setBackoffMultiplier(double backoffMultiplier) {
            this.backoffMultiplier = backoffMultiplier;
        }

        public Duration getInProgressLockDuration() {
            return inProgressLockDuration;
        }

        public void setInProgressLockDuration(Duration inProgressLockDuration) {
            this.inProgressLockDuration = inProgressLockDuration;
        }

        public int getMaxDrainBatches() {
            return maxDrainBatches;
        }

        public void setMaxDrainBatches(int maxDrainBatches) {
            this.maxDrainBatches = maxDrainBatches;
        }

        public Duration getBrokerCooldown() {
            return brokerCooldown;
        }

        public void setBrokerCooldown(Duration brokerCooldown) {
            this.brokerCooldown = brokerCooldown;
        }

        public String getCleanupCron() {
            return cleanupCron;
        }

        public void setCleanupCron(String cleanupCron) {
            this.cleanupCron = cleanupCron;
        }

        public DataSize getMaxPayloadSize() {
            return maxPayloadSize;
        }

        public void setMaxPayloadSize(DataSize maxPayloadSize) {
            this.maxPayloadSize = maxPayloadSize;
        }

        public DataSize getMaxHeaderSize() {
            return maxHeaderSize;
        }

        public void setMaxHeaderSize(DataSize maxHeaderSize) {
            this.maxHeaderSize = maxHeaderSize;
        }

        public boolean isPreserveCreationTimestamp() {
            return preserveCreationTimestamp;
        }

        public void setPreserveCreationTimestamp(boolean preserveCreationTimestamp) {
            this.preserveCreationTimestamp = preserveCreationTimestamp;
        }

        public boolean isAddTypeIdHeader() {
            return addTypeIdHeader;
        }

        public void setAddTypeIdHeader(boolean addTypeIdHeader) {
            this.addTypeIdHeader = addTypeIdHeader;
        }

        public Duration getKafkaDeliveryTimeout() {
            return kafkaDeliveryTimeout;
        }

        public void setKafkaDeliveryTimeout(Duration kafkaDeliveryTimeout) {
            this.kafkaDeliveryTimeout = kafkaDeliveryTimeout;
        }

        public Duration getMaxOutageDuration() {
            return maxOutageDuration;
        }

        public void setMaxOutageDuration(Duration maxOutageDuration) {
            this.maxOutageDuration = maxOutageDuration;
        }

        public PollerProperties getPoller() {
            return poller;
        }

        public void setPoller(PollerProperties poller) {
            this.poller = poller;
        }

        public CleanerProperties getCleaner() {
            return cleaner;
        }

        public void setCleaner(CleanerProperties cleaner) {
            this.cleaner = cleaner;
        }

        public static class PollerProperties {
            private boolean enabled = true;

            @Positive
            @Min(1)
            private long rateMs = 500;

            @SuppressWarnings("BooleanMethodIsAlwaysInverted")
            public boolean isEnabled() {
                return enabled;
            }

            public void setEnabled(boolean enabled) {
                this.enabled = enabled;
            }

            public long getRateMs() {
                return rateMs;
            }

            public void setRateMs(long rateMs) {
                this.rateMs = rateMs;
            }
        }
    }

    public static class IdempotencyProperties {
        private boolean enabled = true;

        private int order = Ordered.HIGHEST_PRECEDENCE;

        @NotBlank
        private String defaultTtl = "7d";

        @NotBlank
        private String defaultLockTimeout = "2m";

        @NotNull
        private Duration inFlightWaitTimeout = Duration.ofSeconds(3);

        @NotBlank
        private String cleanupCron = "0 0 * * * *"; // Hourly

        @Valid
        @NotNull
        private CleanerProperties cleaner = new CleanerProperties();

        private boolean failOnExistingTransaction = true;

        private boolean failOnConditionError = true;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public boolean isFailOnConditionError() {
            return failOnConditionError;
        }

        public void setFailOnConditionError(boolean failOnConditionError) {
            this.failOnConditionError = failOnConditionError;
        }

        public boolean isFailOnExistingTransaction() {
            return failOnExistingTransaction;
        }

        public void setFailOnExistingTransaction(boolean failOnExistingTransaction) {
            this.failOnExistingTransaction = failOnExistingTransaction;
        }

        public int getOrder() {
            return order;
        }

        public void setOrder(int order) {
            this.order = order;
        }

        public CleanerProperties getCleaner() {
            return cleaner;
        }

        public void setCleaner(CleanerProperties cleaner) {
            this.cleaner = cleaner;
        }

        public String getDefaultTtl() {
            return defaultTtl;
        }

        public void setDefaultTtl(String defaultTtl) {
            this.defaultTtl = defaultTtl;
        }

        public String getDefaultLockTimeout() {
            return defaultLockTimeout;
        }

        public void setDefaultLockTimeout(String defaultLockTimeout) {
            this.defaultLockTimeout = defaultLockTimeout;
        }

        public static final Duration MAX_IN_FLIGHT_WAIT_TIMEOUT = Duration.ofSeconds(5);

        public Duration getInFlightWaitTimeout() {
            return inFlightWaitTimeout;
        }

        public void setInFlightWaitTimeout(Duration inFlightWaitTimeout) {
            if (inFlightWaitTimeout != null && !inFlightWaitTimeout.isNegative()) {
                this.inFlightWaitTimeout = inFlightWaitTimeout.compareTo(MAX_IN_FLIGHT_WAIT_TIMEOUT) > 0
                        ? MAX_IN_FLIGHT_WAIT_TIMEOUT
                        : inFlightWaitTimeout;
            } else {
                this.inFlightWaitTimeout = Duration.ofSeconds(3);
            }
        }

        public String getCleanupCron() {
            return cleanupCron;
        }

        public void setCleanupCron(String cleanupCron) {
            this.cleanupCron = cleanupCron;
        }

        private List<String> keyFields = new ArrayList<>(List.of("id", "orderId", "key"));

        public List<String> getKeyFields() {
            return keyFields;
        }

        public void setKeyFields(List<String> keyFields) {
            this.keyFields = (keyFields != null && !keyFields.isEmpty())
                    ? new ArrayList<>(keyFields)
                    : new ArrayList<>(List.of("id", "orderId", "key"));
        }
    }

    public static class CleanerProperties {
        private boolean enabled = true;

        @Positive
        @Min(1)
        private int batchSize = 5000;

        @Positive
        @Min(1)
        private int maxBatchesPerRun = 100;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }

        public int getBatchSize() {
            return batchSize;
        }

        public void setBatchSize(int batchSize) {
            this.batchSize = batchSize;
        }

        public int getMaxBatchesPerRun() {
            return maxBatchesPerRun;
        }

        public void setMaxBatchesPerRun(int maxBatchesPerRun) {
            this.maxBatchesPerRun = maxBatchesPerRun;
        }
    }

    public static class MetricsProperties {
        private boolean enabled = true;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }
    }

    public static class DltProperties {
        private String topicSuffix = ".DLT";
        private String topicPattern;

        public String getTopicSuffix() {
            return topicSuffix;
        }

        public void setTopicSuffix(String topicSuffix) {
            this.topicSuffix = topicSuffix;
        }

        public String getTopicPattern() {
            return topicPattern;
        }

        public void setTopicPattern(String topicPattern) {
            this.topicPattern = topicPattern;
        }

        public String resolveDltTopic(String baseTopic) {
            if (baseTopic == null) {
                return null;
            }
            if (topicPattern != null && !topicPattern.isBlank() && topicPattern.contains("{topic}")) {
                return topicPattern.replace("{topic}", baseTopic);
            }
            String suffix = (topicSuffix != null && !topicSuffix.isBlank()) ? topicSuffix : ".DLT";
            return baseTopic + suffix;
        }
    }

    public static class SchedulingProperties {
        private boolean enabled = false;

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }
    }
}
