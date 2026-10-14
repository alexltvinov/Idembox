-- ==========================================================
-- Idembox Starter: Schema Initializer (PostgreSQL)
-- ==========================================================

-- 1. Transactional Outbox Table
CREATE TABLE IF NOT EXISTS outbox_messages (
    id UUID PRIMARY KEY,
    topic VARCHAR(255) NOT NULL,
    partition_key TEXT,
    payload JSONB NULL,
    headers JSONB DEFAULT '{}'::jsonb,
    status VARCHAR(32) NOT NULL,
    retry_count INT NOT NULL DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    sent_at TIMESTAMP WITH TIME ZONE,
    failed_at TIMESTAMP WITH TIME ZONE,
    next_retry_at TIMESTAMP WITH TIME ZONE,
    last_error TEXT,
    CONSTRAINT chk_outbox_status CHECK (status IN ('PENDING', 'IN_PROGRESS', 'SENT', 'FAILED'))
);

-- Index to quickly fetch pending/in-progress messages in FIFO order with deterministic tie-breaking
CREATE INDEX IF NOT EXISTS idx_outbox_pending_fifo
    ON outbox_messages (created_at ASC, id ASC, next_retry_at ASC)
    WHERE status IN ('PENDING', 'IN_PROGRESS');

-- Index for partition-key ordering lookups with deterministic tie-breaking (scoped per topic)
CREATE INDEX IF NOT EXISTS idx_outbox_partition_key
    ON outbox_messages (topic, partition_key, created_at ASC, id ASC)
    WHERE status IN ('PENDING', 'IN_PROGRESS');

-- Covering index for periodic housekeeping of sent messages (Index-Only Scan)
CREATE INDEX IF NOT EXISTS idx_outbox_sent_cleanup
    ON outbox_messages ((COALESCE(sent_at, created_at)), id)
    WHERE status = 'SENT';

-- Covering index for periodic housekeeping of failed messages (Index-Only Scan)
CREATE INDEX IF NOT EXISTS idx_outbox_failed_cleanup
    ON outbox_messages ((COALESCE(failed_at, created_at)), id)
    WHERE status = 'FAILED';

-- 2. Idempotent Consumer Records Table
CREATE TABLE IF NOT EXISTS idempotent_records (
    event_key TEXT NOT NULL,
    consumer_group VARCHAR(255) NOT NULL,
    status VARCHAR(32) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP WITH TIME ZONE,
    locked_until TIMESTAMP WITH TIME ZONE,
    expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
    PRIMARY KEY (consumer_group, event_key),
    CONSTRAINT chk_idempotent_status CHECK (status IN ('PROCESSING', 'COMPLETED', 'FAILED'))
);

-- Covering index for periodic cleanup of expired idempotency keys (Index-Only Scan)
CREATE INDEX IF NOT EXISTS idx_idempotent_expires_at
    ON idempotent_records (expires_at, consumer_group, event_key);

-- 3. High-Throughput Autovacuum Tuning (Queue Bloat & HOT Degradation Prevention)
-- NOTE: Setting table storage parameters (ALTER TABLE ... SET ...) requires table owner or superuser
-- privileges in PostgreSQL (SQL state 42501). To prevent startup migration failures when running under
-- unprivileged application users (e.g. app_user), apply db/migration/idembox/dba-tuning-postgresql.sql
-- as DBA or table owner:
--
-- ALTER TABLE outbox_messages SET (
--     autovacuum_vacuum_scale_factor = 0.05,
--     autovacuum_vacuum_cost_limit = 1000
-- );
--
-- ALTER TABLE idempotent_records SET (
--     autovacuum_vacuum_scale_factor = 0.05,
--     autovacuum_vacuum_cost_limit = 1000
-- );
