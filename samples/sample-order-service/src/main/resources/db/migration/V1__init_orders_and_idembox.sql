-- ==========================================================
-- Sample Order Service: Domain Table & Idembox Schema
-- ==========================================================

-- 0. Business Domain Table: Orders
CREATE TABLE IF NOT EXISTS orders (
    id VARCHAR(64) PRIMARY KEY,
    customer_id VARCHAR(64) NOT NULL,
    amount NUMERIC(12, 2) NOT NULL,
    status VARCHAR(32) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_order_amount CHECK (amount > 0)
);

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

CREATE INDEX IF NOT EXISTS idx_outbox_pending_fifo
    ON outbox_messages (created_at ASC, id ASC, next_retry_at ASC)
    WHERE status IN ('PENDING', 'IN_PROGRESS');

CREATE INDEX IF NOT EXISTS idx_outbox_partition_key
    ON outbox_messages (topic, partition_key, created_at ASC, id ASC)
    WHERE status IN ('PENDING', 'IN_PROGRESS');

CREATE INDEX IF NOT EXISTS idx_outbox_sent_cleanup
    ON outbox_messages ((COALESCE(sent_at, created_at)), id)
    WHERE status = 'SENT';

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

CREATE INDEX IF NOT EXISTS idx_idempotent_expires_at
    ON idempotent_records (expires_at, consumer_group, event_key);

-- 3. High-Throughput Autovacuum Tuning (Queue Bloat & HOT Degradation Prevention)
ALTER TABLE outbox_messages SET (
    autovacuum_vacuum_scale_factor = 0.05,
    autovacuum_vacuum_cost_limit = 1000
);

ALTER TABLE idempotent_records SET (
    autovacuum_vacuum_scale_factor = 0.05,
    autovacuum_vacuum_cost_limit = 1000
);
