-- =====================================================================================
-- Idembox - PostgreSQL DBA Tuning Script
-- =====================================================================================
-- This script configures aggressive autovacuum thresholds for Outbox and Idempotency tables
-- to prevent queue table bloat and Heap-Only Tuple (HOT) degradation under high throughput.
--
-- NOTE: In PostgreSQL, setting storage parameters (ALTER TABLE ... SET ...) requires
-- table owner or superuser privileges (SQL state 42501).
-- Run this script as DBA/table owner after schema initialization.
-- =====================================================================================

ALTER TABLE outbox_messages SET (
    autovacuum_vacuum_scale_factor = 0.05,
    autovacuum_vacuum_cost_limit = 1000
);

ALTER TABLE idempotent_records SET (
    autovacuum_vacuum_scale_factor = 0.05,
    autovacuum_vacuum_cost_limit = 1000
);
