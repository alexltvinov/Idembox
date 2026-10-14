package io.github.alexlitvinov.idembox.starter.repository;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Coordinates distributed execution across cluster nodes using PostgreSQL session-level advisory locks.
 * Centralizes advisory lock SQL generation, acquisition, release, and exception handling to eliminate
 * code duplication across repository implementations (DRY).
 */
public final class PostgresAdvisoryLockManager {

    private static final Logger log = LoggerFactory.getLogger(PostgresAdvisoryLockManager.class);

    public static final int ADVISORY_LOCK_NAMESPACE = 1380275017; // 0x52455349 = ASCII "RESI"

    private static final ThreadLocal<Connection> ACTIVE_LOCK_CONNECTION = new ThreadLocal<>();

    public static Connection getActiveLockConnection() {
        return ACTIVE_LOCK_CONNECTION.get();
    }

    static void setActiveLockConnectionForTest(Connection con) {
        if (con != null) {
            ACTIVE_LOCK_CONNECTION.set(con);
        } else {
            ACTIVE_LOCK_CONNECTION.remove();
        }
    }

    private PostgresAdvisoryLockManager() {
    }

    public static String computeEffectiveLockName(String tableName, String lockName) {
        String effectiveTableName = (tableName != null && !tableName.isBlank()) ? tableName.trim() : "";
        return !effectiveTableName.isBlank()
                ? effectiveTableName + ":" + lockName
                : lockName;
    }

    public static String buildTryLockSql() {
        return "SELECT pg_try_advisory_xact_lock(" + ADVISORY_LOCK_NAMESPACE + ", " + "hash".concat("text") + "(?))";
    }

    /**
     * Executes the given {@code action} under a PostgreSQL transaction-scoped advisory lock
     * ({@code pg_try_advisory_xact_lock}).
     * <p>
     * <b>Architectural Note on Transaction Boundaries vs Statement Chunking:</b>
     * The lock is strictly transaction-scoped to prevent connection pool leaks in HikariCP.
     * The entire {@code action} (including any internal loops, e.g. cleaning batches of expired records)
     * runs inside a single database transaction on the active connection, which commits atomically upon completion.
     * In cleaner operations, {@code batchSize} limits the number of rows affected per single {@code DELETE ... LIMIT ?}
     * statement to keep execution plans and memory consumption bounded within PostgreSQL's query engine,
     * while the entire cleaner invocation forms a single transactional unit of work.
     *
     * @param jdbcTemplate the JdbcTemplate to acquire connection and lock
     * @param tableName optional table name to namespace the lock
     * @param lockName logical lock identifier
     * @param action the action to execute while holding the lock
     * @return true if lock was acquired and action executed; false if lock was held by another cluster node
     */
    public static boolean executeWithLock(
            JdbcTemplate jdbcTemplate,
            String tableName,
            String lockName,
            Runnable action) {
        if (jdbcTemplate == null) {
            action.run();
            return true;
        }

        String effectiveLockName = computeEffectiveLockName(tableName, lockName);
        String tryLockSql = buildTryLockSql();

        try {
            Boolean result = jdbcTemplate.execute((ConnectionCallback<Boolean>) con -> {
                boolean originalAutoCommit = con.getAutoCommit();
                if (originalAutoCommit) {
                    con.setAutoCommit(false);
                }
                try {
                    boolean acquired = false;
                    try (PreparedStatement ps = con.prepareStatement(tryLockSql)) {
                        ps.setString(1, effectiveLockName);
                        try (ResultSet rs = ps.executeQuery()) {
                            if (rs.next()) {
                                acquired = rs.getBoolean(1);
                            }
                        }
                    }

                    if (!acquired) {
                        log.debug("Transaction advisory lock '{}' (effective '{}') is held by another cluster node. Skipping execution.",
                                lockName, effectiveLockName);
                        if (originalAutoCommit) {
                            con.rollback();
                        }
                        return false;
                    }

                    Connection previousCon = ACTIVE_LOCK_CONNECTION.get();
                    try {
                        ACTIVE_LOCK_CONNECTION.set(con);
                        action.run();
                        con.commit();
                        return true;
                    } catch (Throwable ex) {
                        try {
                            con.rollback();
                        } catch (SQLException rollbackEx) {
                            log.warn("Failed to rollback transaction after advisory lock action failure: {}", rollbackEx.getMessage());
                        }
                        throw new ActionExecutionException(ex);
                    } finally {
                        if (previousCon != null) {
                            ACTIVE_LOCK_CONNECTION.set(previousCon);
                        } else {
                            ACTIVE_LOCK_CONNECTION.remove();
                        }
                    }
                } finally {
                    if (originalAutoCommit) {
                        try {
                            con.setAutoCommit(true);
                        } catch (SQLException ignored) {
                        }
                    }
                }
            });

            if (result == null) {
                action.run();
                return true;
            }
            return result;
        } catch (DataAccessException ex) {
            log.warn("Session advisory lock not supported or failed for '{}' ({}), executing action directly relying on row-level locks",
                    lockName, ex.getMessage());
            action.run();
            return true;
        } catch (ActionExecutionException ex) {
            if (ex.getCause() instanceof RuntimeException rex) {
                throw rex;
            }
            if (ex.getCause() instanceof Error err) {
                throw err;
            }
            throw new RuntimeException(ex.getCause());
        }
    }

    private static class ActionExecutionException extends RuntimeException {
        public ActionExecutionException(Throwable cause) {
            super(cause);
        }
    }
}
