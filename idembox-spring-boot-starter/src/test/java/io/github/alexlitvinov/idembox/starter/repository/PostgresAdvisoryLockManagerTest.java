package io.github.alexlitvinov.idembox.starter.repository;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataRetrievalFailureException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PostgresAdvisoryLockManagerTest {

    private static ConnectionCallback<Boolean> anyConnectionCallback() {
        return any();
    }

    @Mock
    private JdbcTemplate jdbcTemplate;

    @Test
    @DisplayName("computeEffectiveLockName correctly prefixes non-blank table names")
    void shouldComputeEffectiveLockNameCorrectly() {
        assertThat(PostgresAdvisoryLockManager.computeEffectiveLockName("outbox_messages", "cleanup"))
                .isEqualTo("outbox_messages:cleanup");
        assertThat(PostgresAdvisoryLockManager.computeEffectiveLockName("  tenant_a.outbox  ", "cleanup"))
                .isEqualTo("tenant_a.outbox:cleanup");
        assertThat(PostgresAdvisoryLockManager.computeEffectiveLockName("", "cleanup"))
                .isEqualTo("cleanup");
        assertThat(PostgresAdvisoryLockManager.computeEffectiveLockName("   ", "cleanup"))
                .isEqualTo("cleanup");
        assertThat(PostgresAdvisoryLockManager.computeEffectiveLockName(null, "cleanup"))
                .isEqualTo("cleanup");
    }

    @Test
    @DisplayName("executeWithLock executes directly when jdbcTemplate is null")
    void shouldExecuteDirectlyWhenJdbcTemplateIsNull() {
        boolean[] ran = new boolean[1];
        boolean result = PostgresAdvisoryLockManager.executeWithLock(null, "table", "lock", () -> ran[0] = true);

        assertThat(result).isTrue();
        assertThat(ran[0]).isTrue();
    }

    @Test
    @DisplayName("executeWithLock acquires transaction advisory lock, runs action, and commits transaction")
    void shouldAcquireTransactionAdvisoryLockExecuteActionAndCommit() throws SQLException {
        Connection con = mock(Connection.class);
        PreparedStatement psLock = mock(PreparedStatement.class);
        ResultSet rs = mock(ResultSet.class);

        when(con.prepareStatement(eq(PostgresAdvisoryLockManager.buildTryLockSql()))).thenReturn(psLock);
        when(psLock.executeQuery()).thenReturn(rs);
        when(rs.next()).thenReturn(true);
        when(rs.getBoolean(1)).thenReturn(true);

        when(jdbcTemplate.execute(anyConnectionCallback())).thenAnswer(inv -> {
            ConnectionCallback<Boolean> callback = inv.getArgument(0);
            return callback.doInConnection(con);
        });

        boolean[] ran = new boolean[1];
        boolean result = PostgresAdvisoryLockManager.executeWithLock(
                jdbcTemplate,
                "outbox_messages",
                "cleanup_job",
                () -> ran[0] = true
        );

        assertThat(result).isTrue();
        assertThat(ran[0]).isTrue();

        verify(psLock).setString(1, "outbox_messages:cleanup_job");
        verify(con).commit();
        verify(con, never()).close();
    }

    @Test
    @DisplayName("executeWithLock returns false and skips action when lock cannot be acquired")
    void shouldReturnFalseWhenLockCannotBeAcquired() throws SQLException {
        Connection con = mock(Connection.class);
        PreparedStatement psLock = mock(PreparedStatement.class);
        ResultSet rs = mock(ResultSet.class);

        when(con.prepareStatement(eq(PostgresAdvisoryLockManager.buildTryLockSql()))).thenReturn(psLock);
        when(psLock.executeQuery()).thenReturn(rs);
        when(rs.next()).thenReturn(true);
        when(rs.getBoolean(1)).thenReturn(false);

        when(jdbcTemplate.execute(anyConnectionCallback())).thenAnswer(inv -> {
            ConnectionCallback<Boolean> callback = inv.getArgument(0);
            return callback.doInConnection(con);
        });

        boolean[] ran = new boolean[1];
        boolean result = PostgresAdvisoryLockManager.executeWithLock(
                jdbcTemplate,
                "outbox_messages",
                "cleanup_job",
                () -> ran[0] = true
        );

        assertThat(result).isFalse();
        assertThat(ran[0]).isFalse();
        verify(con, never()).commit();
    }

    @Test
    @DisplayName("executeWithLock rolls back transaction on action exception and delegates connection lifecycle to Spring")
    void shouldRollbackTransactionOnActionException() throws SQLException {
        Connection con = mock(Connection.class);
        PreparedStatement psLock = mock(PreparedStatement.class);
        ResultSet rs = mock(ResultSet.class);

        when(con.prepareStatement(eq(PostgresAdvisoryLockManager.buildTryLockSql()))).thenReturn(psLock);
        when(psLock.executeQuery()).thenReturn(rs);
        when(rs.next()).thenReturn(true);
        when(rs.getBoolean(1)).thenReturn(true);

        when(jdbcTemplate.execute(anyConnectionCallback())).thenAnswer(inv -> {
            ConnectionCallback<Boolean> callback = inv.getArgument(0);
            return callback.doInConnection(con);
        });

        assertThatThrownBy(() ->
                PostgresAdvisoryLockManager.executeWithLock(
                        jdbcTemplate,
                        "outbox_messages",
                        "cleanup_job",
                        () -> {
                            throw new IllegalStateException("Business logic failure during cleanup");
                        }
                )
        ).isInstanceOf(IllegalStateException.class).hasMessage("Business logic failure during cleanup");

        verify(con).rollback();
        verify(con, never()).close();
    }

    @Test
    @DisplayName("executeWithLock falls back to direct execution on DataAccessException")
    void shouldFallbackToDirectExecutionOnDataAccessException() {
        when(jdbcTemplate.execute(anyConnectionCallback()))
                .thenThrow(new DataRetrievalFailureException("Database function pg_try_advisory_xact_lock does not exist"));

        boolean[] ran = new boolean[1];
        boolean result = PostgresAdvisoryLockManager.executeWithLock(
                jdbcTemplate,
                "outbox_messages",
                "cleanup_job",
                () -> ran[0] = true
        );

        assertThat(result).isTrue();
        assertThat(ran[0]).isTrue();
    }

    @Test
    @DisplayName("executeWithLock exposes active connection via getActiveLockConnection during action execution")
    void shouldExposeActiveLockConnectionDuringActionExecution() throws SQLException {
        Connection con = mock(Connection.class);
        PreparedStatement psLock = mock(PreparedStatement.class);
        ResultSet rs = mock(ResultSet.class);

        when(con.prepareStatement(eq(PostgresAdvisoryLockManager.buildTryLockSql()))).thenReturn(psLock);
        when(psLock.executeQuery()).thenReturn(rs);
        when(rs.next()).thenReturn(true);
        when(rs.getBoolean(1)).thenReturn(true);

        when(jdbcTemplate.execute(anyConnectionCallback())).thenAnswer(inv -> {
            ConnectionCallback<Boolean> callback = inv.getArgument(0);
            return callback.doInConnection(con);
        });

        Connection[] capturedCon = new Connection[1];
        assertThat(PostgresAdvisoryLockManager.getActiveLockConnection()).isNull();

        boolean result = PostgresAdvisoryLockManager.executeWithLock(
                jdbcTemplate,
                "outbox_messages",
                "cleanup_job",
                () -> capturedCon[0] = PostgresAdvisoryLockManager.getActiveLockConnection()
        );

        assertThat(result).isTrue();
        assertThat(capturedCon[0]).isSameAs(con);
        assertThat(PostgresAdvisoryLockManager.getActiveLockConnection()).isNull();
        verify(con, never()).close();
    }

    @Test
    @DisplayName("Issue 2: executeWithLock cleans up ThreadLocal and restores previous connection on nested execution")
    void shouldCleanUpThreadLocalAndRestorePreviousConnectionOnNestedExecution() throws SQLException {
        Connection con1 = mock(Connection.class);
        Connection con2 = mock(Connection.class);
        PreparedStatement psLock = mock(PreparedStatement.class);
        ResultSet rs = mock(ResultSet.class);

        when(con1.prepareStatement(eq(PostgresAdvisoryLockManager.buildTryLockSql()))).thenReturn(psLock);
        when(psLock.executeQuery()).thenReturn(rs);
        when(rs.next()).thenReturn(true);
        when(rs.getBoolean(1)).thenReturn(true);

        when(jdbcTemplate.execute(anyConnectionCallback())).thenAnswer(inv -> {
            ConnectionCallback<Boolean> callback = inv.getArgument(0);
            return callback.doInConnection(con1);
        });

        PostgresAdvisoryLockManager.setActiveLockConnectionForTest(con2);
        try {
            Connection[] capturedInside = new Connection[1];
            boolean result = PostgresAdvisoryLockManager.executeWithLock(
                    jdbcTemplate,
                    "outbox_messages",
                    "cleanup_job",
                    () -> capturedInside[0] = PostgresAdvisoryLockManager.getActiveLockConnection()
            );
            assertThat(result).isTrue();
            assertThat(capturedInside[0]).isSameAs(con1);
            assertThat(PostgresAdvisoryLockManager.getActiveLockConnection()).isSameAs(con2);
        } finally {
            PostgresAdvisoryLockManager.setActiveLockConnectionForTest(null);
        }
        assertThat(PostgresAdvisoryLockManager.getActiveLockConnection()).isNull();
    }
}
