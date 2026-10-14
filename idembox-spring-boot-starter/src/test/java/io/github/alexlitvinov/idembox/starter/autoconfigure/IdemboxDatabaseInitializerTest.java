package io.github.alexlitvinov.idembox.starter.autoconfigure;

import io.github.alexlitvinov.idembox.starter.config.IdemboxProperties;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.sql.init.DatabaseInitializationSettings;
import org.springframework.boot.sql.init.DatabaseInitializationMode;
import org.springframework.core.io.DefaultResourceLoader;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.*;

class IdemboxDatabaseInitializerTest {

    @Test
    @DisplayName("Issue 6: should execute autovacuum tuning when user has table owner privileges")
    void shouldExecuteAutovacuumTuningWhenPermitted() throws Exception {
        DataSource dataSource = mock(DataSource.class);
        Connection connection = mock(Connection.class);
        Statement statement = mock(Statement.class);

        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.createStatement()).thenReturn(statement);

        DatabaseInitializationSettings settings = new DatabaseInitializationSettings();
        settings.setMode(DatabaseInitializationMode.ALWAYS);
        settings.setSchemaLocations(List.of());

        IdemboxProperties properties = new IdemboxProperties();
        IdemboxDatabaseInitializer initializer =
                new IdemboxDatabaseInitializer(dataSource, settings, properties, new DefaultResourceLoader());

        assertThatCode(initializer::applyAutovacuumTuningIfPermitted).doesNotThrowAnyException();

        verify(statement, atLeastOnce()).execute(contains("autovacuum_vacuum_scale_factor = 0.05"));
    }

    @Test
    @DisplayName("Issue 6: should gracefully ignore SQLState 42501 (insufficient_privilege) without throwing exception")
    void shouldGracefullyHandleInsufficientPrivilegeForAutovacuum() throws Exception {
        DataSource dataSource = mock(DataSource.class);
        Connection connection = mock(Connection.class);
        Statement statement = mock(Statement.class);

        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.createStatement()).thenReturn(statement);

        SQLException privilegeException = new SQLException("must be table owner to set table storage parameters", "42501");
        doThrow(privilegeException).when(statement).execute(contains("autovacuum_vacuum_scale_factor"));

        DatabaseInitializationSettings settings = new DatabaseInitializationSettings();
        settings.setMode(DatabaseInitializationMode.ALWAYS);
        settings.setSchemaLocations(List.of());

        IdemboxProperties properties = new IdemboxProperties();
        IdemboxDatabaseInitializer initializer =
                new IdemboxDatabaseInitializer(dataSource, settings, properties, new DefaultResourceLoader());

        assertThatCode(initializer::applyAutovacuumTuningIfPermitted).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("Issue 3: should parse comma-separated schema locations and load multiple scripts")
    void shouldParseCommaSeparatedSchemaLocations() {
        DataSource dataSource = mock(DataSource.class);
        DatabaseInitializationSettings settings = new DatabaseInitializationSettings();
        settings.setMode(DatabaseInitializationMode.ALWAYS);
        settings.setSchemaLocations(List.of());

        IdemboxProperties properties = new IdemboxProperties();
        properties.setSchemaLocation("classpath:db/migration/idembox/schema-postgresql.sql , classpath:db/migration/idembox/schema-postgresql.sql ");

        IdemboxDatabaseInitializer initializer =
                new IdemboxDatabaseInitializer(dataSource, settings, properties, new DefaultResourceLoader());

        org.springframework.jdbc.datasource.init.ResourceDatabasePopulator populator =
                new org.springframework.jdbc.datasource.init.ResourceDatabasePopulator();

        assertThatCode(() -> initializer.customize(populator)).doesNotThrowAnyException();

        Object scriptsField = org.springframework.test.util.ReflectionTestUtils.getField(populator, "scripts");
        assertThat(scriptsField).isInstanceOf(List.class);
        List<?> scripts = (List<?>) scriptsField;
        assertThat(scripts).hasSize(2);
    }
}
