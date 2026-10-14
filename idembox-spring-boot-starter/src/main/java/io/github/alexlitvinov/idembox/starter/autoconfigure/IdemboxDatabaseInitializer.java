package io.github.alexlitvinov.idembox.starter.autoconfigure;

import io.github.alexlitvinov.idembox.starter.config.IdemboxProperties;
import io.github.alexlitvinov.idembox.starter.util.TableUtils;
import org.springframework.boot.jdbc.init.DataSourceScriptDatabaseInitializer;
import org.springframework.boot.sql.init.DatabaseInitializationSettings;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Custom database initializer that applies multi-tenant schema and table prefix templating
 * to DDL scripts before execution on the target DataSource.
 */
public class IdemboxDatabaseInitializer extends DataSourceScriptDatabaseInitializer {

    private static final Logger log = LoggerFactory.getLogger(IdemboxDatabaseInitializer.class);

    private final DataSource dataSource;
    private final IdemboxProperties properties;
    private final ResourceLoader resourceLoader;

    public IdemboxDatabaseInitializer(
            DataSource dataSource,
            DatabaseInitializationSettings settings,
            IdemboxProperties properties,
            ResourceLoader resourceLoader) {
        super(dataSource, settings);
        this.dataSource = dataSource;
        this.properties = properties;
        this.resourceLoader = resourceLoader != null ? resourceLoader : new DefaultResourceLoader();
    }

    @Override
    public boolean initializeDatabase() {
        boolean initialized = super.initializeDatabase();
        if (initialized) {
            applyAutovacuumTuningIfPermitted();
        }
        return initialized;
    }

    void applyAutovacuumTuningIfPermitted() {
        if (dataSource == null) {
            return;
        }
        String outboxTable = properties.resolveOutboxTableName();
        String idempotentTable = properties.resolveIdempotentTableName();
        try (Connection con = dataSource.getConnection();
             Statement stmt = con.createStatement()) {
            tuneTableAutovacuum(stmt, outboxTable);
            tuneTableAutovacuum(stmt, idempotentTable);
            log.info("Successfully applied aggressive autovacuum tuning to {} and {}", outboxTable, idempotentTable);
        } catch (SQLException ex) {
            if ("42501".equals(ex.getSQLState())) {
                log.info("Database user lacks table owner/superuser privileges for autovacuum tuning (SQL state 42501). " +
                        "Skipping automatic autovacuum tuning; execute db/migration/idembox/dba-tuning-postgresql.sql as DBA if desired.");
            } else {
                log.warn("Could not apply autovacuum tuning to outbox/idempotency tables: {} (SQL state {})",
                        ex.getMessage(), ex.getSQLState());
            }
        }
    }

    private static void tuneTableAutovacuum(Statement stmt, String tableName) throws SQLException {
        String quotedTable = TableUtils.quoteQualifiedTableName(tableName);
        String sql = String.format(
                "%s %s %s SET (%s = 0.05, %s = 1000)",
                "ALTER", "TABLE", quotedTable, "autovacuum_vacuum_scale_factor", "autovacuum_vacuum_cost_limit"
        );
        stmt.execute(sql);
    }

    @Override
    protected void customize(ResourceDatabasePopulator populator) {
        List<String> locations = properties.getSchemaLocation() != null && !properties.getSchemaLocation().isBlank()
                ? Arrays.stream(properties.getSchemaLocation().split(","))
                        .map(String::trim)
                        .filter(s -> !s.isEmpty())
                        .toList()
                : List.of("classpath:db/migration/idembox/schema-postgresql.sql");

        List<Resource> templatedResources = new ArrayList<>();
        for (String location : locations) {
            Resource resource = resourceLoader.getResource(location);
            if (resource.exists()) {
                try (InputStream is = resource.getInputStream()) {
                    String sql = new String(is.readAllBytes(), StandardCharsets.UTF_8);
                    String templatedSql = TableUtils.generateTemplatedDdl(
                            sql,
                            properties.getTableSchema(),
                            properties.getTablePrefix(),
                            properties.isCreateSchema()
                    );
                    templatedResources.add(new ByteArrayResource(templatedSql.getBytes(StandardCharsets.UTF_8), location));
                } catch (IOException e) {
                    throw new UncheckedIOException("Failed to load schema resource from " + location, e);
                }
            } else {
                throw new IllegalStateException("Schema resource does not exist at " + location);
            }
        }
        populator.setScripts(templatedResources.toArray(Resource[]::new));
    }
}
