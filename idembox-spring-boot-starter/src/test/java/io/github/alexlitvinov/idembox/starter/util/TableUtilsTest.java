package io.github.alexlitvinov.idembox.starter.util;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TableUtilsTest {

    private static String nullString() {
        return null;
    }

    @Test
    @DisplayName("Should validate standard SQL identifiers and reject invalid characters")
    void shouldValidateIdentifiers() {
        assertThat(TableUtils.validateIdentifier("outbox_messages")).isEqualTo("outbox_messages");
        assertThat(TableUtils.validateIdentifier("tenant_1")).isEqualTo("tenant_1");
        assertThat(TableUtils.validateIdentifier("_private_tbl")).isEqualTo("_private_tbl");

        assertThatThrownBy(() -> TableUtils.validateIdentifier(nullString()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> TableUtils.validateIdentifier(""))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> TableUtils.validateIdentifier("   "))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> TableUtils.validateIdentifier("123table"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> TableUtils.validateIdentifier("table; DROP TABLE users;"))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> TableUtils.validateIdentifier("schema-name"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("Should validate qualified table names (table or schema.table)")
    void shouldValidateQualifiedTableNames() {
        assertThat(TableUtils.validateQualifiedTableName("outbox_messages")).isEqualTo("outbox_messages");
        assertThat(TableUtils.validateQualifiedTableName("custom_schema.outbox_messages")).isEqualTo("custom_schema.outbox_messages");
        assertThat(TableUtils.validateQualifiedTableName("tenant_1.app_idempotent_records")).isEqualTo("tenant_1.app_idempotent_records");

        assertThatThrownBy(() -> TableUtils.validateQualifiedTableName("a.b.c"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("At most one '.' allowed");
        assertThatThrownBy(() -> TableUtils.validateQualifiedTableName("invalid schema.table"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("Should resolve table name with optional schema and table prefix")
    void shouldResolveTableName() {
        assertThat(TableUtils.resolveTableName(null, null, "outbox_messages"))
                .isEqualTo("outbox_messages");
        assertThat(TableUtils.resolveTableName("", "", "outbox_messages"))
                .isEqualTo("outbox_messages");
        assertThat(TableUtils.resolveTableName("my_schema", null, "outbox_messages"))
                .isEqualTo("my_schema.outbox_messages");
        assertThat(TableUtils.resolveTableName(null, "tenant1_", "outbox_messages"))
                .isEqualTo("tenant1_outbox_messages");
        assertThat(TableUtils.resolveTableName("tenant_db", "prod_", "idempotent_records"))
                .isEqualTo("tenant_db.prod_idempotent_records");
    }

    @Test
    @DisplayName("Senior Refinements v45: Should quote identifiers and qualified tables including reserved keywords")
    void shouldQuoteIdentifiersAndQualifiedTableNames() {
        assertThat(TableUtils.quoteIdentifier("order")).isEqualTo("\"order\"");
        assertThat(TableUtils.quoteIdentifier("user")).isEqualTo("\"user\"");
        assertThat(TableUtils.quoteIdentifier("group")).isEqualTo("\"group\"");

        assertThat(TableUtils.quoteQualifiedTableName("order")).isEqualTo("\"order\"");
        assertThat(TableUtils.quoteQualifiedTableName("user.order")).isEqualTo("\"user\".\"order\"");
        assertThat(TableUtils.quoteQualifiedTableName("my_schema.outbox_messages")).isEqualTo("\"my_schema\".\"outbox_messages\"");
    }

    @Test
    @DisplayName("Senior Refinements v64: Should template DDL with schema and prefix, prepending CREATE SCHEMA IF NOT EXISTS")
    void shouldGenerateTemplatedDdl() {
        String originalDdl = """
                CREATE TABLE IF NOT EXISTS outbox_messages (
                    id UUID PRIMARY KEY,
                    topic VARCHAR(255) NOT NULL,
                    CONSTRAINT chk_outbox_status CHECK (status IN ('PENDING', 'SENT'))
                );
                CREATE INDEX IF NOT EXISTS idx_outbox_pending_fifo
                    ON outbox_messages (created_at ASC);
                CREATE TABLE IF NOT EXISTS idempotent_records (
                    event_key TEXT NOT NULL,
                    consumer_group VARCHAR(255) NOT NULL,
                    CONSTRAINT chk_idempotent_status CHECK (status IN ('PROCESSING', 'COMPLETED'))
                );
                CREATE INDEX IF NOT EXISTS idx_idempotent_expires_at
                    ON idempotent_records (expires_at);
                """;

        // 1. Both schema and prefix (default createSchema=false)
        String templated = TableUtils.generateTemplatedDdl(originalDdl, "tenant_1", "prod_");
        assertThat(templated).doesNotContain("CREATE SCHEMA");
        assertThat(templated).contains("CREATE TABLE IF NOT EXISTS \"tenant_1\".\"prod_outbox_messages\"");
        assertThat(templated).contains("ON \"tenant_1\".\"prod_outbox_messages\"");
        assertThat(templated).contains("CREATE INDEX IF NOT EXISTS prod_idx_outbox_pending_fifo");
        assertThat(templated).contains("CONSTRAINT prod_chk_outbox_status");
        assertThat(templated).contains("CREATE TABLE IF NOT EXISTS \"tenant_1\".\"prod_idempotent_records\"");
        assertThat(templated).contains("CREATE INDEX IF NOT EXISTS prod_idx_idempotent_expires_at");
        assertThat(templated).contains("CONSTRAINT prod_chk_idempotent_status");

        // 1b. Both schema and prefix with explicit createSchema=true
        String templatedWithCreate = TableUtils.generateTemplatedDdl(originalDdl, "tenant_1", "prod_", true);
        assertThat(templatedWithCreate).startsWith("CREATE SCHEMA IF NOT EXISTS \"tenant_1\";\n\n");
        assertThat(templatedWithCreate).contains("CREATE TABLE IF NOT EXISTS \"tenant_1\".\"prod_outbox_messages\"");

        // 2. Prefix only, no schema
        String prefixOnly = TableUtils.generateTemplatedDdl(originalDdl, null, "dev_");
        assertThat(prefixOnly).doesNotContain("CREATE SCHEMA");
        assertThat(prefixOnly).contains("CREATE TABLE IF NOT EXISTS \"dev_outbox_messages\"");
        assertThat(prefixOnly).contains("CREATE INDEX IF NOT EXISTS dev_idx_outbox_pending_fifo");

        // 3. Schema only, no prefix (default createSchema=false)
        String schemaOnly = TableUtils.generateTemplatedDdl(originalDdl, "my_schema", null);
        assertThat(schemaOnly).doesNotContain("CREATE SCHEMA");
        assertThat(schemaOnly).contains("CREATE TABLE IF NOT EXISTS \"my_schema\".\"outbox_messages\"");
        assertThat(schemaOnly).contains("CREATE INDEX IF NOT EXISTS idx_outbox_pending_fifo");

        // 3b. Schema only with explicit createSchema=true
        String schemaOnlyWithCreate = TableUtils.generateTemplatedDdl(originalDdl, "my_schema", null, true);
        assertThat(schemaOnlyWithCreate).startsWith("CREATE SCHEMA IF NOT EXISTS \"my_schema\";\n\n");
        assertThat(schemaOnlyWithCreate).contains("CREATE TABLE IF NOT EXISTS \"my_schema\".\"outbox_messages\"");

        // 4. Neither schema nor prefix
        String unchanged = TableUtils.generateTemplatedDdl(originalDdl, null, null);
        assertThat(unchanged).doesNotContain("CREATE SCHEMA");
        assertThat(unchanged).contains("CREATE TABLE IF NOT EXISTS \"outbox_messages\"");
        assertThat(unchanged).contains("CREATE INDEX IF NOT EXISTS idx_outbox_pending_fifo");

        // 5. Blank/null sql
        assertThat(TableUtils.generateTemplatedDdl(null, "s", "p")).isNull();
        assertThat(TableUtils.generateTemplatedDdl("   ", "s", "p")).isEqualTo("   ");
    }
}
