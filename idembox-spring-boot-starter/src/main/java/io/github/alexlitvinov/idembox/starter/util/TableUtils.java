package io.github.alexlitvinov.idembox.starter.util;

import java.util.regex.Pattern;

/**
 * Utility for resolving and validating multi-tenant schema and table identifiers,
 * protecting against SQL injection vulnerabilities in dynamic table qualification.
 */
public final class TableUtils {

    private static final Pattern IDENTIFIER_PATTERN = Pattern.compile("^[a-zA-Z_][a-zA-Z0-9_]*$");

    private TableUtils() {
    }

    /**
     * Validates that the given identifier is a valid SQL identifier (alphanumeric and underscore, starting with letter or underscore).
     */
    public static String validateIdentifier(String identifier) {
        if (identifier == null || identifier.isBlank()) {
            throw new IllegalArgumentException("SQL identifier cannot be null or blank");
        }
        if (!IDENTIFIER_PATTERN.matcher(identifier).matches()) {
            throw new IllegalArgumentException("Invalid SQL identifier: '" + identifier + "'. Must match ^[a-zA-Z_][a-zA-Z0-9_]*$");
        }
        return identifier;
    }

    /**
     * Validates that a qualified table name consists of either a single valid identifier,
     * or exactly two valid identifiers separated by a single dot (schema.table).
     */
    public static String validateQualifiedTableName(String tableName) {
        if (tableName == null || tableName.isBlank()) {
            throw new IllegalArgumentException("Table name cannot be null or blank");
        }
        String[] parts = tableName.split("\\.", -1);
        if (parts.length > 2) {
            throw new IllegalArgumentException("Invalid table name format: '" + tableName + "'. At most one '.' allowed (schema.table)");
        }
        for (String part : parts) {
            validateIdentifier(part);
        }
        return tableName;
    }

    /**
     * Resolves and validates a table name given an optional schema, optional table prefix, and default table name.
     */
    public static String resolveTableName(String schema, String prefix, String defaultTable) {
        validateIdentifier(defaultTable);
        String cleanPrefix = (prefix != null && !prefix.isBlank()) ? prefix.trim() : "";
        if (!cleanPrefix.isEmpty()) {
            validateIdentifier(cleanPrefix);
        }
        String effectiveTable = cleanPrefix + defaultTable;
        if (schema != null && !schema.isBlank()) {
            String cleanSchema = schema.trim();
            validateIdentifier(cleanSchema);
            return cleanSchema + "." + effectiveTable;
        }
        return effectiveTable;
    }

    /**
     * Quotes a single SQL identifier with double quotes (e.g. "order").
     */
    public static String quoteIdentifier(String identifier) {
        validateIdentifier(identifier);
        return "\"" + identifier + "\"";
    }

    /**
     * Quotes a qualified table name with double quotes (e.g. "schema"."table" or "table").
     */
    public static String quoteQualifiedTableName(String tableName) {
        validateQualifiedTableName(tableName);
        String[] parts = tableName.split("\\.", -1);
        if (parts.length == 2) {
            return "\"" + parts[0] + "\".\"" + parts[1] + "\"";
        }
        return "\"" + parts[0] + "\"";
    }

    /**
     * Applies schema and table prefix templating to SQL DDL statements.
     * Qualifies tables, prefixes indexes and constraints, and prepends schema creation if configured.
     *
     * @param sql Original SQL DDL script
     * @param schema Optional target database schema
     * @param prefix Optional table/index name prefix
     * @return Templated SQL DDL script
     */
    public static String generateTemplatedDdl(String sql, String schema, String prefix) {
        return generateTemplatedDdl(sql, schema, prefix, false);
    }

    /**
     * Replaces default table and index names in the SQL DDL with custom schema and prefix.
     *
     * @param sql Original DDL script
     * @param schema Optional target database schema
     * @param prefix Optional table/index name prefix
     * @param createSchema Whether to emit CREATE SCHEMA IF NOT EXISTS when schema is configured
     * @return Templated SQL DDL script
     */
    public static String generateTemplatedDdl(String sql, String schema, String prefix, boolean createSchema) {
        if (sql == null || sql.isBlank()) {
            return sql;
        }

        String cleanSchema = (schema != null && !schema.isBlank()) ? validateIdentifier(schema.trim()) : null;
        String cleanPrefix = (prefix != null && !prefix.isBlank()) ? validateIdentifier(prefix.trim()) : "";

        String effectiveOutboxTable = quoteQualifiedTableName(resolveTableName(cleanSchema, cleanPrefix, "outbox_messages"));
        String effectiveIdempotentTable = quoteQualifiedTableName(resolveTableName(cleanSchema, cleanPrefix, "idempotent_records"));

        String templatedSql = sql.replaceAll("\\boutbox_messages\\b", java.util.regex.Matcher.quoteReplacement(effectiveOutboxTable));
        templatedSql = templatedSql.replaceAll("\\bidempotent_records\\b", java.util.regex.Matcher.quoteReplacement(effectiveIdempotentTable));

        if (!cleanPrefix.isEmpty()) {
            String[] identifiersToPrefix = {
                    "idx_outbox_pending_fifo",
                    "idx_outbox_partition_key",
                    "idx_outbox_sent_cleanup",
                    "idx_outbox_failed_cleanup",
                    "idx_idempotent_expires_at",
                    "chk_outbox_status",
                    "chk_idempotent_status"
            };
            for (String id : identifiersToPrefix) {
                templatedSql = templatedSql.replaceAll("\\b" + id + "\\b", java.util.regex.Matcher.quoteReplacement(cleanPrefix + id));
            }
        }

        if (cleanSchema != null && createSchema) {
            templatedSql = "CREATE SCHEMA IF NOT EXISTS " + quoteIdentifier(cleanSchema) + ";\n\n" + templatedSql;
        }

        return templatedSql;
    }
}
