package com.NLP2SparkSQL.project.utils;

import java.util.*;
import java.util.regex.*;
import lombok.extern.slf4j.Slf4j;
import java.util.stream.Collectors;

@Slf4j
public class SparkSchemaParser {

    public static Map<String, List<Column>> parseSparkSchema(final String rawSchema) {
        if (rawSchema == null || rawSchema.trim().isEmpty()) {
            log.warn("Empty or null schema provided");
            return new LinkedHashMap<>();
        }

        // Clean the schema (remove control characters, normalize line breaks)
        String schema = cleanSchema(rawSchema);

        Map<String, List<Column>> tables = tryDynamicSparkSchemaParsing(schema);
        if (tables.isEmpty()) {
            tables = tryStructBasedParsing(schema);
        }
        if (tables.isEmpty()) {
            tables = tryDataFrameBasedParsing(schema);
        }
        if (tables.isEmpty()) {
            tables = tryGenericParsing(schema);
        }

        log.info("Successfully parsed {} tables from schema", tables.size());
        logParsedTables(tables);

        return tables;
    }

    /**
     * Cleans the schema: removes invisible characters except tabs and line breaks,
     * normalizes line breaks.
     */
    private static String cleanSchema(final String schema) {
        return schema.replaceAll("[\\u0000-\\u001F&&[^\\n\\t]]", "")
                     .replaceAll("\\r\\n", "\n")
                     .replaceAll("\\r", "\n")
                     .trim();
    }

    /**
     * Main strategy: dynamic parsing of Spark format with variable indentation.
     */
   private static Map<String, List<Column>> tryDynamicSparkSchemaParsing(final String schema) {
    Map<String, List<Column>> tables = new LinkedHashMap<>();

    // Pattern for table (root)
    Pattern tablePattern = Pattern.compile("\\|--\\s+([a-zA-Z_][\\w_]*)\\s*:\\s*struct\\s*\\(nullable\\s*=\\s*\\w+\\)", Pattern.CASE_INSENSITIVE);

    // Pattern for columns, including those in nested structs
    Pattern columnPattern = Pattern.compile("\\|(?:\\s*\\|)*--\\s+([a-zA-Z_][\\w_]*)\\s*:\\s*([a-zA-Z_][\\w_<>()\\[\\],\\s]*)\\s*\\(nullable\\s*=\\s*\\w+\\)", Pattern.CASE_INSENSITIVE);

    String[] lines = schema.split("\\n");
    String currentTable = null;

    for (String line : lines) {
        if (line.trim().isEmpty()) continue;

        Matcher tableMatcher = tablePattern.matcher(line);
        if (tableMatcher.find()) {
            currentTable = tableMatcher.group(1);
            tables.put(currentTable, new ArrayList<>());
            log.debug("Found table: {}", currentTable);
            continue;
        }

        if (currentTable != null) {
            Matcher columnMatcher = columnPattern.matcher(line);
            if (columnMatcher.find()) {
                String colName = columnMatcher.group(1);
                String sparkType = columnMatcher.group(2).trim().toLowerCase();

                // Always ignore arrays (optional), but NOT structs
                if (!sparkType.contains("array")) {
                    String sqlType = mapSparkTypeToSQL(sparkType);
                    tables.get(currentTable).add(new Column(colName, sqlType));
                    log.debug("Added column: {} ({}) to table {}", colName, sqlType, currentTable);
                }
            }
        }
    }

    return tables;
}

    /**
     * Fallback strategy 1: struct-based parsing
     */
    private static Map<String, List<Column>> tryStructBasedParsing(final String schema) {
        Map<String, List<Column>> tables = new LinkedHashMap<>();
        String[] lines = schema.split("\\n");
        String currentTable = null;

        Pattern tablePattern = Pattern.compile("^[\\s|]*[-|]*\\s*([a-zA-Z_][\\w_]*)\\s*:\\s*struct.*", Pattern.CASE_INSENSITIVE);
        Pattern columnPattern = Pattern.compile("^[\\s|]*[-|]+\\s*([a-zA-Z_][\\w_]*)\\s*:\\s*([a-zA-Z_][\\w_<>()\\[\\],\\s]*).*", Pattern.CASE_INSENSITIVE);

        for (String line : lines) {
            if (line.trim().isEmpty()) continue;

            Matcher tableMatcher = tablePattern.matcher(line);
            if (tableMatcher.find()) {
                currentTable = tableMatcher.group(1);
                tables.put(currentTable, new ArrayList<>());
                log.debug("Found table: {}", currentTable);
                continue;
            }

            if (currentTable != null) {
                Matcher columnMatcher = columnPattern.matcher(line);
                if (columnMatcher.find()) {
                    String colName = columnMatcher.group(1);
                    String sparkType = columnMatcher.group(2).trim();

                    if (!sparkType.toLowerCase().contains("struct") && !sparkType.toLowerCase().contains("array")) {
                        String sqlType = mapSparkTypeToSQL(sparkType);
                        tables.get(currentTable).add(new Column(colName, sqlType));
                        log.debug("Added column: {} ({}) to table {}", colName, sqlType, currentTable);
                    }
                }
            }
        }

        return tables;
    }

    /**
     * Fallback strategy 2: classic DataFrame.printSchema() output parsing
     */
    private static Map<String, List<Column>> tryDataFrameBasedParsing(final String schema) {
        Map<String, List<Column>> tables = new LinkedHashMap<>();

        if (!schema.toLowerCase().contains("root")) {
            return tables;
        }

        String[] lines = schema.split("\\n");
        String tableName = "main_table";
        List<Column> columns = new ArrayList<>();

        Pattern columnPattern = Pattern.compile("\\|--\\s+([a-zA-Z_][\\w_]*)\\s*:\\s*([a-zA-Z_][\\w_<>()\\[\\],\\s]*).*", Pattern.CASE_INSENSITIVE);

        for (String line : lines) {
            Matcher matcher = columnPattern.matcher(line);
            if (matcher.find()) {
                String colName = matcher.group(1);
                String sparkType = matcher.group(2).trim();

                // Clean type (remove nullable info)
                sparkType = sparkType.replaceAll("\\s*\\(.*\\)", "").trim();

                String sqlType = mapSparkTypeToSQL(sparkType);
                columns.add(new Column(colName, sqlType));
            }
        }

        if (!columns.isEmpty()) {
            tables.put(tableName, columns);
        }

        return tables;
    }

    /**
     * Fallback strategy 3: generic key-value pair parsing
     */
    private static Map<String, List<Column>> tryGenericParsing(final String schema) {
        Map<String, List<Column>> tables = new LinkedHashMap<>();

        Pattern columnPattern = Pattern.compile("([a-zA-Z_][\\w_]*)\\s*[:=]\\s*([a-zA-Z_][\\w_<>()\\[\\],\\s]*)", Pattern.CASE_INSENSITIVE);

        String tableName = "inferred_table";
        List<Column> columns = new ArrayList<>();

        Matcher matcher = columnPattern.matcher(schema);
        while (matcher.find()) {
            String name = matcher.group(1);
            String type = matcher.group(2).trim();

            if (isValidColumnName(name) && isValidTypeName(type)) {
                String sqlType = mapSparkTypeToSQL(type);
                columns.add(new Column(name, sqlType));
            }
        }

        if (!columns.isEmpty()) {
            tables.put(tableName, columns);
        }

        return tables;
    }

    private static boolean isValidColumnName(final String name) {
        return name != null &&
               name.matches("[a-zA-Z_][\\w_]*") &&
               !isReservedWord(name.toLowerCase());
    }

    private static boolean isValidTypeName(final String type) {
        if (type == null || type.trim().isEmpty()) return false;

        Set<String> validTypes = Set.of(
            "string", "integer", "int", "long", "bigint", "double", "float",
            "boolean", "date", "timestamp", "decimal", "binary", "byte",
            "short", "array", "map", "struct"
        );

        String cleanType = type.toLowerCase().replaceAll("[<>()\\[\\],\\s].*", "");
        return validTypes.contains(cleanType);
    }

    private static boolean isReservedWord(final String word) {
        Set<String> reserved = Set.of(
            "select", "from", "where", "group", "order", "by", "having",
            "union", "join", "inner", "outer", "left", "right", "on",
            "root", "struct", "array", "map", "nullable", "true", "false"
        );
        return reserved.contains(word);
    }

    /**
     * Mapping Spark types to simple SQL types
     */
    private static String mapSparkTypeToSQL(final String sparkType) {
        if (sparkType == null) return "STRING";

        String cleanType = sparkType.toLowerCase().trim();

        if (cleanType.startsWith("array")) {
            return "ARRAY<STRING>";
        }

        if (cleanType.startsWith("map")) {
            return "MAP<STRING,STRING>";
        }

        if (cleanType.startsWith("struct")) {
            return "STRUCT";
        }

        if (cleanType.startsWith("decimal")) {
            return sparkType.toUpperCase();
        }

        return switch (cleanType) {
            case "integer", "int" -> "INT";
            case "string" -> "STRING";
            case "double" -> "DOUBLE";
            case "date" -> "DATE";
            case "timestamp" -> "TIMESTAMP";
            case "boolean" -> "BOOLEAN";
            case "long", "bigint" -> "BIGINT";
            case "float" -> "FLOAT";
            case "binary" -> "BINARY";
            case "byte" -> "TINYINT";
            case "short" -> "SMALLINT";
            default -> sparkType.toUpperCase();
        };
    }

    /**
     * Generate a SQL CREATE TABLE DDL script from the parsed schema
     */
    public static String generateDDL(final Map<String, List<Column>> tables) {
        if (tables.isEmpty()) {
            return "-- No tables found in schema";
        }

        StringBuilder ddl = new StringBuilder();
        ddl.append("-- Generated DDL from Spark Context\n");
        ddl.append("-- Note: This is inferred from the schema and may need adjustments\n\n");

        for (Map.Entry<String, List<Column>> entry : tables.entrySet()) {
            String tableName = entry.getKey();
            List<Column> columns = entry.getValue();

            if (columns.isEmpty()) continue;

            ddl.append("CREATE TABLE ").append(tableName).append(" (\n");

            for (int i = 0; i < columns.size(); i++) {
                Column col = columns.get(i);
                ddl.append("    ").append(col.name).append(" ").append(col.type);
                if (i < columns.size() - 1) {
                    ddl.append(",");
                }
                ddl.append("\n");
            }

            ddl.append(");\n\n");
        }

        return ddl.toString();
    }

    /**
     * Generates a textual summary of tables and columns to provide context for SQL generation
     */
    public static String generateContextInfo(final Map<String, List<Column>> tables) {
        if (tables.isEmpty()) {
            return "No schema information available";
        }

        StringBuilder context = new StringBuilder();
        context.append("AVAILABLE TABLES AND COLUMNS:\n\n");

        for (Map.Entry<String, List<Column>> entry : tables.entrySet()) {
            String tableName = entry.getKey();
            List<Column> columns = entry.getValue();

            context.append("TABLE: ").append(tableName).append("\n");
            context.append("COLUMNS:\n");

            for (Column col : columns) {
                context.append("  - ").append(col.name)
                       .append(" (").append(col.type).append(")\n");
            }
            context.append("\n");
        }

        context.append(getInferredRelationships(tables));

        return context.toString();
    }

    /**
     * Infers potential relationships based on the convention of _id suffix in columns
     */
    public static String getInferredRelationships(final Map<String, List<Column>> tables) {
        StringBuilder relationships = new StringBuilder();
        relationships.append("POTENTIAL RELATIONSHIPS:\n");

        Set<String> foundRelationships = new HashSet<>();

        for (Map.Entry<String, List<Column>> sourceEntry : tables.entrySet()) {
            String sourceTable = sourceEntry.getKey();

            for (Column sourceCol : sourceEntry.getValue()) {
                String colName = sourceCol.name.toLowerCase();

                if (colName.endsWith("_id") || colName.endsWith("id")) {
                    String baseName = colName.replaceAll("_?id$", "");

                    for (String targetTable : tables.keySet()) {
                        if (targetTable.equals(sourceTable)) continue;

                        if (isTableMatch(targetTable, baseName)) {
                            String relation = String.format("-- %s.%s might reference %s.id", sourceTable, sourceCol.name, targetTable);
                            if (!foundRelationships.contains(relation)) {
                                relationships.append(relation).append("\n");
                                foundRelationships.add(relation);
                            }
                        }
                    }
                }
            }
        }

        if (foundRelationships.isEmpty()) {
            relationships.append("-- No obvious relationships detected\n");
        }

        return relationships.toString();
    }

    private static boolean isTableMatch(final String tableName, final String baseName) {
        if (baseName.isEmpty()) return false;

        String cleanTable = tableName.toLowerCase();
        String cleanBase = baseName.toLowerCase();

        return cleanTable.equals(cleanBase) ||
               cleanTable.equals(cleanBase + "s") ||
               cleanTable.equals(cleanBase + "es") ||
               cleanBase.equals(cleanTable) ||
               cleanTable.contains(cleanBase) ||
               cleanBase.contains(cleanTable);
    }

    private static void logParsedTables(final Map<String, List<Column>> tables) {
        if (log.isDebugEnabled()) {
            for (Map.Entry<String, List<Column>> entry : tables.entrySet()) {
                log.debug("Table {}: {} columns", entry.getKey(), entry.getValue().size());
                for (Column col : entry.getValue()) {
                    log.debug("  - {} ({})", col.name, col.type);
                }
            }
        }
    }

    /**
     * Inner class representing a column (name + SQL type)
     */
    public static class Column {
        public final String name;
        public final String type;

        public Column(final String name, final String type) {
            this.name = name;
            this.type = type;
        }

        @Override
        public String toString() {
            return name + " " + type;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) return true;
            if (obj == null || getClass() != obj.getClass()) return false;
            Column column = (Column) obj;
            return Objects.equals(name, column.name) && Objects.equals(type, column.type);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, type);
        }
    }
}
