package com.NLP2SparkSQL.project.utils;

import java.util.*;
import java.util.regex.*;

public class SparkSchemaParser {

    public static Map<String, List<Column>> parseSparkSchema(String schema) {
        // Clean control characters and normalize whitespace
        schema = schema.replaceAll("[\\u0000-\\u001F&&[^\\n]]", "");
        
        Map<String, List<Column>> tables = new LinkedHashMap<>();
        String currentTable = null;

        String[] lines = schema.split("\\r?\\n");
        
        // More flexible patterns to handle dynamic table names
        Pattern tablePattern = Pattern.compile("^\\|--\\s*(\\w+):\\s*struct");
        Pattern columnPattern = Pattern.compile("^\\|\\s*\\|--\\s*(\\w+):\\s*(\\w+)");
        
        // Alternative patterns for different schema formats
        Pattern altTablePattern = Pattern.compile("Found table:\\s*(\\w+)");
        Pattern altColumnPattern = Pattern.compile("Parsed column:\\s*(\\w+)\\s+type:\\s*(\\w+)\\s+for table\\s+(\\w+)");

        for (String line : lines) {
            line = line.trim();
            
            // Try standard tree format first
            Matcher tableMatcher = tablePattern.matcher(line);
            if (tableMatcher.find()) {
                currentTable = tableMatcher.group(1);
                tables.put(currentTable, new ArrayList<>());
                System.out.println("Found table: " + currentTable);
                continue;
            }
            
            // Try alternative format from logs
            Matcher altTableMatcher = altTablePattern.matcher(line);
            if (altTableMatcher.find()) {
                currentTable = altTableMatcher.group(1);
                tables.put(currentTable, new ArrayList<>());
                System.out.println("Found table (alt): " + currentTable);
                continue;
            }
            
            // Parse columns in standard format
            if (currentTable != null) {
                Matcher columnMatcher = columnPattern.matcher(line);
                if (columnMatcher.find()) {
                    String colName = columnMatcher.group(1);
                    String sparkType = columnMatcher.group(2);
                    String sqlType = sparkTypeToSqlType(sparkType);
                    tables.get(currentTable).add(new Column(colName, sqlType));
                    System.out.println("Parsed column: " + colName + " type: " + sqlType + " for table " + currentTable);
                    continue;
                }
            }
            
            // Parse columns in alternative format
            Matcher altColumnMatcher = altColumnPattern.matcher(line);
            if (altColumnMatcher.find()) {
                String colName = altColumnMatcher.group(1);
                String sparkType = altColumnMatcher.group(2);
                String tableName = altColumnMatcher.group(3);
                String sqlType = sparkTypeToSqlType(sparkType);
                
                // Ensure table exists
                tables.computeIfAbsent(tableName, k -> new ArrayList<>());
                tables.get(tableName).add(new Column(colName, sqlType));
                System.out.println("Parsed column (alt): " + colName + " type: " + sqlType + " for table " + tableName);
            }
        }
        
        System.out.println("Final parsed tables: " + tables.keySet());
        return tables;
    }

    // Enhanced type mapping
    private static String sparkTypeToSqlType(String sparkType) {
        return switch (sparkType.toLowerCase()) {
            case "integer", "int" -> "INT";
            case "string" -> "STRING";
            case "double" -> "DOUBLE";
            case "date" -> "DATE";
            case "timestamp" -> "TIMESTAMP";
            case "boolean" -> "BOOLEAN";
            case "long", "bigint" -> "BIGINT";
            case "float" -> "FLOAT";
            case "decimal" -> "DECIMAL";
            default -> sparkType.toUpperCase();
        };
    }

    // Dynamic table formatting that handles any table names
    public static String formatTablesAsSql(Map<String, List<Column>> tables) {
        StringBuilder sb = new StringBuilder();
        for (var entry : tables.entrySet()) {
            sb.append("TABLE ").append(entry.getKey()).append(" (\n");
            List<String> cols = new ArrayList<>();
            for (Column col : entry.getValue()) {
                cols.add("    " + col.name + " " + col.type);
            }
            sb.append(String.join(",\n", cols));
            sb.append("\n);\n\n");
        }
        return sb.toString().trim();
    }

    // Improved relationship inference that works with any table names
    public static String getInferredRelationships(Map<String, List<Column>> tables) {
        StringBuilder sb = new StringBuilder("-- INFERRED RELATIONSHIPS:\n");
        Set<String> addedRelationships = new HashSet<>();

        for (var sourceEntry : tables.entrySet()) {
            String sourceTable = sourceEntry.getKey();
            for (Column sourceCol : sourceEntry.getValue()) {
                if (sourceCol.name.endsWith("_id")) {
                    String baseName = sourceCol.name.replace("_id", "");
                    
                    // Find target table by matching base name (flexible matching)
                    for (var targetEntry : tables.entrySet()) {
                        String targetTable = targetEntry.getKey();
                        if (targetTable.equals(sourceTable)) continue;
                        
                        // More flexible matching for dynamic table names
                        if (isTableNameMatch(targetTable, baseName)) {
                            // Check if target table has matching ID column
                            boolean hasMatchingId = targetEntry.getValue().stream()
                                .anyMatch(col -> col.name.equals(sourceCol.name) || 
                                               col.name.equals("id") ||
                                               col.name.equals(extractBaseName(targetTable) + "_id"));
                            
                            if (hasMatchingId) {
                                String relationship = sourceTable + "." + sourceCol.name + " -> " + targetTable + "." + getTargetIdColumn(targetEntry.getValue(), sourceCol.name);
                                if (!addedRelationships.contains(relationship)) {
                                    sb.append("-- ").append(relationship).append("\n");
                                    addedRelationships.add(relationship);
                                }
                            }
                        }
                    }
                }
            }
        }

        // Add known relationships based on common patterns
        addKnownRelationships(sb, tables, addedRelationships);
        
        return sb.toString().trim();
    }

    // Extract base name from table name (removes extra 's' characters)
    private static String extractBaseName(String tableName) {
        // Remove trailing 's' characters to get base name
        return tableName.replaceAll("s+$", "");
    }

    // Check if table name matches the base name (handles dynamic naming)
    private static boolean isTableNameMatch(String tableName, String baseName) {
        String cleanTableName = extractBaseName(tableName);
        return cleanTableName.equalsIgnoreCase(baseName) ||
               cleanTableName.equalsIgnoreCase(baseName + "s") ||
               cleanTableName.equalsIgnoreCase(baseName + "es") ||
               baseName.equalsIgnoreCase(cleanTableName);
    }

    // Find the correct target ID column
    private static String getTargetIdColumn(List<Column> columns, String sourceIdColumn) {
        // Try to find exact match first
        for (Column col : columns) {
            if (col.name.equals(sourceIdColumn)) {
                return col.name;
            }
        }
        
        // Try to find 'id' column
        for (Column col : columns) {
            if (col.name.equals("id")) {
                return col.name;
            }
        }
        
        // Default to the source column name
        return sourceIdColumn;
    }

    // Enhanced known relationships that work with dynamic table names
    private static void addKnownRelationships(StringBuilder sb, Map<String, List<Column>> tables, Set<String> added) {
        // Find tables by pattern matching instead of exact names
        String employeeTable = findTableByPattern(tables, "employee");
        String departmentTable = findTableByPattern(tables, "department");
        String jobTable = findTableByPattern(tables, "job");
        String locationTable = findTableByPattern(tables, "location");
        String countryTable = findTableByPattern(tables, "countr");
        String regionTable = findTableByPattern(tables, "region");
        
        // Build relationships dynamically
        List<String> knownRelations = new ArrayList<>();
        
        if (employeeTable != null && departmentTable != null) {
            knownRelations.add(employeeTable + ".department_id -> " + departmentTable + ".department_id");
        }
        if (employeeTable != null && jobTable != null) {
            knownRelations.add(employeeTable + ".job_id -> " + jobTable + ".job_id");
        }
        if (departmentTable != null && locationTable != null) {
            knownRelations.add(departmentTable + ".location_id -> " + locationTable + ".location_id");
        }
        if (locationTable != null && countryTable != null) {
            knownRelations.add(locationTable + ".country_id -> " + countryTable + ".country_id");
        }
        if (countryTable != null && regionTable != null) {
            knownRelations.add(countryTable + ".region_id -> " + regionTable + ".region_id");
        }
        
        for (String relation : knownRelations) {
            if (!added.contains(relation)) {
                String[] parts = relation.split(" -> ");
                String[] source = parts[0].split("\\.");
                String[] target = parts[1].split("\\.");
                
                if (tables.containsKey(source[0]) && tables.containsKey(target[0])) {
                    boolean sourceHasCol = tables.get(source[0]).stream()
                        .anyMatch(col -> col.name.equals(source[1]));
                    boolean targetHasCol = tables.get(target[0]).stream()
                        .anyMatch(col -> col.name.equals(target[1]));
                    
                    if (sourceHasCol && targetHasCol) {
                        sb.append("-- ").append(relation).append("\n");
                        added.add(relation);
                    }
                }
            }
        }
    }

    // Find table by pattern matching
    private static String findTableByPattern(Map<String, List<Column>> tables, String pattern) {
        for (String tableName : tables.keySet()) {
            if (tableName.toLowerCase().contains(pattern.toLowerCase())) {
                return tableName;
            }
        }
        return null;
    }

    // Column class remains the same
    public static class Column {
        public String name;
        public String type;

        public Column(String name, String type) {
            this.name = name;
            this.type = type;
        }
        
        @Override
        public String toString() {
            return name + " " + type;
        }
    }
}