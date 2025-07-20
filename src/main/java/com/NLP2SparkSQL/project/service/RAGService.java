package com.NLP2SparkSQL.project.service;

import com.NLP2SparkSQL.project.dto.QueryResponse;
import com.NLP2SparkSQL.project.utils.EmbeddingUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import com.NLP2SparkSQL.project.utils.SparkSchemaParser;

import java.util.Map;
import java.util.List;
import java.util.UUID;
import java.util.regex.Pattern;

@Slf4j
@Service
@RequiredArgsConstructor
public class RAGService {

    private final QdrantService qdrantService;
    private final LangChainSQLService langChainSQLService;

    @Value("${app.max-query-length:10000}")
    private int maxQueryLength;

    @Value("${app.enable-fallback-context:true}")
    private boolean enableFallbackContext;

    @Value("${app.enable-sql-validation:true}")
    private boolean enableSQLValidation;

    private static final Pattern COMPLEX_QUESTION_PATTERN = Pattern.compile(
        "(?i)(join|combine|merge|relationship|across|between|multiple|compare|analyze|trend|pattern|correlation|distribution|group|by)"
    );

    public QueryResponse processQuestion(String question) {
        String requestId = UUID.randomUUID().toString().substring(0, 8);
        log.info("[{}] Processing question: {}", requestId, question);

        if (question == null || question.trim().isEmpty()) {
    return createErrorResponse("Question cannot be empty", 0L);
}
if (question.length() > maxQueryLength) {
    return createErrorResponse("Question too long (max " + maxQueryLength + " characters)", 0L);
}


        String processedQuestion = preprocessQuestion(question);
        float[] embedding = EmbeddingUtils.embed(processedQuestion);
        if (embedding == null || embedding.length == 0) {
        return createErrorResponse("Message d'erreur", 0L);
        }

        String context = retrieveEnhancedContext(embedding, processedQuestion, requestId);
        String sparkSql = langChainSQLService.generateSQL(context, processedQuestion);
        String finalSql = postProcessSQL(sparkSql, requestId);

        return new QueryResponse(
    finalSql,
    sparkSql.contains("ERROR:") ? "Error in SQL generation" : "SQL generated successfully",
    0L
);
}

    public QueryResponse processQuestionWithContext(String sparkContext, String question) {
    String requestId = UUID.randomUUID().toString().substring(0, 8);
    log.info("[{}] Processing question with context: {}", requestId, question);

    if (question == null || question.trim().isEmpty()) {
    return createErrorResponse("Question cannot be empty", 0L);
}
if (sparkContext == null || sparkContext.trim().isEmpty()) {
    return createErrorResponse("Spark context must not be empty", 0L);
}


    String processedQuestion = preprocessQuestion(question);
    
    // 1. Parse the dynamic Spark schema
    Map<String, List<SparkSchemaParser.Column>> tables = 
        SparkSchemaParser.parseSparkSchema(sparkContext);
    
    if (tables.isEmpty()) {
        log.warn("[{}] No tables found in schema", requestId);
        return createErrorResponse("Could not parse any tables from the provided schema",0L);
    }

    // 2. Build enhanced context for dynamic tables
    StringBuilder contextBuilder = new StringBuilder();
    
    // Add available tables and columns
    contextBuilder.append("AVAILABLE TABLES AND COLUMNS:\n");
    for (var entry : tables.entrySet()) {
        contextBuilder.append("TABLE ").append(entry.getKey()).append(":\n");
        for (var col : entry.getValue()) {
            contextBuilder.append("  - ").append(col.name).append(" (").append(col.type).append(")\n");
        }
        contextBuilder.append("\n");
    }
    
    // 3. Add relationships
    String relationships = SparkSchemaParser.getInferredRelationships(tables);
    contextBuilder.append(relationships).append("\n\n");
    
    // 4. Add query-specific hints
    contextBuilder.append(generateDynamicQueryHints(processedQuestion, tables));
    
    // 5. Enhanced prompt for dynamic schema
    String enhancedPrompt = buildDynamicSchemaPrompt(contextBuilder.toString(), processedQuestion, tables);
    
    // 6. Generate SQL with enhanced context
    String sparkSql = langChainSQLService.generateSQL(enhancedPrompt, processedQuestion);
    String finalSql = postProcessSQL(sparkSql, requestId);

   return new QueryResponse(
    finalSql,
    sparkSql.contains("ERROR:") ? "Error in SQL generation" : "SQL generated successfully",
    0L
);
}

private String buildDynamicSchemaPrompt(String context, String question, 
                                       Map<String, List<SparkSchemaParser.Column>> tables) {
    StringBuilder prompt = new StringBuilder();
    
    prompt.append("You are an expert Spark SQL generator. Generate ONLY valid Spark SQL queries.\n\n");
    
    prompt.append("CRITICAL RULES:\n");
    prompt.append("1. Use ONLY the exact table and column names provided below\n");
    prompt.append("2. Table names may have multiple 's' characters - use them exactly as shown\n");
    prompt.append("3. When joining tables, use the relationships indicated\n");
    prompt.append("4. Always use table aliases for clarity\n");
    prompt.append("5. Return only the SQL query with semicolon at the end\n\n");
    
    prompt.append(context);
    
    // Add specific examples based on available tables
    if (hasEmployeeRelatedTables(tables)) {
        prompt.append("EXAMPLE FOR EMPLOYEE QUERIES:\n");
        String empTable = findTableByPattern(tables, "employee");
        String deptTable = findTableByPattern(tables, "department");
        if (empTable != null && deptTable != null) {
            prompt.append("SELECT d.department_name, COUNT(e.emp_id) as employee_count\n");
            prompt.append("FROM ").append(empTable).append(" e\n");
            prompt.append("JOIN ").append(deptTable).append(" d ON e.department_id = d.department_id\n");
            prompt.append("GROUP BY d.department_name;\n\n");
        }
    }
    
    prompt.append("QUESTION: ").append(question).append("\n\n");
    prompt.append("SPARK SQL QUERY:");
    
    return prompt.toString();
}

private String generateDynamicQueryHints(String question, 
                                       Map<String, List<SparkSchemaParser.Column>> tables) {
    StringBuilder hints = new StringBuilder("QUERY HINTS:\n");
    
    // Extract actual table names for hints
    String employeeTable = findTableByPattern(tables, "employee");
    String departmentTable = findTableByPattern(tables, "department");
    String jobTable = findTableByPattern(tables, "job");
    String locationTable = findTableByPattern(tables, "location");
    String countryTable = findTableByPattern(tables, "countr");
    String regionTable = findTableByPattern(tables, "region");
    
    if (question.toLowerCase().contains("region") && employeeTable != null) {
        hints.append("-- To get data by region, join: ");
        hints.append(employeeTable);
        if (departmentTable != null) hints.append(" -> ").append(departmentTable);
        if (locationTable != null) hints.append(" -> ").append(locationTable);
        if (countryTable != null) hints.append(" -> ").append(countryTable);
        if (regionTable != null) hints.append(" -> ").append(regionTable);
        hints.append("\n");
    }
    
    if (question.toLowerCase().contains("job") && jobTable != null) {
        hints.append("-- For job-related queries, join ").append(employeeTable)
              .append(" with ").append(jobTable).append(" using job_id\n");
    }
    
    if (question.toLowerCase().contains("salary") || question.toLowerCase().contains("average")) {
        hints.append("-- Use AVG(salary) for average salary calculations\n");
        hints.append("-- Use COUNT(*) or COUNT(emp_id) for employee counts\n");
    }
    
    if (question.toLowerCase().contains("2015")) {
        hints.append("-- Filter by hire_date > '2015-12-31' for employees hired after 2015\n");
    }
    
    return hints.toString();
}

private boolean hasEmployeeRelatedTables(Map<String, List<SparkSchemaParser.Column>> tables) {
    return tables.keySet().stream()
        .anyMatch(name -> name.toLowerCase().contains("employee"));
}

private String findTableByPattern(Map<String, List<SparkSchemaParser.Column>> tables, String pattern) {
    return tables.keySet().stream()
        .filter(name -> name.toLowerCase().contains(pattern.toLowerCase()))
        .findFirst()
        .orElse(null);
}
    private String preprocessQuestion(String question) {
        String processed = question.trim()
            .replaceAll("(?i)how many", "count")
            .replaceAll("(?i)what is the total", "sum")
            .replaceAll("(?i)what is the average", "average")
            .replaceAll("(?i)(show me|get|give me|find|list)", "select")
            .replaceAll("(?i)distributed by", "group by")
            .replaceAll("(?i)distribution", "grouping");

        // Ajouter des tags pour le type de requête
        if (processed.matches("(?i).*\\b(today|yesterday|last month|this year|last year)\\b.*")) {
            processed += " [TIME_SENSITIVE]";
        }

        if (processed.matches("(?i).*(total|sum|count|average|avg|max|min|group by|distribution).*")) {
            processed += " [AGGREGATION]";
        }

        if (COMPLEX_QUESTION_PATTERN.matcher(processed).find()) {
            processed += " [COMPLEX]";
        }

        return processed;
    }

    private String retrieveEnhancedContext(float[] embedding, String question, String requestId) {
    try {
        Map<String, String> contextMap = qdrantService.searchRelevantContextStructured(embedding);
        String context = contextMap.getOrDefault("sql", "");

        if ((context == null || context.isBlank()) && enableFallbackContext) {
            context = getFallbackContext();
        }

        return enhanceContextForQuestion(context, question);
    } catch (Exception e) {
        log.error("[{}] Context retrieval error: {}", requestId, e.getMessage());
        return getFallbackContext();
    }
}
    private String enhanceContextForQuestion(String baseContext, String question) {
        StringBuilder context = new StringBuilder(baseContext);
        if (question.contains("[TIME_SENSITIVE]")) {
            context.append("\n-- Tip: Use CURRENT_DATE, DATE_SUB, etc.");
        }
        if (question.contains("[AGGREGATION]")) {
            context.append("\n-- Tip: Use SUM(), COUNT(), GROUP BY");
        }
        if (question.contains("[COMPLEX]")) {
            context.append("\n-- Tip: JOIN tables using foreign keys, use aliases");
        }
        return context.toString();
    }

    private String postProcessSQL(String sql, String requestId) {
        if (sql == null || sql.isBlank()) {
            return createErrorSQL("Empty SQL generated");
        }

        // Enlever les tags de preprocessing
        sql = sql.replaceAll("\\[.*?\\]", "");
        
        // Formater le SQL pour une meilleure lisibilité
        sql = formatSQL(sql);

        if (enableSQLValidation && !isValidSQL(sql)) {
            log.warn("[{}] SQL failed validation: {}", requestId, sql);
            return createErrorSQL("Generated SQL failed validation");
        }

        return sql;
    }

    private String formatSQL(String sql) {
        return sql.replaceAll("(?i)\\b(SELECT|FROM|WHERE|GROUP BY|HAVING|ORDER BY|JOIN|LEFT JOIN|RIGHT JOIN|INNER JOIN|WITH|CREATE|DELETE|INSERT|UPDATE|DROP)\\b", "\n$1")
                  .replaceAll("\\n+", "\n")
                  .replaceAll("^\\n", "")
                  .trim();
    }

    private boolean isValidSQL(String sql) {
        if (sql == null || sql.isBlank()) return false;
        String upper = sql.toUpperCase().trim();
        String[] allowed = {"SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "WITH"};
        for (String cmd : allowed) {
            if (upper.startsWith(cmd)) return true;
        }
        return false;
    }

    private QueryResponse createErrorResponse(String message, long time) {
    return new QueryResponse(
        createErrorSQL(message),
        "Error: " + message,
        time
    );
    }

    private String createErrorSQL(String message) {
        return String.format("/* Error: %s */\nSELECT 'ERROR: %s' AS error_message;", message, message);
    }

    private String getFallbackContext() {
        return """
            -- FALLBACK DATABASE SCHEMA --

            TABLE employees (
                emp_id INT,
                first_name STRING,
                last_name STRING,
                email STRING,
                hire_date DATE,
                salary DOUBLE,
                department_id INT
            );

            TABLE departments (
                department_id INT,
                department_name STRING,
                location_id INT
            );

            TABLE locations (
                location_id INT,
                city STRING,
                country_id STRING
            );

            TABLE countries (
                country_id STRING,
                country_name STRING,
                region_id INT
            );

            TABLE regions (
                region_id INT,
                region_name STRING
            );

            RELATIONSHIPS:
            - employees.department_id -> departments.department_id
            - departments.location_id -> locations.location_id
            - locations.country_id -> countries.country_id
            - countries.region_id -> regions.region_id
            """;
    }

    public boolean isHealthy() {
        try {
            return qdrantService.isHealthy() && langChainSQLService.isHealthy();
        } catch (Exception e) {
            log.error("RAG service health check failed: {}", e.getMessage());
            return false;
        }
    }
} 