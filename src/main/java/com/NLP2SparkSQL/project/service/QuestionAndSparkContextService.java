package com.NLP2SparkSQL.project.service;

import com.NLP2SparkSQL.project.dto.QueryResponse;
import com.NLP2SparkSQL.project.utils.SparkSchemaParser;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class QuestionAndSparkContextService {

    private final LangChainSQLService langChainSQLService;

    public QueryResponse generateSqlFromSchemaAndQuestion(String sparkContext, String question) {
        String requestId = java.util.UUID.randomUUID().toString().substring(0, 8);
        log.info("[{}] Generating SQL for question: {}", requestId, question);

        if (question == null || question.trim().isEmpty()) {
            return createErrorResponse("Question cannot be empty"); // MODIF: createErrorResponse modifié aussi
        }
        if (sparkContext == null || sparkContext.trim().isEmpty()) {
            return createErrorResponse("Spark context must not be empty"); // MODIF
        }

        Map<String, List<SparkSchemaParser.Column>> tables = SparkSchemaParser.parseSparkSchema(sparkContext);
        if (tables.isEmpty()) {
            log.warn("[{}] No tables found in schema", requestId);
            return createErrorResponse("Could not parse any tables from the provided schema"); // MODIF
        }

        String prompt = buildPrompt(tables, question);

        String rawSql = langChainSQLService.generateSQL(prompt, question);

        String finalSql = postProcessSQL(rawSql, requestId);

        // MODIF: ajout du 3ème paramètre 0L (temps non mesuré ici)
        return new QueryResponse(
            finalSql,
            finalSql.contains("ERROR:") ? "Error in SQL generation" : "SQL generated successfully",
            0L
        );
    }

    private String buildPrompt(Map<String, List<SparkSchemaParser.Column>> tables, String question) {
        StringBuilder prompt = new StringBuilder();

        prompt.append("You are an expert Spark SQL generator. Generate ONLY valid Spark SQL queries.\n\n");
        prompt.append("CRITICAL RULES:\n");
        prompt.append("1. Use ONLY the exact table and column names provided below.\n");
        prompt.append("2. Use table aliases for clarity when joining.\n");
        prompt.append("3. Return only the SQL query with a semicolon at the end.\n\n");

        prompt.append("AVAILABLE TABLES AND COLUMNS:\n");
        for (var entry : tables.entrySet()) {
            prompt.append("TABLE ").append(entry.getKey()).append(":\n");
            for (var col : entry.getValue()) {
                prompt.append("  - ").append(col.name).append(" (").append(col.type).append(")\n");
            }
            prompt.append("\n");
        }

        String relationships = SparkSchemaParser.getInferredRelationships(tables);
        if (relationships != null && !relationships.isBlank()) {
            prompt.append("RELATIONSHIPS:\n").append(relationships).append("\n\n");
        }

        prompt.append("QUESTION:\n").append(question).append("\n\n");
        prompt.append("SPARK SQL QUERY:");

        return prompt.toString();
    }

    private String postProcessSQL(String sql, String requestId) {
        if (sql == null || sql.isBlank()) {
            return createErrorSQL("Empty SQL generated");
        }

        sql = sql.replaceAll("\\[.*?\\]", "");

        sql = formatSQL(sql);

        if (!isValidSQL(sql)) {
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
        String[] allowedStart = {"SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "WITH"};
        for (String cmd : allowedStart) {
            if (upper.startsWith(cmd)) return true;
        }
        return false;
    }

    // MODIF ici aussi: ajout du 3ème argument 0L dans le constructeur
    private QueryResponse createErrorResponse(String message) {
        return new QueryResponse(
            createErrorSQL(message),
            "Error: " + message,
            0L
        );
    }

    private String createErrorSQL(String message) {
        return String.format("/* Error: %s */\nSELECT 'ERROR: %s' AS error_message;", message, message);
    }
}
