package com.NLP2SparkSQL.project.service;

import dev.langchain4j.model.ollama.OllamaLanguageModel;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.regex.Pattern;

@Slf4j
@Service
public class LangChainSQLService {

    private final OllamaLanguageModel model;

    // Filtrage des instructions réellement dangereuses
    private static final Pattern FORBIDDEN_PATTERN = Pattern.compile(
            "(?i)(SCRIPT|DECLARE|\\bEXEC\\b|--|;\\s*--)", Pattern.CASE_INSENSITIVE
    );

    @Value("${ollama.url:http://localhost:11434}")
    private String ollamaUrl;

    @Value("${ollama.model:qwen3:1.7b}")
    private String modelName;

    public LangChainSQLService() {
        this.model = OllamaLanguageModel.builder()
                .baseUrl("http://localhost:11434")
                .modelName("qwen3:1.7b")
                .temperature(0.1)
                .timeout(java.time.Duration.ofSeconds(120))
                .build();
    }

    public String generateSQL(String context, String question) {
        String requestId = java.util.UUID.randomUUID().toString().substring(0, 8);
        log.info("[{}] Generating SQL for question: {}", requestId, question);

        try {
            if (context == null || context.trim().isEmpty()) {
                return generateErrorSQL("No database context provided");
            }

            if (question == null || question.trim().isEmpty()) {
                return generateErrorSQL("No question provided");
            }

            String prompt = buildEnhancedPrompt(context, question);
            String generatedSQL = generateWithRetry(prompt, 3);
            String cleanedSQL = cleanSQL(generatedSQL);

            if (!isValidSparkSQL(cleanedSQL)) {
                log.warn("[{}] Generated SQL failed validation: {}", requestId, cleanedSQL);
                return generateErrorSQL("Generated SQL failed validation checks");
            }

            log.info("[{}] Successfully generated SQL: {}", requestId, cleanedSQL);
            return cleanedSQL;

        } catch (Exception e) {
            log.error("[{}] Error generating SQL: {}", requestId, e.getMessage(), e);
            return generateErrorSQL("SQL generation failed: " + e.getMessage());
        }
    }

    private String buildEnhancedPrompt(String context, String question) {
        return """
                You are a Spark SQL expert. Generate ONLY valid Spark SQL queries (SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, WITH, RANK, etc.)
                based on the provided schema.

                RULES:
                1. Use ONLY tables and columns mentioned in the schema
                2. Do not invent schema or table names
                3. Use valid Spark SQL syntax
                4. Respond with 'INSUFFICIENT_DATA' if schema does not allow answering
                5. Prefer Spark SQL functions like AVG(), COUNT(), RANK() OVER (...) etc. where appropriate
                6. Support DML (INSERT, UPDATE, DELETE), DDL (CREATE, DROP), CTEs (WITH), aggregates, joins, subqueries, etc.

                EXAMPLES:
                Schema: TABLE employees (id INT, name STRING, salary DOUBLE)
                Question: Add a new employee named 'Alice' with a salary of 3000
                SQL: INSERT INTO employees (id, name, salary) VALUES (1, 'Alice', 3000);

                Schema: TABLE orders (id INT, total DOUBLE, customer_id INT)
                Question: Total order value per customer
                SQL: SELECT customer_id, SUM(total) AS total_value FROM orders GROUP BY customer_id;

                DATABASE SCHEMA:
                """ + context + "\n\nQUESTION: " + question + "\n\nSPARK SQL QUERY:";
    }

    private String generateWithRetry(String prompt, int maxRetries) {
        Exception lastException = null;

        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                log.debug("SQL generation attempt {} of {}", attempt, maxRetries);
                Object response = model.generate(prompt);
                String result = extractContentFromResponse(response);

                if (result != null && !result.trim().isEmpty()) {
                    return result;
                }
            } catch (Exception e) {
                lastException = e;
                log.warn("SQL generation attempt {} failed: {}", attempt, e.getMessage());
                if (attempt < maxRetries) {
                    try {
                        Thread.sleep(1000 * attempt);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }
        throw new RuntimeException("Failed to generate SQL after " + maxRetries + " attempts", lastException);
    }

    private String extractContentFromResponse(Object response) {
        if (response instanceof String) {
            return (String) response;
        } else {
            try {
                var method = response.getClass().getMethod("content");
                return (String) method.invoke(response);
            } catch (Exception e) {
                return response.toString();
            }
        }
    }

    private String cleanSQL(String sql) {
        if (sql == null || sql.trim().isEmpty()) return "";

        String cleaned = sql
                .replaceAll("(?is)<think>.*?</think>", "")
                .replaceAll("(?s)/\\*.*?\\*/", "")
                .replaceAll("--.*", "")
                .replaceAll("(?s)```sql\\s*", "")
                .replaceAll("(?s)```\\s*", "")
                .replaceAll("(?im)^(Explanation|Answer|Question|SQL|Query):.*$", "")
                .replaceAll("<[^>]+>", "")
                .replaceAll("\\n{2,}", "\n")
                .trim();

        if (cleaned.contains("INSUFFICIENT_DATA")) {
            return "SELECT 'INSUFFICIENT_DATA' AS message;";
        }

        if (cleaned.toUpperCase().contains("SELECT")) {
            int selectIndex = cleaned.toUpperCase().indexOf("SELECT");
            cleaned = cleaned.substring(selectIndex);
        }

        if (!cleaned.endsWith(";")) {
            cleaned += ";";
        }

        return cleaned;
    }

    private boolean isValidSparkSQL(String sql) {
        if (sql == null || sql.trim().isEmpty()) return false;

        String sqlUpper = sql.toUpperCase().trim();

        // Instructions valides
        boolean validStart = sqlUpper.startsWith("SELECT") ||
                             sqlUpper.startsWith("INSERT") ||
                             sqlUpper.startsWith("UPDATE") ||
                             sqlUpper.startsWith("DELETE") ||
                             sqlUpper.startsWith("CREATE") ||
                             sqlUpper.startsWith("DROP") ||
                             sqlUpper.startsWith("WITH");

        if (!validStart) return false;

        if (FORBIDDEN_PATTERN.matcher(sql).find()) {
            log.warn("SQL contains forbidden patterns: {}", sql);
            return false;
        }

        return hasMatchingParentheses(sql);
    }

    private boolean hasMatchingParentheses(String sql) {
        int count = 0;
        for (char c : sql.toCharArray()) {
            if (c == '(') count++;
            else if (c == ')') count--;
            if (count < 0) return false;
        }
        return count == 0;
    }

    private String generateErrorSQL(String error) {
        return String.format("/* Error: %s */\nSELECT 'ERROR: %s' AS error_message;", error, error);
    }

    public boolean isHealthy() {
        try {
            String testResponse = generateSQL(
                    "TABLE test (id INT, name STRING)",
                    "How many records are there?"
            );
            return testResponse != null && !testResponse.contains("ERROR:");
        } catch (Exception e) {
            log.error("Health check failed: {}", e.getMessage());
            return false;
        }
    }
}
