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
    
    /**
     * Pattern to detect potentially dangerous SQL operations or injections.
     * We keep EXEC, SCRIPT, DECLARE, MERGE, UNION to avoid dangerous injections,
     * but allow standard DML and DDL commands.
     */
    private static final Pattern SQL_INJECTION_PATTERN = Pattern.compile(
        "(?i)(EXEC|EXECUTE|SCRIPT|DECLARE|MERGE|UNION)\\s", 
        Pattern.CASE_INSENSITIVE
    );
    
    /**
     * Pattern to validate the SQL starts with allowed commands:
     * SELECT, INSERT, UPDATE, DELETE, CREATE, ALTER, DROP, TRUNCATE
     */
    private static final Pattern VALID_SQL_PATTERN = Pattern.compile(
        "^\\s*(SELECT|INSERT|UPDATE|DELETE|CREATE|ALTER|DROP|TRUNCATE)\\b.*", 
        Pattern.CASE_INSENSITIVE | Pattern.DOTALL
    );

    @Value("${ollama.url:http://localhost:11434}")
    private String ollamaUrl;

    @Value("${ollama.model:qwen3:1.7b}")
    private String modelName;

    public LangChainSQLService() {
        this.model = OllamaLanguageModel.builder()
                .baseUrl("http://localhost:11434")
                .modelName("qwen3:1.7b")
                .temperature(0.1) // Low temperature for deterministic generation
                .timeout(java.time.Duration.ofSeconds(120))
                .build();
    }

    /**
     * Main method to generate SQL query from schema context and natural language question.
     * Supports full SQL functionality: joins, aggregations, DML and DDL commands.
     * Returns either a valid SQL string or an error SQL with explanation.
     */
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

    /**
     * Build a clear prompt including strict generation rules and few-shot examples,
     * specifying that all valid SQL query types, including joins and aggregations, 
     * are allowed and expected.
     */
    private String buildEnhancedPrompt(String context, String question) {
        StringBuilder prompt = new StringBuilder();

        prompt.append("You are a Spark SQL expert. Generate ONLY valid Spark SQL queries based on the provided schema.\n\n");
        prompt.append("RULES:\n");
        prompt.append("1. Use ONLY tables and columns from the schema.\n");
        prompt.append("2. You can generate any valid SQL statements: SELECT, INSERT, UPDATE, DELETE, CREATE, ALTER, DROP, TRUNCATE.\n");
        prompt.append("3. Use correct Spark SQL syntax, including JOINs (INNER, LEFT, RIGHT, FULL), aggregations (GROUP BY, HAVING), and window functions.\n");
        prompt.append("4. Use aliases for tables and columns for readability.\n");
        prompt.append("5. Handle NULL values properly.\n");
        prompt.append("6. If the question cannot be answered with the given schema, respond exactly with: 'INSUFFICIENT_DATA'.\n\n");

        prompt.append("EXAMPLES:\n");
        prompt.append("Schema: TABLE customers (id INT, name STRING, city STRING)\n");
        prompt.append("Question: How many customers are there?\n");
        prompt.append("SQL: SELECT COUNT(*) AS customer_count FROM customers;\n\n");

        prompt.append("Schema: TABLE orders (id INT, customer_id INT, amount DOUBLE, order_date DATE)\n");
        prompt.append("Question: What's the total revenue last month?\n");
        prompt.append("SQL: SELECT SUM(amount) AS total_revenue FROM orders WHERE YEAR(order_date) = YEAR(CURRENT_DATE()) AND MONTH(order_date) = MONTH(CURRENT_DATE()) - 1;\n\n");

        prompt.append("Schema: TABLE employees (id INT, name STRING, department STRING)\n");
        prompt.append("Question: Insert a new employee with id 10, name 'John Doe', department 'Sales'\n");
        prompt.append("SQL: INSERT INTO employees (id, name, department) VALUES (10, 'John Doe', 'Sales');\n\n");

        prompt.append("Schema: TABLE customers (id INT, name STRING), TABLE orders (id INT, customer_id INT, amount DOUBLE)\n");
        prompt.append("Question: Get the total amount ordered by each customer with their name\n");
        prompt.append("SQL: SELECT c.name, SUM(o.amount) AS total_amount FROM customers c INNER JOIN orders o ON c.id = o.customer_id GROUP BY c.name;\n\n");

        prompt.append("DATABASE SCHEMA:\n");
        prompt.append(context);
        prompt.append("\n\nQUESTION: ");
        prompt.append(question);
        prompt.append("\n\nSPARK SQL QUERY:");

        return prompt.toString();
    }

    /**
     * Retry generating SQL up to maxRetries times if the model fails or returns empty.
     */
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
                    try { Thread.sleep(1000 * attempt); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); break; }
                }
            }
        }
        throw new RuntimeException("Failed to generate SQL after " + maxRetries + " attempts", lastException);
    }

    /**
     * Extracts the textual SQL content from the model's response.
     * Supports String or objects with a 'content()' method.
     */
    private String extractContentFromResponse(Object response) {
        if (response instanceof String) {
            return (String) response;
        }
        try {
            var method = response.getClass().getMethod("content");
            return (String) method.invoke(response);
        } catch (Exception e) {
            return response.toString();
        }
    }

    /**
     * Cleans the SQL output from the model by removing unwanted tokens, comments,
     * code fences, explanations, and normalizes whitespace.
     * Also extracts the first SQL statement found.
     * Converts 'INSUFFICIENT_DATA' to a standard SQL message query.
     */
    private String cleanSQL(String sql) {
        if (sql == null || sql.trim().isEmpty()) {
            return "";
        }

        String cleaned = sql;
        cleaned = cleaned.replaceAll("(?is)<think>.*?</think>", "");
        cleaned = cleaned.replaceAll("(?s)/\\*.*?\\*/", "");
        cleaned = cleaned.replaceAll("--.*", "");
        cleaned = cleaned.replaceAll("(?s)```sql\\s*", "");
        cleaned = cleaned.replaceAll("(?s)```\\s*", "");
        cleaned = cleaned.replaceAll("(?im)^(Explanation|Answer|Question|SQL|Query):.*$", "");
        cleaned = cleaned.replaceAll("<[^>]+>", "");
        cleaned = cleaned.replaceAll("\\n{2,}", "\n").trim();

        if (cleaned.contains("INSUFFICIENT_DATA")) {
            return "SELECT 'INSUFFICIENT_DATA' AS message;";
        }

        if (cleaned.contains(";")) {
            // Extract first SQL statement before semicolon
            int endIndex = cleaned.indexOf(";") + 1;
            cleaned = cleaned.substring(0, endIndex);
        } else if (cleaned.contains("SELECT") || cleaned.contains("INSERT") || cleaned.contains("UPDATE") || cleaned.contains("DELETE") ||
                   cleaned.contains("CREATE") || cleaned.contains("ALTER") || cleaned.contains("DROP") || cleaned.contains("TRUNCATE")) {
            // Try to extract from first keyword to end if no semicolon
            String[] keywords = {"SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "ALTER", "DROP", "TRUNCATE"};
            int pos = -1;
            for (String kw : keywords) {
                int idx = cleaned.toUpperCase().indexOf(kw);
                if (idx != -1 && (pos == -1 || idx < pos)) pos = idx;
            }
            if (pos != -1) cleaned = cleaned.substring(pos).trim();
        }

        // Ensure SQL ends with semicolon
        if (!cleaned.endsWith(";")) {
            cleaned += ";";
        }

        return cleaned;
    }

    /**
     * Validates that the SQL string is non-empty, starts with allowed SQL command,
     * does not contain dangerous keywords, and parentheses are balanced.
     */
    private boolean isValidSparkSQL(String sql) {
        if (sql == null || sql.trim().isEmpty()) {
            return false;
        }
        String sqlUpper = sql.toUpperCase().trim();

        if (SQL_INJECTION_PATTERN.matcher(sql).find()) {
            log.warn("SQL contains potentially dangerous operations: {}", sql);
            return false;
        }

        if (!VALID_SQL_PATTERN.matcher(sql).find()) {
            log.warn("SQL is not a valid allowed SQL statement: {}", sql);
            return false;
        }

        if (!hasMatchingParentheses(sql)) {
            log.warn("SQL has unmatched parentheses: {}", sql);
            return false;
        }

        // Additional basic checks could be added here if needed

        return true;
    }

    /**
     * Checks if parentheses in the SQL are balanced.
     */
    private boolean hasMatchingParentheses(String sql) {
        int count = 0;
        for (char c : sql.toCharArray()) {
            if (c == '(') count++;
            else if (c == ')') count--;
            if (count < 0) return false;
        }
        return count == 0;
    }

    /**
     * Generates a SQL error message wrapped in a SELECT statement
     * so it can be safely returned and displayed.
     */
    private String generateErrorSQL(String error) {
        return String.format("/* Error: %s */\nSELECT 'ERROR: %s' AS error_message;", error, error);
    }

    /**
     * Basic health check for the Ollama service by running a simple query.
     */
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
