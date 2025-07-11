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
    
    // SQL validation patterns
    private static final Pattern SQL_INJECTION_PATTERN = Pattern.compile(
        "(?i)(DROP|DELETE|ALTER|TRUNCATE|INSERT|UPDATE|CREATE|EXEC|EXECUTE|UNION|SCRIPT|DECLARE|MERGE)\\s", 
        Pattern.CASE_INSENSITIVE
    );
    
    private static final Pattern VALID_SQL_PATTERN = Pattern.compile(
        "^\\s*SELECT\\s+.*\\s+FROM\\s+.*", 
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
                .temperature(0.1) // Lower temperature for more deterministic outputs
                .timeout(java.time.Duration.ofSeconds(120))
                .build();
    }

    /**
     * Generate a Spark SQL query based on context and user question.
     * Uses advanced prompt engineering and validation techniques.
     *
     * @param context Database schema (tables, columns, types)
     * @param question User's natural language question
     * @return Generated SQL query or error message
     */
    public String generateSQL(String context, String question) {
        String requestId = java.util.UUID.randomUUID().toString().substring(0, 8);
        log.info("[{}] Generating SQL for question: {}", requestId, question);

        try {
            // Validate inputs
            if (context == null || context.trim().isEmpty()) {
                return generateErrorSQL("No database context provided");
            }
            
            if (question == null || question.trim().isEmpty()) {
                return generateErrorSQL("No question provided");
            }

            // Build enhanced prompt with few-shot examples
            String prompt = buildEnhancedPrompt(context, question);
            
            // Generate SQL with retry mechanism
            String generatedSQL = generateWithRetry(prompt, 3);
            
            // Clean and validate the generated SQL
            String cleanedSQL = cleanSQL(generatedSQL);
            
            // Validate the SQL structure
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
     * Build an enhanced prompt with few-shot examples and clear instructions
     */
    private String buildEnhancedPrompt(String context, String question) {
        StringBuilder prompt = new StringBuilder();
        
        prompt.append("You are a Spark SQL expert. Generate ONLY valid Spark SQL queries based on the provided schema.\n\n");
        
        // Add strict rules
        prompt.append("RULES:\n");
        prompt.append("1. Use ONLY tables and columns mentioned in the schema\n");
        prompt.append("2. Generate only SELECT statements - no INSERT, UPDATE, DELETE, DROP\n");
        prompt.append("3. Use proper Spark SQL syntax and functions\n");
        prompt.append("4. Include appropriate WHERE clauses, JOINs, and GROUP BY when needed\n");
        prompt.append("5. Use aliases for readability\n");
        prompt.append("6. Handle NULL values appropriately\n");
        prompt.append("7. If the question cannot be answered with the given schema, respond with: 'INSUFFICIENT_DATA'\n\n");
        
        // Add few-shot examples
        prompt.append("EXAMPLES:\n");
        prompt.append("Schema: TABLE customers (id INT, name STRING, city STRING)\n");
        prompt.append("Question: How many customers are there?\n");
        prompt.append("SQL: SELECT COUNT(*) AS customer_count FROM customers;\n\n");
        
        prompt.append("Schema: TABLE orders (id INT, customer_id INT, amount DOUBLE, order_date DATE)\n");
        prompt.append("Question: What's the total revenue last month?\n");
        prompt.append("SQL: SELECT SUM(amount) AS total_revenue FROM orders WHERE YEAR(order_date) = YEAR(CURRENT_DATE()) AND MONTH(order_date) = MONTH(CURRENT_DATE()) - 1;\n\n");
        
        // Add the actual schema and question
        prompt.append("DATABASE SCHEMA:\n");
        prompt.append(context);
        prompt.append("\n\nQUESTION: ");
        prompt.append(question);
        prompt.append("\n\nSPARK SQL QUERY:");
        
        return prompt.toString();
    }

    /**
     * Generate SQL with retry mechanism for better reliability
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
                    try {
                        Thread.sleep(1000 * attempt); // Exponential backoff
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }
        
        throw new RuntimeException("Failed to generate SQL after " + maxRetries + " attempts", lastException);
    }

    /**
     * Extract content from different types of responses
     */
    private String extractContentFromResponse(Object response) {
        if (response instanceof String) {
            return (String) response;
        } else {
            try {
                // Try to get content via reflection
                var method = response.getClass().getMethod("content");
                return (String) method.invoke(response);
            } catch (Exception e) {
                // Fallback to toString
                return response.toString();
            }
        }
    }

    /**
     * Clean and format the generated SQL
     */
    private String cleanSQL(String sql) {
        if (sql == null || sql.trim().isEmpty()) {
            return "";
        }

        String cleaned = sql;
        
        // Remove thinking blocks and comments
        cleaned = cleaned.replaceAll("(?is)<think>.*?</think>", "");
        cleaned = cleaned.replaceAll("(?s)/\\*.*?\\*/", "");
        cleaned = cleaned.replaceAll("--.*", "");
        
        // Remove code blocks
        cleaned = cleaned.replaceAll("(?s)```sql\\s*", "");
        cleaned = cleaned.replaceAll("(?s)```\\s*", "");
        
        // Remove explanation text
        cleaned = cleaned.replaceAll("(?im)^(Explanation|Answer|Question|SQL|Query):.*$", "");
        
        // Remove HTML tags
        cleaned = cleaned.replaceAll("<[^>]+>", "");
        
        // Clean up whitespace
        cleaned = cleaned.replaceAll("\\n{2,}", "\n").trim();
        
        // Handle special responses
        if (cleaned.contains("INSUFFICIENT_DATA")) {
            return "SELECT 'INSUFFICIENT_DATA' AS message;";
        }
        
        // Extract SQL from mixed content
        if (cleaned.contains("SELECT")) {
            int selectIndex = cleaned.indexOf("SELECT");
            cleaned = cleaned.substring(selectIndex);
        }
        
        // Ensure proper semicolon
        if (!cleaned.endsWith(";")) {
            cleaned += ";";
        }
        
        return cleaned;
    }

    /**
     * Validate if the generated SQL is safe and valid for Spark
     */
    private boolean isValidSparkSQL(String sql) {
        if (sql == null || sql.trim().isEmpty()) {
            return false;
        }
        
        String sqlUpper = sql.toUpperCase().trim();
        
        // Check for dangerous operations
        if (SQL_INJECTION_PATTERN.matcher(sql).find()) {
            log.warn("SQL contains potentially dangerous operations: {}", sql);
            return false;
        }
        
        // Must be a SELECT statement
        if (!VALID_SQL_PATTERN.matcher(sql).find()) {
            log.warn("SQL is not a valid SELECT statement: {}", sql);
            return false;
        }
        
        // Basic syntax validation
        if (!hasMatchingParentheses(sql)) {
            log.warn("SQL has unmatched parentheses: {}", sql);
            return false;
        }
        
        // Check for required keywords
        if (!sqlUpper.contains("SELECT") || !sqlUpper.contains("FROM")) {
            log.warn("SQL missing required keywords: {}", sql);
            return false;
        }
        
        return true;
    }

    /**
     * Check if parentheses are balanced in the SQL
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
     * Generate error SQL with proper formatting
     */
    private String generateErrorSQL(String error) {
        return String.format("/* Error: %s */\nSELECT 'ERROR: %s' AS error_message;", error, error);
    }

    /**
     * Health check for the Ollama service
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