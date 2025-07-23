package com.NLP2SparkSQL.project.service;

import dev.langchain4j.model.ollama.OllamaLanguageModel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;

import java.time.Duration;
import java.util.regex.Pattern;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

@Slf4j
@Service 
@Getter
public class LangChainSQLService {

    private final OllamaLanguageModel model;
    private final long timeoutSeconds;

    private static final Pattern FORBIDDEN_PATTERN = Pattern.compile(
            "(?i)(SCRIPT|DECLARE|\\bEXEC\\b|\\bSP_\\b|\\bXP_\\b|--|;\\s*--)", Pattern.CASE_INSENSITIVE
    );

    public LangChainSQLService(
            @Value("${ollama.url:http://localhost:11434}") String ollamaUrl,
            @Value("${ollama.model:qwen3:1.7b}") String modelName,
            @Value("${ollama.timeout:600}") long timeoutSeconds
    ) {
        this.timeoutSeconds = timeoutSeconds;
        log.info("Initializing Ollama model with URL: {}, model: {}, timeout: {}s", 
                ollamaUrl, modelName, timeoutSeconds);

        // Configure system properties for HTTP timeouts
        configureSystemHttpProperties();

        // Build the model with timeout configuration
        this.model = OllamaLanguageModel.builder()
                .baseUrl(ollamaUrl)
                .modelName(modelName)
                .temperature(0.1)
                .timeout(Duration.ofSeconds(timeoutSeconds))
                .maxRetries(3)
                .build();

        log.info("Ollama model configured successfully");
    }

    private void configureSystemHttpProperties() {
        // Configure system-wide HTTP timeouts
        System.setProperty("http.keepAlive", "true");
        System.setProperty("http.maxConnections", "10");
        System.setProperty("http.maxRedirects", "3");
        
        // Configure connection and read timeouts
        System.setProperty("sun.net.client.defaultConnectTimeout", "30000");
        System.setProperty("sun.net.client.defaultReadTimeout", String.valueOf(timeoutSeconds * 1000));
        
        log.debug("System HTTP properties configured with timeout: {}s", timeoutSeconds);
    }

    @EventListener(ApplicationReadyEvent.class)
    public void onApplicationReady() {
        log.info("Application ready, testing Ollama connection...");
        try {
            testConnection();
            log.info("Ollama connection test successful");
        } catch (Exception e) {
            log.error("Ollama connection test failed: {}", e.getMessage());
        }
    }

    private void testConnection() {
        try {
            String testPrompt = "SELECT 1 as test;";
            
            // Use CompletableFuture for timeout control
            CompletableFuture<String> future = CompletableFuture.supplyAsync(() -> {
                try {
                    return model.generate(testPrompt).content();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            
            String response = future.get(30, TimeUnit.SECONDS);
            log.debug("Test connection response: {}", response);
            
        } catch (TimeoutException e) {
            log.error("Connection test timed out after 30 seconds");
            throw new RuntimeException("Connection test timeout", e);
        } catch (Exception e) {
            log.error("Connection test failed: {}", e.getMessage());
            throw new RuntimeException("Connection test failed", e);
        }
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
            String generatedSQL = generateWithTimeoutControl(prompt, requestId);
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

    private String generateWithTimeoutControl(String prompt, String requestId) {
        int maxRetries = 3;
        Exception lastException = null;

        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                log.debug("[{}] SQL generation attempt {} of {}", requestId, attempt, maxRetries);
                
                if (attempt > 1) {
                    log.info("[{}] Retrying SQL generation, attempt {} of {}", requestId, attempt, maxRetries);
                }
                
                // Use CompletableFuture for better timeout control
                CompletableFuture<String> future = CompletableFuture.supplyAsync(() -> {
                    try {
                        long startTime = System.currentTimeMillis();
                        String response = model.generate(prompt).content();
                        long endTime = System.currentTimeMillis();
                        
                        log.debug("[{}] Generation took {} ms", requestId, endTime - startTime);
                        return response;
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });
                
                String response = future.get(timeoutSeconds, TimeUnit.SECONDS);
                String result = extractContentFromResponse(response);

                if (result != null && !result.trim().isEmpty()) {
                    log.debug("[{}] Successfully generated SQL on attempt {}", requestId, attempt);
                    return result;
                }
                
                log.warn("[{}] Empty response received on attempt {}", requestId, attempt);
                
            } catch (TimeoutException e) {
                lastException = e;
                log.error("[{}] Timeout occurred during SQL generation attempt {} ({}s)", 
                         requestId, attempt, timeoutSeconds);
                
            } catch (ExecutionException e) {
                lastException = e.getCause() instanceof Exception ? (Exception) e.getCause() : e;
                log.warn("[{}] SQL generation attempt {} failed: {}", requestId, attempt, lastException.getMessage());
                
            } catch (Exception e) {
                lastException = e;
                log.warn("[{}] SQL generation attempt {} failed: {}", requestId, attempt, e.getMessage());
            }
                
            if (attempt < maxRetries) {
                try {
                    // Exponential backoff
                    long sleepTime = 2000 * (long) Math.pow(2, attempt - 1);
                    log.debug("[{}] Sleeping for {} ms before retry", requestId, sleepTime);
                    Thread.sleep(sleepTime);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    log.warn("[{}] Retry interrupted", requestId);
                    break;
                }
            }
        }
        
        String errorMessage = String.format("[%s] Failed to generate SQL after %d attempts", requestId, maxRetries);
        if (lastException != null) {
            errorMessage += ": " + lastException.getMessage();
        }
        
        throw new RuntimeException(errorMessage, lastException);
    }

    private String buildEnhancedPrompt(String context, String question) {
    return """
        You are an expert Spark SQL generator. Generate ONLY valid Spark SQL queries based on the provided schema.

        CRITICAL RULES:
        1. Use ONLY tables and columns mentioned in the schema
        2. Do not invent or assume any table/column names
        3. Always use proper Spark SQL syntax
        4. For questions about distribution/grouping, use GROUP BY with appropriate aggregation
        5. When joining tables, use explicit JOIN syntax with ON conditions
        6. Use table aliases for better readability
        7. Return only the SQL query, no explanations
        8. End the query with semicolon
        9. Keep the query concise and focused

        SPARK SQL FEATURES TO USE:
        - Window functions: ROW_NUMBER(), RANK(), DENSE_RANK() OVER (...)
        - Common Table Expressions (WITH clause)
        - Aggregations: COUNT(), SUM(), AVG(), MAX(), MIN()
        - Joins: INNER JOIN, LEFT JOIN, RIGHT JOIN
        - Grouping: GROUP BY, HAVING
        - Ordering: ORDER BY
        - Use LEFT JOIN to include rows with no matching related records
        - For filtering by date, use BETWEEN with inclusive dates

        COMPLEX QUERY TIPS:
        - For filters on minimum number of reviews and average ratings, use HAVING clauses.
        - When combining aggregated data from multiple tables (e.g., reviews and orders), use CTEs (WITH clauses) to compute intermediate results, then join them.
        - Always handle possible missing data with COALESCE().

        EXAMPLES:
        Schema: employees (emp_id INT, name STRING, department_id INT), departments (department_id INT, dept_name STRING)
        Question: "List employees by department"
        SQL: SELECT d.dept_name, e.name FROM employees e JOIN departments d ON e.department_id = d.department_id ORDER BY d.dept_name;

        Schema: sales (id INT, amount DOUBLE, region STRING)
        Question: "Show sales distribution by region"
        SQL: SELECT region, COUNT(*) as count, SUM(amount) as total_amount FROM sales GROUP BY region ORDER BY total_amount DESC;

        Schema: products (product_id INT, product_name STRING, category STRING),
                reviews (review_id INT, product_id INT, rating DOUBLE, review_date DATE),
                orders (order_id INT, product_id INT, quantity INT, order_date DATE)
        Question: "For each product with at least 5 reviews and average rating > 4.5 in 2023, show product name, category, average rating, number of reviews, and total orders in 2023 ordered by rating desc"
        SQL: |
            WITH ReviewStats AS (
                SELECT product_id, COUNT(*) AS total_reviews, AVG(rating) AS avg_rating
                FROM reviews
                WHERE review_date BETWEEN '2023-01-01' AND '2023-12-31'
                GROUP BY product_id
                HAVING COUNT(*) >= 5 AND AVG(rating) > 4.5
            ),
            OrderStats AS (
                SELECT product_id, SUM(quantity) AS total_orders
                FROM orders
                WHERE order_date BETWEEN '2023-01-01' AND '2023-12-31'
                GROUP BY product_id
            )
            SELECT p.product_name, p.category, r.avg_rating, r.total_reviews, COALESCE(o.total_orders, 0) AS total_orders
            FROM products p
            JOIN ReviewStats r ON p.product_id = r.product_id
            LEFT JOIN OrderStats o ON p.product_id = o.product_id
            ORDER BY r.avg_rating DESC, total_orders DESC;

        DATABASE SCHEMA:
        """ + context + """

        QUESTION:
        """ + question + """

        SPARK SQL QUERY:
        """;
}


    private String extractContentFromResponse(Object response) {
        if (response == null) {
            return "";
        }
        
        if (response instanceof String) {
            return (String) response;
        } else {
            try {
                // Try to call content() method if it exists
                var method = response.getClass().getMethod("content");
                Object content = method.invoke(response);
                return content != null ? content.toString() : "";
            } catch (Exception e) {
                log.debug("Could not extract content using content() method, using toString(): {}", e.getMessage());
                return response.toString();
            }
        }
    }

    private String cleanSQL(String sql) {
        if (sql == null || sql.trim().isEmpty()) return "";

        // Initial cleanup - remove comments, code blocks, etc.
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

        // Better handling of CTEs and regular queries
        String sqlUpper = cleaned.toUpperCase().trim();
        
        // Find the first valid SQL keyword
        String[] validStarts = {"WITH", "SELECT", "INSERT", "UPDATE", "DELETE", "CREATE", "DROP","ALTER TABLE"};
        int earliestIndex = -1;
        
        for (String keyword : validStarts) {
            int index = sqlUpper.indexOf(keyword);
            if (index != -1 && (earliestIndex == -1 || index < earliestIndex)) {
                earliestIndex = index;
            }
        }
        
        // If we find a valid keyword, start from there
        if (earliestIndex != -1) {
            cleaned = cleaned.substring(earliestIndex);
        }

        // Clean up multiple spaces
        cleaned = cleaned.replaceAll("\\s+", " ");

        // Ensure the query ends with semicolon
        if (!cleaned.endsWith(";")) {
            cleaned += ";";
        }

        return cleaned;
    }

    private boolean isValidSparkSQL(String sql) {
        if (sql == null || sql.trim().isEmpty()) return false;

        String sqlUpper = sql.trim().toUpperCase();

        boolean validStart = sqlUpper.startsWith("SELECT") ||
                             sqlUpper.startsWith("INSERT") ||
                             sqlUpper.startsWith("UPDATE") ||
                             sqlUpper.startsWith("DELETE") ||
                             sqlUpper.startsWith("CREATE") ||
                             sqlUpper.startsWith("DROP") ||
                             sqlUpper.startsWith("ALTERA") ||
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
            log.debug("Performing health check...");
            
            CompletableFuture<String> healthFuture = CompletableFuture.supplyAsync(() -> {
                return generateSQL(
                        "TABLE test (id INT, name STRING)",
                        "How many records are there?"
                );
            });
            
            String testResponse = healthFuture.get(30, TimeUnit.SECONDS);
            boolean healthy = testResponse != null && !testResponse.contains("ERROR:");
            log.debug("Health check result: {}", healthy);
            return healthy;
            
        } catch (Exception e) {
            log.error("Health check failed: {}", e.getMessage());
            return false;
        }
    }
}