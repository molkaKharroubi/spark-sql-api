package com.NLP2SparkSQL.project.service;

import com.NLP2SparkSQL.project.dto.QueryResponse;
import com.NLP2SparkSQL.project.utils.EmbeddingUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.UUID;
import java.util.regex.Pattern;

@Slf4j
@Service
@RequiredArgsConstructor
public class RAGService {

    private final QdrantService qdrantService;
    private final LangChainSQLService langChainSQLService;

    // Maximum allowed length of the user question
    @Value("${app.max-query-length:10000}")
    private int maxQueryLength;

    // Enable or disable fallback context usage if primary retrieval fails
    @Value("${app.enable-fallback-context:true}")
    private boolean enableFallbackContext;

    // Enable or disable SQL validation after generation
    @Value("${app.enable-sql-validation:true}")
    private boolean enableSQLValidation;

    // Regex pattern to detect complex questions requiring special handling
    private static final Pattern COMPLEX_QUESTION_PATTERN = Pattern.compile(
        "(?i)(join|combine|merge|relationship|across|between|multiple|compare|analyze|trend|pattern|correlation)"
    );

    /**
     * Main method to process user question and generate a Spark SQL query.
     * It uses a Retrieval-Augmented Generation (RAG) approach:
     * - Generate embedding of the question
     * - Retrieve relevant schema context from Qdrant vector database
     * - Generate SQL query with LangChain model
     * - Post-process and validate SQL query
     *
     * @param question User's natural language question
     * @return QueryResponse containing generated SQL and status message
     */
    public QueryResponse processQuestion(String question) {
        String requestId = UUID.randomUUID().toString().substring(0, 8);
        log.info("[{}] Processing question: {}", requestId, question);

        try {
            // Validate the input question
            if (question == null || question.trim().isEmpty()) {
                return createErrorResponse("Question cannot be empty");
            }

            if (question.length() > maxQueryLength) {
                return createErrorResponse("Question too long (max " + maxQueryLength + " characters)");
            }

            // Preprocess question to normalize and enrich it with metadata
            String processedQuestion = preprocessQuestion(question);
            log.debug("[{}] Processed question: {}", requestId, processedQuestion);

            // Generate embedding vector from processed question
            float[] embedding = EmbeddingUtils.embed(processedQuestion);
            if (embedding == null || embedding.length == 0) {
                log.warn("[{}] Failed to generate embedding for question", requestId);
                return createErrorResponse("Failed to process question semantically");
            }

            // Retrieve relevant schema context from Qdrant, with fallback support
            String context = retrieveEnhancedContext(embedding, processedQuestion, requestId);
            log.info("[{}] Retrieved context:\n{}", requestId, context);

            // Generate Spark SQL query from retrieved context and question
            String sparkSql = langChainSQLService.generateSQL(context, processedQuestion);
            log.info("[{}] Generated Spark SQL:\n{}", requestId, sparkSql);

            // Post-process generated SQL: cleanup, format, validate
            String finalSql = postProcessSQL(sparkSql, requestId);

            // Prepare and return response with SQL and status message
            QueryResponse response = new QueryResponse(
                finalSql,
                sparkSql.contains("ERROR:") ? "Error in SQL generation" : "SQL generated successfully"
            );

            log.info("[{}] Successfully processed question", requestId);
            return response;

        } catch (Exception e) {
            log.error("[{}] Failed to process question: {}", requestId, e.getMessage(), e);
            return createErrorResponse("Internal error: " + e.getMessage());
        }
    }

    /**
     * Preprocess the user question for better semantic understanding.
     * It normalizes some SQL keywords and adds tags for query type hints.
     *
     * @param question Raw user question
     * @return Processed and tagged question string
     */
    private String preprocessQuestion(String question) {
        String processed = question.trim();

        // Normalize common SQL-related phrases to keywords
        processed = processed.replaceAll("(?i)how many", "count");
        processed = processed.replaceAll("(?i)what is the total", "sum");
        processed = processed.replaceAll("(?i)what is the average", "average");
        processed = processed.replaceAll("(?i)show me", "select");
        processed = processed.replaceAll("(?i)give me", "select");
        processed = processed.replaceAll("(?i)find", "select");
        processed = processed.replaceAll("(?i)get", "select");

        // Tag for time-sensitive queries to add context in prompt
        if (processed.matches("(?i).*\\b(today|yesterday|this week|last week|this month|last month|this year|last year)\\b.*")) {
            processed += " [TIME_SENSITIVE]";
        }

        // Tag for aggregation queries (sum, count, average, etc.)
        if (processed.matches("(?i).*(total|sum|count|average|avg|max|min|group by).*")) {
            processed += " [AGGREGATION]";
        }

        // Tag complex queries that may require joins and multi-table logic
        if (COMPLEX_QUESTION_PATTERN.matcher(processed).find()) {
            processed += " [COMPLEX]";
        }

        return processed;
    }

    /**
     * Retrieve enhanced context using the embedding vector and question tags.
     * Uses primary Qdrant search and fallback static schema if enabled.
     * Enhances context with usage hints based on question characteristics.
     *
     * @param embedding Semantic vector of the question
     * @param question Preprocessed question string
     * @param requestId Unique ID for logging correlation
     * @return Enhanced schema context string
     */
    private String retrieveEnhancedContext(float[] embedding, String question, String requestId) {
        try {
            // Query Qdrant vector DB for relevant schema documents
            String primaryContext = qdrantService.searchRelevantContext(embedding);

            // Use fallback static context if primary retrieval fails and fallback enabled
            if ((primaryContext == null || primaryContext.trim().isEmpty()) && enableFallbackContext) {
                log.warn("[{}] Primary context retrieval failed, using fallback", requestId);
                primaryContext = getFallbackContext();
            }

            // Add usage hints and clarifications to context based on question tags
            String enhancedContext = enhanceContextForQuestion(primaryContext, question);

            return enhancedContext;

        } catch (Exception e) {
            log.error("[{}] Error retrieving context: {}", requestId, e.getMessage(), e);
            return enableFallbackContext ? getFallbackContext() : "";
        }
    }

    /**
     * Enhance the base context with hints and instructions to help the LLM generate better SQL.
     *
     * @param baseContext Retrieved or fallback schema context
     * @param question Preprocessed question with tags
     * @return Context augmented with usage hints
     */
    private String enhanceContextForQuestion(String baseContext, String question) {
        StringBuilder enhanced = new StringBuilder();

        // Append base schema context
        enhanced.append(baseContext);

        // Add instructions for time-sensitive queries
        if (question.contains("[TIME_SENSITIVE]")) {
            enhanced.append("\n\nIMPORTANT: Use appropriate date functions like CURRENT_DATE(), YEAR(), MONTH(), DAY()");
            enhanced.append("\nFor 'today': WHERE DATE(column) = CURRENT_DATE()");
            enhanced.append("\nFor 'this month': WHERE YEAR(column) = YEAR(CURRENT_DATE()) AND MONTH(column) = MONTH(CURRENT_DATE())");
        }

        // Add hints for aggregation queries
        if (question.contains("[AGGREGATION]")) {
            enhanced.append("\n\nIMPORTANT: Use GROUP BY clause when using aggregate functions with other columns");
            enhanced.append("\nAvailable aggregate functions: COUNT(), SUM(), AVG(), MAX(), MIN()");
        }

        // Add guidance for complex multi-table queries involving joins
        if (question.contains("[COMPLEX]")) {
            enhanced.append("\n\nIMPORTANT: Consider using JOINs to combine data from multiple tables");
            enhanced.append("\nUse table aliases for readability: SELECT c.name, o.total FROM customers c JOIN orders o ON c.id = o.customer_id");
        }

        return enhanced.toString();
    }

    /**
     * Returns a static fallback schema context as a last resort.
     * Helps ensure SQL generation can proceed with minimal info.
     */
    private String getFallbackContext() {
        return """
                -- FALLBACK DATABASE SCHEMA --
                
                TABLE: customers
                COLUMNS: id (INT), name (STRING), email (STRING), city (STRING), country (STRING), created_date (DATE)
                DESCRIPTION: Customer information and demographics
                
                TABLE: orders
                COLUMNS: id (INT), customer_id (INT), total_amount (DOUBLE), order_date (DATE), status (STRING)
                DESCRIPTION: Order transactions and history
                
                TABLE: products
                COLUMNS: id (INT), name (STRING), category (STRING), price (DOUBLE), stock_quantity (INT)
                DESCRIPTION: Product catalog and inventory
                
                TABLE: order_items
                COLUMNS: id (INT), order_id (INT), product_id (INT), quantity (INT), unit_price (DOUBLE)
                DESCRIPTION: Individual items within orders
                
                RELATIONSHIPS:
                - orders.customer_id -> customers.id
                - order_items.order_id -> orders.id
                - order_items.product_id -> products.id
                
                COMMON PATTERNS:
                - Join customers with orders: customers c JOIN orders o ON c.id = o.customer_id
                - Join orders with items: orders o JOIN order_items oi ON o.id = oi.order_id
                - Full sales analysis: customers c JOIN orders o ON c.id = o.customer_id JOIN order_items oi ON o.id = oi.order_id JOIN products p ON oi.product_id = p.id
                """;
    }

    /**
     * Post-processes the generated SQL:
     * - Cleans special tags from preprocessing
     * - Formats SQL for readability
     * - Validates SQL if enabled
     *
     * @param sql Raw SQL generated by LangChain model
     * @param requestId Unique ID for logging
     * @return Final validated and formatted SQL or error SQL
     */
    private String postProcessSQL(String sql, String requestId) {
        if (sql == null || sql.trim().isEmpty()) {
            return createErrorSQL("Empty SQL generated");
        }

        String processed = sql;

        // Remove preprocessing tags from SQL text
        processed = processed.replaceAll("(?i)\\[TIME_SENSITIVE\\]", "");
        processed = processed.replaceAll("(?i)\\[AGGREGATION\\]", "");
        processed = processed.replaceAll("(?i)\\[COMPLEX\\]", "");

        // Format SQL nicely with line breaks and indentation
        processed = formatSQL(processed);

        // Validate SQL if validation is enabled
        if (enableSQLValidation && !isValidSQL(processed)) {
            log.warn("[{}] Generated SQL failed final validation: {}", requestId, processed);
            return createErrorSQL("Generated SQL failed validation");
        }

        return processed;
    }

    /**
     * Basic SQL formatting for improved readability.
     *
     * @param sql Raw SQL string
     * @return Formatted SQL string with line breaks before keywords
     */
    private String formatSQL(String sql) {
        if (sql == null) return "";

        String formatted = sql;

        // Insert line breaks before main SQL keywords for readability
        formatted = formatted.replaceAll("(?i)\\bSELECT\\b", "\nSELECT");
        formatted = formatted.replaceAll("(?i)\\bFROM\\b", "\nFROM");
        formatted = formatted.replaceAll("(?i)\\bWHERE\\b", "\nWHERE");
        formatted = formatted.replaceAll("(?i)\\bGROUP BY\\b", "\nGROUP BY");
        formatted = formatted.replaceAll("(?i)\\bHAVING\\b", "\nHAVING");
        formatted = formatted.replaceAll("(?i)\\bORDER BY\\b", "\nORDER BY");
        formatted = formatted.replaceAll("(?i)\\bJOIN\\b", "\nJOIN");
        formatted = formatted.replaceAll("(?i)\\bLEFT JOIN\\b", "\nLEFT JOIN");
        formatted = formatted.replaceAll("(?i)\\bRIGHT JOIN\\b", "\nRIGHT JOIN");
        formatted = formatted.replaceAll("(?i)\\bINNER JOIN\\b", "\nINNER JOIN");

        // Remove extra empty lines
        formatted = formatted.replaceAll("\\n+", "\n").trim();

        return formatted;
    }

    /**
     * Basic validation to check if SQL is likely valid:
     * - Starts with SELECT
     * - Contains FROM
     * - Ends with semicolon
     *
     * @param sql SQL string to validate
     * @return true if valid, false otherwise
     */
    private boolean isValidSQL(String sql) {
        if (sql == null || sql.trim().isEmpty()) {
            return false;
        }

        String sqlUpper = sql.toUpperCase().trim();

        if (!sqlUpper.startsWith("SELECT")) {
            return false;
        }

        if (!sqlUpper.contains("FROM")) {
            return false;
        }

        if (!sql.trim().endsWith(";")) {
            return false;
        }

        return true;
    }

    /**
     * Create a QueryResponse indicating an error with an appropriate SQL message
     *
     * @param error Error message string
     * @return QueryResponse with error SQL and message
     */
    private QueryResponse createErrorResponse(String error) {
        return new QueryResponse(
            createErrorSQL(error),
            "Error: " + error
        );
    }

    /**
     * Create an error SQL query string for reporting failures gracefully
     *
     * @param error Error message string
     * @return SQL string that returns the error message in query results
     */
    private String createErrorSQL(String error) {
        return String.format("/* Error: %s */\nSELECT 'ERROR: %s' AS error_message;", error, error);
    }

    /**
     * Health check method to verify dependencies are healthy
     *
     * @return true if both Qdrant and LangChain services are healthy
     */
    public boolean isHealthy() {
        try {
            return qdrantService.isHealthy() && langChainSQLService.isHealthy();
        } catch (Exception e) {
            log.error("RAG service health check failed: {}", e.getMessage());
            return false;
        }
    }
}
