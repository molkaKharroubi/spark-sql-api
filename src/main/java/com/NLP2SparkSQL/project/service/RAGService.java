package com.NLP2SparkSQL.project.service;

import com.NLP2SparkSQL.project.dto.QueryResponse;
import com.NLP2SparkSQL.project.utils.EmbeddingUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

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

    // Pattern for detecting complex questions that might need special handling
    private static final Pattern COMPLEX_QUESTION_PATTERN = Pattern.compile(
        "(?i)(join|combine|merge|relationship|across|between|multiple|compare|analyze|trend|pattern|correlation)"
    );

    /**
     * Process user question and generate Spark SQL using RAG approach
     * with enhanced context retrieval and validation
     */
    public QueryResponse processQuestion(String question) {
        String requestId = UUID.randomUUID().toString().substring(0, 8);
        log.info("[{}] Processing question: {}", requestId, question);

        try {
            // Validate input
            if (question == null || question.trim().isEmpty()) {
                return createErrorResponse("Question cannot be empty");
            }

            if (question.length() > maxQueryLength) {
                return createErrorResponse("Question too long (max " + maxQueryLength + " characters)");
            }

            // Enhance question preprocessing
            String processedQuestion = preprocessQuestion(question);
            log.debug("[{}] Processed question: {}", requestId, processedQuestion);

            // Generate embedding for the question
            float[] embedding = EmbeddingUtils.embed(processedQuestion);
            if (embedding == null || embedding.length == 0) {
                log.warn("[{}] Failed to generate embedding for question", requestId);
                return createErrorResponse("Failed to process question semantically");
            }

            // Retrieve relevant context with enhanced search
            String context = retrieveEnhancedContext(embedding, processedQuestion, requestId);
            log.info("[{}] Retrieved context:\n{}", requestId, context);

            // Generate SQL with enhanced context
            String sparkSql = langChainSQLService.generateSQL(context, processedQuestion);
            log.info("[{}] Generated Spark SQL:\n{}", requestId, sparkSql);

            // Post-process and validate the generated SQL
            String finalSql = postProcessSQL(sparkSql, requestId);
            
            // Create success response
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
     * Preprocess the question to improve understanding
     */
    private String preprocessQuestion(String question) {
        String processed = question.trim();
        
        // Normalize common SQL terms
        processed = processed.replaceAll("(?i)how many", "count");
        processed = processed.replaceAll("(?i)what is the total", "sum");
        processed = processed.replaceAll("(?i)what is the average", "average");
        processed = processed.replaceAll("(?i)show me", "select");
        processed = processed.replaceAll("(?i)give me", "select");
        processed = processed.replaceAll("(?i)find", "select");
        processed = processed.replaceAll("(?i)get", "select");
        
        // Add context for time-based queries
        if (processed.matches("(?i).*\\b(today|yesterday|this week|last week|this month|last month|this year|last year)\\b.*")) {
            processed += " [TIME_SENSITIVE]";
        }
        
        // Add context for aggregation queries
        if (processed.matches("(?i).*(total|sum|count|average|avg|max|min|group by).*")) {
            processed += " [AGGREGATION]";
        }
        
        // Add context for complex queries
        if (COMPLEX_QUESTION_PATTERN.matcher(processed).find()) {
            processed += " [COMPLEX]";
        }
        
        return processed;
    }

    /**
     * Retrieve enhanced context using multiple strategies
     */
    private String retrieveEnhancedContext(float[] embedding, String question, String requestId) {
        try {
            // Primary context retrieval
            String primaryContext = qdrantService.searchRelevantContext(embedding);
            
            // If primary context is insufficient, try fallback
            if ((primaryContext == null || primaryContext.trim().isEmpty()) && enableFallbackContext) {
                log.warn("[{}] Primary context retrieval failed, using fallback", requestId);
                primaryContext = getFallbackContext();
            }
            
            // Enhance context based on question type
            String enhancedContext = enhanceContextForQuestion(primaryContext, question);
            
            return enhancedContext;
            
        } catch (Exception e) {
            log.error("[{}] Error retrieving context: {}", requestId, e.getMessage(), e);
            return enableFallbackContext ? getFallbackContext() : "";
        }
    }

    /**
     * Enhance context based on question characteristics
     */
    private String enhanceContextForQuestion(String baseContext, String question) {
        StringBuilder enhanced = new StringBuilder();
        
        // Add base context
        enhanced.append(baseContext);
        
        // Add query hints based on question type
        if (question.contains("[TIME_SENSITIVE]")) {
            enhanced.append("\n\nIMPORTANT: Use appropriate date functions like CURRENT_DATE(), YEAR(), MONTH(), DAY()");
            enhanced.append("\nFor 'today': WHERE DATE(column) = CURRENT_DATE()");
            enhanced.append("\nFor 'this month': WHERE YEAR(column) = YEAR(CURRENT_DATE()) AND MONTH(column) = MONTH(CURRENT_DATE())");
        }
        
        if (question.contains("[AGGREGATION]")) {
            enhanced.append("\n\nIMPORTANT: Use GROUP BY clause when using aggregate functions with other columns");
            enhanced.append("\nAvailable aggregate functions: COUNT(), SUM(), AVG(), MAX(), MIN()");
        }
        
        if (question.contains("[COMPLEX]")) {
            enhanced.append("\n\nIMPORTANT: Consider using JOINs to combine data from multiple tables");
            enhanced.append("\nUse table aliases for readability: SELECT c.name, o.total FROM customers c JOIN orders o ON c.id = o.customer_id");
        }
        
        return enhanced.toString();
    }

    /**
     * Get fallback context when primary retrieval fails
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
     * Post-process the generated SQL for final validation and cleanup
     */
    private String postProcessSQL(String sql, String requestId) {
        if (sql == null || sql.trim().isEmpty()) {
            return createErrorSQL("Empty SQL generated");
        }

        // Additional cleaning
        String processed = sql;
        
        // Remove any remaining artifacts
        processed = processed.replaceAll("(?i)\\[TIME_SENSITIVE\\]", "");
        processed = processed.replaceAll("(?i)\\[AGGREGATION\\]", "");
        processed = processed.replaceAll("(?i)\\[COMPLEX\\]", "");
        
        // Format SQL nicely
        processed = formatSQL(processed);
        
        // Final validation if enabled
        if (enableSQLValidation && !isValidSQL(processed)) {
            log.warn("[{}] Generated SQL failed final validation: {}", requestId, processed);
            return createErrorSQL("Generated SQL failed validation");
        }
        
        return processed;
    }

    /**
     * Format SQL for better readability
     */
    private String formatSQL(String sql) {
        if (sql == null) return "";
        
        String formatted = sql;
        
        // Add line breaks for readability
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
        
        // Clean up extra whitespace
        formatted = formatted.replaceAll("\\n+", "\n").trim();
        
        return formatted;
    }

    /**
     * Basic SQL validation
     */
    private boolean isValidSQL(String sql) {
        if (sql == null || sql.trim().isEmpty()) {
            return false;
        }
        
        String sqlUpper = sql.toUpperCase().trim();
        
        // Must start with SELECT
        if (!sqlUpper.startsWith("SELECT")) {
            return false;
        }
        
        // Must contain FROM
        if (!sqlUpper.contains("FROM")) {
            return false;
        }
        
        // Should end with semicolon
        if (!sql.trim().endsWith(";")) {
            return false;
        }
        
        return true;
    }

    /**
     * Create error response
     */
    private QueryResponse createErrorResponse(String error) {
        return new QueryResponse(
            createErrorSQL(error),
            "Error: " + error
        );
    }

    /**
     * Create error SQL
     */
    private String createErrorSQL(String error) {
        return String.format("/* Error: %s */\nSELECT 'ERROR: %s' AS error_message;", error, error);
    }

    /**
     * Health check for the RAG service
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