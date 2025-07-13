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

    @Value("${app.max-query-length:10000}")
    private int maxQueryLength;

    @Value("${app.enable-fallback-context:true}")
    private boolean enableFallbackContext;

    @Value("${app.enable-sql-validation:true}")
    private boolean enableSQLValidation;

    private static final Pattern COMPLEX_QUESTION_PATTERN = Pattern.compile(
        "(?i)(join|combine|merge|relationship|across|between|multiple|compare|analyze|trend|pattern|correlation)"
    );

    public QueryResponse processQuestion(String question) {
        String requestId = UUID.randomUUID().toString().substring(0, 8);
        log.info("[{}] Processing question: {}", requestId, question);

        if (question == null || question.trim().isEmpty()) {
            return createErrorResponse("Question cannot be empty");
        }
        if (question.length() > maxQueryLength) {
            return createErrorResponse("Question too long (max " + maxQueryLength + " characters)");
        }

        String processedQuestion = preprocessQuestion(question);
        float[] embedding = EmbeddingUtils.embed(processedQuestion);
        if (embedding == null || embedding.length == 0) {
            return createErrorResponse("Failed to process question semantically");
        }

        String context = retrieveEnhancedContext(embedding, processedQuestion, requestId);
        String sparkSql = langChainSQLService.generateSQL(context, processedQuestion);
        String finalSql = postProcessSQL(sparkSql, requestId);

        return new QueryResponse(
            finalSql,
            sparkSql.contains("ERROR:") ? "Error in SQL generation" : "SQL generated successfully"
        );
    }

    public QueryResponse processQuestionWithContext(String sparkContext, String question) {
        String requestId = UUID.randomUUID().toString().substring(0, 8);
        log.info("[{}] Processing question with context", requestId);

        if (question == null || question.trim().isEmpty()) {
            return createErrorResponse("Question cannot be empty");
        }
        if (sparkContext == null || sparkContext.trim().isEmpty()) {
            return createErrorResponse("Spark context must not be empty");
        }

        String processedQuestion = preprocessQuestion(question);
        float[] embedding = EmbeddingUtils.embed(processedQuestion);

        String retrievedExamples = "";
        try {
            retrievedExamples = qdrantService.searchRelevantContext(embedding);
        } catch (Exception e) {
            log.warn("[{}] Failed to retrieve examples from Qdrant: {}", requestId, e.getMessage());
        }

        StringBuilder promptBuilder = new StringBuilder();
        promptBuilder.append("-- Spark Context:\n").append(sparkContext).append("\n\n");
        if (!retrievedExamples.isEmpty()) {
            promptBuilder.append("-- Retrieved Examples:\n").append(retrievedExamples).append("\n\n");
        }
        promptBuilder.append("-- Question:\n").append(processedQuestion);

        String sparkSql = langChainSQLService.generateSQL(promptBuilder.toString(), processedQuestion);
        String finalSql = postProcessSQL(sparkSql, requestId);

        return new QueryResponse(
            finalSql,
            sparkSql.contains("ERROR:") ? "Error in SQL generation" : "SQL generated successfully"
        );
    }

    private String preprocessQuestion(String question) {
        String processed = question.trim()
            .replaceAll("(?i)how many", "count")
            .replaceAll("(?i)what is the total", "sum")
            .replaceAll("(?i)what is the average", "average")
            .replaceAll("(?i)(show me|get|give me|find)", "select");

        if (processed.matches("(?i).*\\b(today|yesterday|last month|this year|last year)\\b.*")) {
            processed += " [TIME_SENSITIVE]";
        }

        if (processed.matches("(?i).*(total|sum|count|average|avg|max|min|group by).*")) {
            processed += " [AGGREGATION]";
        }

        if (COMPLEX_QUESTION_PATTERN.matcher(processed).find()) {
            processed += " [COMPLEX]";
        }

        return processed;
    }

    private String retrieveEnhancedContext(float[] embedding, String question, String requestId) {
        try {
            String context = qdrantService.searchRelevantContext(embedding);
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

        sql = sql.replaceAll("\\[.*?\\]", "");  // Remove [TAGS]
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

    private QueryResponse createErrorResponse(String message) {
        return new QueryResponse(
            createErrorSQL(message),
            "Error: " + message
        );
    }

    private String createErrorSQL(String message) {
        return String.format("/* Error: %s */\nSELECT 'ERROR: %s' AS error_message;", message, message);
    }

    private String getFallbackContext() {
        return """
            -- FALLBACK DATABASE SCHEMA --

            TABLE: customers
            COLUMNS: id (INT), name (STRING), email (STRING), city (STRING)

            TABLE: orders
            COLUMNS: id (INT), customer_id (INT), total_amount (DOUBLE), order_date (DATE), status (STRING)

            TABLE: products
            COLUMNS: id (INT), name (STRING), category (STRING), price (DOUBLE)

            TABLE: order_items
            COLUMNS: id (INT), order_id (INT), product_id (INT), quantity (INT)

            RELATIONSHIPS:
            - orders.customer_id -> customers.id
            - order_items.order_id -> orders.id
            - order_items.product_id -> products.id
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
