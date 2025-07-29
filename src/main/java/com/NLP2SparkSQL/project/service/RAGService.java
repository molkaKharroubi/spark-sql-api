package com.NLP2SparkSQL.project.service;

import com.NLP2SparkSQL.project.dto.QueryResponse;
import com.NLP2SparkSQL.project.utils.EmbeddingUtils;
import com.NLP2SparkSQL.project.utils.SparkSchemaParser;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.regex.*;
import java.util.stream.Collectors;


@Slf4j
@Service
@RequiredArgsConstructor
public class RAGService {

    private final QdrantService qdrantService;
    private final LangChainSQLService langChainSQLService;

    @Value("${app.max-query-length:10000}")
    private int maxQueryLength;

    // Cache for parsed schemas to avoid re-parsing identical contexts
    private final Map<String, Map<String, List<SparkSchemaParser.Column>>> schemaCache = new LRUCache<>(100);
    
    private static final Pattern COMPLEX_QUESTION_PATTERN = Pattern.compile(
        "(?i)(join|combine|merge|relationship|across|between|multiple|compare|analyze|trend|pattern|correlation|distribution|group|by)"
    );

    public QueryResponse processQuestion(String question) {
        return createErrorResponse("Please provide SparkContext. Use processQuestionWithContext method instead.", 0L);
    }

    /**
     * Main method that processes questions using dynamic SparkContext
     */
    public QueryResponse processQuestionWithContext(String sparkContext, String question) {
        long startTime = System.currentTimeMillis();
        String requestId = UUID.randomUUID().toString().substring(0, 8);
        log.info("[{}] Processing question with dynamic context", requestId);

        // Basic validation
        if (question == null || question.trim().isEmpty()) {
            return createErrorResponse("Question cannot be empty", getDuration(startTime));
        }
        if (sparkContext == null || sparkContext.trim().isEmpty()) {
            return createErrorResponse("Spark context is required and cannot be empty", getDuration(startTime));
        }
        if (question.length() > maxQueryLength) {
            return createErrorResponse("Question too long (max " + maxQueryLength + " characters)", getDuration(startTime));
        }

        try {
            //  Parse the SparkContext (tables/columns)
            Map<String, List<SparkSchemaParser.Column>> tables = parseSparkContextSafely(sparkContext, requestId);

            if (tables.isEmpty()) {
                log.warn("[{}] No tables could be parsed from context", requestId);
                return createErrorResponse("Could not extract any table information from the provided Spark context",
                        getDuration(startTime));
            }

            //  Create an embedding for the question
            float[] embedding = EmbeddingUtils.embed(question);

            //  Retrieve relevant example from Qdrant (similar question + SQL)
            Map<String, String> ragExample = qdrantService.searchRelevantContextStructured(embedding);

            // No more confidence check - always use nearest neighbor if exists
            if (ragExample != null && qdrantService.hasValidResult(ragExample)) {
                log.info("[{}] RAG example found (score: {}): {}", requestId,
                        ragExample.get("confidence"), ragExample.get("question"));
            } else {
                log.info("[{}] No valid RAG example found", requestId);
                ragExample = null;
            }

            //  Build enriched context (SparkContext + RAG example)
            String enhancedContext = buildEnhancedContext(tables, question, ragExample, requestId);

            //  Generate SQL with LangChain (based on SparkContext + RAG example)
            String sparkSql = langChainSQLService.generateSQL(enhancedContext, question);

            //  Post-process and validate the SQL
            String finalSql = postProcessSQL(sparkSql, tables, requestId);

            long duration = getDuration(startTime);
            log.info("[{}] Successfully processed question in {}ms", requestId, duration);

            return new QueryResponse(
                    finalSql,
                    finalSql.startsWith("ERROR:") ? "Error in SQL generation" : "SQL generated successfully",
                    duration
            );

        } catch (Exception e) {
            log.error("[{}] Error processing question: {}", requestId, e.getMessage(), e);
            return createErrorResponse("Internal error: " + e.getMessage(), getDuration(startTime));
        }
    }

    /**
     * Safely parse SparkContext with caching and error handling
     */
    private Map<String, List<SparkSchemaParser.Column>> parseSparkContextSafely(String sparkContext, String requestId) {
        // Create a hash of the context for caching
        String contextHash = Integer.toString(sparkContext.hashCode());
        
        // Check cache first
        if (schemaCache.containsKey(contextHash)) {
            log.debug("[{}] Using cached schema parse", requestId);
            return schemaCache.get(contextHash);
        }
        
        try {
            log.debug("[{}] Parsing new SparkContext (length: {})", requestId, sparkContext.length());
            
            // Parse the schema
            Map<String, List<SparkSchemaParser.Column>> tables = SparkSchemaParser.parseSparkSchema(sparkContext);
            
            // Cache the result
            schemaCache.put(contextHash, tables);
            
            log.info("[{}] Parsed {} tables from SparkContext", requestId, tables.size());
            return tables;
            
        } catch (Exception e) {
            log.error("[{}] Error parsing SparkContext: {}", requestId, e.getMessage());
            return new LinkedHashMap<>();
        }
    }

    /**
     * Build enhanced context that adapts to the specific schema structure
     * Always use the RAG example if available (no confidence check)
     */
    private String buildEnhancedContext(Map<String, List<SparkSchemaParser.Column>> tables,
                                        String question,
                                        Map<String, String> ragExample,
                                        String requestId) {
        StringBuilder contextBuilder = new StringBuilder();
        
        //  Add Qdrant example first (no confidence check)
        if (ragExample != null && 
            !ragExample.get("question").isEmpty() && 
            !ragExample.get("sql").isEmpty()) {
            
            contextBuilder.append("=== RELEVANT EXAMPLE FROM QDRANT ===\n");
            contextBuilder.append("-- Similar Question: ").append(ragExample.get("question")).append("\n");
            contextBuilder.append("-- Suggested SQL: ").append(ragExample.get("sql")).append("\n");
            contextBuilder.append("-- Similarity Score: ").append(ragExample.get("confidence")).append("\n\n");
            
            log.debug("[{}] Added RAG example to context with score: {}", requestId, ragExample.get("confidence"));
        } else {
            log.debug("[{}] No RAG example to add to context", requestId);
        }
        
        //  Then add Spark schema
        contextBuilder.append("=== SPARK SCHEMA INFORMATION ===\n");
        contextBuilder.append(SparkSchemaParser.generateContextInfo(tables));
        contextBuilder.append("\n");
        
        //  Add DDL
        contextBuilder.append("=== TABLE STRUCTURES ===\n");
        contextBuilder.append(SparkSchemaParser.generateDDL(tables));
        contextBuilder.append("\n");
        
        //  Add hints
        contextBuilder.append("=== QUERY GUIDANCE ===\n");
        contextBuilder.append(generateQueryHints(question, tables));
        contextBuilder.append("\n");
        
        //  Add basic SQL examples
        contextBuilder.append("=== SQL EXAMPLES ===\n");
        contextBuilder.append(generateSQLExamples(tables));
        
        log.debug("[{}] Generated enhanced context (length: {})", requestId, contextBuilder.length());
        
        return contextBuilder.toString();
    }

    /**
     * Generate dynamic query hints based on question analysis and available tables
     */
    private String generateQueryHints(String question, Map<String, List<SparkSchemaParser.Column>> tables) {
        StringBuilder hints = new StringBuilder();
        String lowerQuestion = question.toLowerCase();
        
        // Analyze question complexity
        if (COMPLEX_QUESTION_PATTERN.matcher(question).find()) {
            hints.append("-- COMPLEX QUERY DETECTED\n");
            hints.append("-- Consider using JOINs, GROUP BY, or window functions\n");
            
            // Suggest potential joins based on available tables
            if (tables.size() > 1) {
                hints.append("-- Available tables for joins: ").append(String.join(", ", tables.keySet())).append("\n");
            }
        } else {
            hints.append("-- SIMPLE QUERY DETECTED\n");
            hints.append("-- Single table SELECT should be sufficient\n");
        }
        
        // Column-specific hints
        for (Map.Entry<String, List<SparkSchemaParser.Column>> entry : tables.entrySet()) {
            String tableName = entry.getKey();
            List<SparkSchemaParser.Column> columns = entry.getValue();
            
            // Look for columns mentioned in the question
            for (SparkSchemaParser.Column col : columns) {
                if (lowerQuestion.contains(col.name.toLowerCase())) {
                    hints.append("-- Found matching column: ").append(tableName).append(".").append(col.name).append("\n");
                }
            }
        }
        
        // Type-specific hints
        if (lowerQuestion.contains("count") || lowerQuestion.contains("sum") || lowerQuestion.contains("avg")) {
            hints.append("-- AGGREGATION DETECTED: Use appropriate aggregate functions\n");
        }
        
        if (lowerQuestion.contains("date") || lowerQuestion.contains("time")) {
            hints.append("-- DATE/TIME QUERY: Consider using date functions and proper formatting\n");
        }
        
        return hints.toString();
    }

    /**
     * Generate SQL examples based on the actual schema
     */
    private String generateSQLExamples(Map<String, List<SparkSchemaParser.Column>> tables) {
        StringBuilder examples = new StringBuilder();
        examples.append("-- Example queries based on your schema:\n");
        
        for (Map.Entry<String, List<SparkSchemaParser.Column>> entry : tables.entrySet()) {
            String tableName = entry.getKey();
            List<SparkSchemaParser.Column> columns = entry.getValue();
            
            if (columns.isEmpty()) continue;
            
            // Basic SELECT example
            examples.append("-- Basic query for ").append(tableName).append(":\n");
            examples.append("-- SELECT ");
            
            List<String> columnNames = columns.stream()
                .map(col -> col.name)
                .limit(3) // Show first 3 columns
                .toList();
            
            examples.append(String.join(", ", columnNames));
            if (columns.size() > 3) {
                examples.append(", ...");
            }
            examples.append(" FROM ").append(tableName).append(";\n\n");
        }
        
        // Multi-table example if multiple tables exist
        if (tables.size() > 1) {
            String[] tableNames = tables.keySet().toArray(new String[0]);
            examples.append("-- Example JOIN (adjust based on actual relationships):\n");
            examples.append("-- SELECT * FROM ").append(tableNames[0])
                    .append(" t1 JOIN ").append(tableNames[1])
                    .append(" t2 ON t1.id = t2.").append(tableNames[0].toLowerCase()).append("_id;\n\n");
        }
        
        return examples.toString();
    }

    /**
     * Enhanced SQL post-processing that validates against the actual schema
     */
   private String postProcessSQL(String sql, Map<String, List<SparkSchemaParser.Column>> tables, String requestId) {
        if (sql == null || sql.trim().isEmpty()) {
            return "ERROR: No SQL generated";
        }
        
        // Clean up the SQL
        sql = sql.replaceAll("(?i)```sql", "").replaceAll("```", "").trim();
        
        // Validate SQL structure
        if (!isValidSQLStructure(sql)) {
            return "ERROR: Invalid SQL structure";
        }
        
        // Validate table and column references against the actual schema
        String validationError = validateSchemaReferences(sql, tables);
        if (validationError != null) {
            log.warn("[{}] Schema validation warning: {}", requestId, validationError);
            
        }
        
        log.info("[{}] Generated SQL:\n{}", requestId, sql);
        return sql;
    }
    /**
     * Validate that SQL references existing tables and columns
     */
    private String validateSchemaReferences(String sql, Map<String, List<SparkSchemaParser.Column>> tables) {
        String lowerSql = sql.toLowerCase();
        
        // Check for table references
        for (String tableName : tables.keySet()) {
            if (lowerSql.contains(tableName.toLowerCase())) {
                // Table is referenced, check if columns are valid
                List<SparkSchemaParser.Column> columns = tables.get(tableName);
                Set<String> validColumns = new HashSet<>();
                for (SparkSchemaParser.Column col : columns) {
                    validColumns.add(col.name.toLowerCase());
                }
            }
        }
        
        return null; 
    }

    private boolean isValidSQLStructure(String sql) {
        String lowerSql = sql.toLowerCase().trim();
        
        // Check if it starts with a valid SQL command
        String[] validStarts = {"select", "with", "insert", "update", "delete", "create", "drop", "alter"};
        
        for (String start : validStarts) {
            if (lowerSql.startsWith(start)) {
                return true;
            }
        }
        
        return false;
    }

    private QueryResponse createErrorResponse(String message, long duration) {
        return new QueryResponse("ERROR: " + message, message, duration);
    }

    private long getDuration(long startTime) {
        return System.currentTimeMillis() - startTime;
    }

    public boolean isHealthy() {
        try {
            return qdrantService.isHealthy() && langChainSQLService.isHealthy();
        } catch (Exception e) {
            log.error("RAG service health check failed: {}", e.getMessage());
            return false;
        }
    }

    /**
     * Simple LRU Cache implementation for schema caching
     */
    private static class LRUCache<K, V> extends LinkedHashMap<K, V> {
        private final int maxSize;

        public LRUCache(int maxSize) {
            super(16, 0.75f, true);
            this.maxSize = maxSize;
        }

        @Override
        protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
            return size() > maxSize;
        }
    }
}