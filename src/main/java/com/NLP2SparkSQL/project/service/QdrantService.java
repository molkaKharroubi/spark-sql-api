package com.NLP2SparkSQL.project.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class QdrantService {

    @Value("${qdrant.url:http://localhost:6333}")
    private String qdrantUrl;

    @Value("${qdrant.collection.name:my_sql_docs}")
    private String collectionName;

    @Value("${qdrant.search.top:1}")
    private int topResults;

    private final WebClient webClient;

    public QdrantService() {
        this.webClient = WebClient.builder()
                .codecs(configurer -> configurer.defaultCodecs().maxInMemorySize(10 * 1024 * 1024))
                .build();
    }

    public Map<String, String> searchRelevantContextStructured(float[] embedding) {
        log.info("=== QDRANT SEARCH DEBUG ===");
        log.info("Qdrant URL: {}", qdrantUrl);
        log.info("Collection: {}", collectionName);
        log.info("Embedding dimension: {}", embedding.length);
        log.info("Embedding norm: {}", calculateNorm(embedding));
        log.info("First 5 embedding values: {}", Arrays.toString(Arrays.copyOf(embedding, Math.min(5, embedding.length))));
        
        try {
            Map<String, Object> request = new HashMap<>();
            request.put("vector", embedding);
            request.put("limit", topResults);
            request.put("with_payload", true);
            request.put("with_vector", false);
            

            log.info("Sending request to Qdrant: {}", request.keySet());

            Map<String, Object> response = webClient.post()
                    .uri(qdrantUrl + "/collections/{collection}/points/search", collectionName)
                    .bodyValue(request)
                    .retrieve()
                    .bodyToMono(Map.class)
                    .timeout(Duration.ofSeconds(30))
                    .block();

            log.info("Qdrant response received: {}", response != null ? "SUCCESS" : "NULL");
            if (response != null) {
                log.info("Response keys: {}", response.keySet());
                if (response.containsKey("result")) {
                    List<Map<String, Object>> results = (List<Map<String, Object>>) response.get("result");
                    log.info("Number of results: {}", results.size());
                    
                    for (int i = 0; i < Math.min(results.size(), 3); i++) {
                        Map<String, Object> result = results.get(i);
                        log.info("Result {}: score={}, payload keys={}", 
                                i, result.get("score"), 
                                result.containsKey("payload") ? ((Map<String, Object>) result.get("payload")).keySet() : "null");
                    }
                }
            }

            return extractQuestionAndSqlFromResponse(response);

        } catch (Exception e) {
            log.error("Error searching Qdrant: {}", e.getMessage(), e);
            return createEmptyResult();
        }
    }

    private Map<String, String> extractQuestionAndSqlFromResponse(Map<String, Object> response) {
        if (response == null) {
            log.warn("Qdrant response is null");
            return createEmptyResult();
        }
        
        if (!response.containsKey("result")) {
            log.warn("Qdrant response missing 'result' key. Available keys: {}", response.keySet());
            return createEmptyResult();
        }

        List<Map<String, Object>> results = (List<Map<String, Object>>) response.get("result");
        if (results == null) {
            log.warn("Results list is null");
            return createEmptyResult();
        }
        
        if (results.isEmpty()) {
            log.warn("Results list is empty - no vectors found in collection or collection is empty");
            return createEmptyResult();
        }

        log.info("Processing {} search results", results.size());

        
        Map<String, Object> bestMatch = results.get(0);

        double confidence = 0.0;
        if (bestMatch.containsKey("score")) {
            confidence = ((Number) bestMatch.get("score")).doubleValue();
            log.info("Best match score: {}", confidence);
        } else {
            log.warn("Best match missing 'score' field");
        }

        if (!bestMatch.containsKey("payload")) {
            log.warn("Best match missing 'payload' field. Available keys: {}", bestMatch.keySet());
            return createEmptyResult();
        }

        Map<String, Object> payload = (Map<String, Object>) bestMatch.get("payload");
        if (payload == null) {
            log.warn("Payload is null");
            return createEmptyResult();
        }

        log.info("Payload keys: {}", payload.keySet());

        String question = "";
        String sql = "";

        if (payload.containsKey("question")) {
            Object questionObj = payload.get("question");
            question = (questionObj instanceof String) ? (String) questionObj : "";
            log.info("Extracted question (length {}): '{}'", question.length(), 
                    question.length() > 100 ? question.substring(0, 100) + "..." : question);
        } else {
            log.warn("Payload missing 'question' field");
        }

        if (payload.containsKey("sql")) {
            Object sqlObj = payload.get("sql");
            sql = (sqlObj instanceof String) ? (String) sqlObj : "";
            log.info("Extracted SQL (length {}): '{}'", sql.length(), 
                    sql.length() > 100 ? sql.substring(0, 100) + "..." : sql);
        } else {
            log.warn("Payload missing 'sql' field");
        }

       
        if (question.isEmpty() && sql.isEmpty()) {
            log.warn("Both question and SQL are empty");
            return createEmptyResult();
        }

        log.info("Successfully extracted data - Question: {} chars, SQL: {} chars, Score: {}", 
                question.length(), sql.length(), confidence);
        
        return Map.of(
            "question", question,
            "sql", sql,
            "confidence", String.valueOf(confidence)
        );
    }

  
    private Map<String, String> createEmptyResult() {
        log.info("Returning empty result");
        return Map.of(
            "question", "",
            "sql", "",
            "confidence", "0"
        );
    }

   
    public boolean hasValidResult(Map<String, String> result) {
        boolean isValid = result != null && 
               !result.get("question").isEmpty() && 
               !result.get("sql").isEmpty();
        
        log.info("Result validity check: {}", isValid);
        if (result != null) {
            log.info("Result content - Question: '{}', SQL: '{}', Confidence: '{}'", 
                    result.get("question"), result.get("sql"), result.get("confidence"));
        }
        
        return isValid;
    }

    public boolean isHealthy() {
        try {
            log.info("Checking Qdrant health at: {}/collections/{}", qdrantUrl, collectionName);
            
            Map<String, Object> response = webClient.get()
                    .uri(qdrantUrl + "/collections/{collection}", collectionName)
                    .retrieve()
                    .bodyToMono(Map.class)
                    .timeout(Duration.ofSeconds(5))
                    .block();
                    
            boolean healthy = response != null && response.containsKey("result");
            log.info("Qdrant health check result: {}", healthy);
            
            if (response != null && response.containsKey("result")) {
                Map<String, Object> result = (Map<String, Object>) response.get("result");
                if (result.containsKey("points_count")) {
                    log.info("Collection '{}' contains {} points", collectionName, result.get("points_count"));
                }
            }
            
            return healthy;
        } catch (Exception e) {
            log.error("Qdrant health check failed: {}", e.getMessage());
            return false;
        }
    }

    // Helper method to calculate embedding norm
    private float calculateNorm(float[] embedding) {
        float norm = 0.0f;
        for (float v : embedding) {
            norm += v * v;
        }
        return (float) Math.sqrt(norm);
    }
    /* cosine distance
    🔴 < 10 → low similarity (unrelated questions)

    🟠 10 – 15 → medium similarity

    🟢 15 – 20+ → very good similarity

    🟣 25 – 30+ → almost identical  
    */
}
