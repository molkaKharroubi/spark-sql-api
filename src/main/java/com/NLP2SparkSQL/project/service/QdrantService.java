package com.NLP2SparkSQL.project.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

import java.time.Duration;
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

    /**
     * Search Qdrant vector DB using embedding and return question and SQL separately.
     */
    public Map<String, String> searchRelevantContextStructured(float[] embedding) {
        try {
            Map<String, Object> request = new HashMap<>();
            request.put("vector", embedding);
            request.put("limit", topResults);
            request.put("with_payload", true);
            request.put("with_vector", false);

            Map<String, Object> response = webClient.post()
                    .uri(qdrantUrl + "/collections/{collection}/points/search", collectionName)
                    .bodyValue(request)
                    .retrieve()
                    .bodyToMono(Map.class)
                    .timeout(Duration.ofSeconds(30))
                    .block();

            return extractQuestionAndSqlFromResponse(response);
        } catch (Exception e) {
            log.error("Error searching Qdrant: {}", e.getMessage(), e);
            return Map.of("question", "No question found", "sql", "No SQL found");
        }
    }

    private Map<String, String> extractQuestionAndSqlFromResponse(Map<String, Object> response) {
        if (response == null || !response.containsKey("result")) {
            return Map.of("question", "No question found", "sql", "No SQL found");
        }

        List<Map<String, Object>> results = (List<Map<String, Object>>) response.get("result");
        if (results.isEmpty()) {
            return Map.of("question", "No question found", "sql", "No SQL found");
        }

        Map<String, Object> payload = (Map<String, Object>) results.get(0).get("payload");
        String question = "";
        String sql = "";

        if (payload != null) {
            if (payload.get("question") instanceof String) {
                question = (String) payload.get("question");
            }
            if (payload.get("sql") instanceof String) {
                sql = (String) payload.get("sql");
            }
        }

        return Map.of("question", question, "sql", sql);
    }

    /**
     * Simple health check to verify if Qdrant collection is accessible.
     */
    public boolean isHealthy() {
        try {
            Map<String, Object> response = webClient.get()
                    .uri(qdrantUrl + "/collections/{collection}", collectionName)
                    .retrieve()
                    .bodyToMono(Map.class)
                    .timeout(Duration.ofSeconds(5))
                    .block();
            return response != null && response.containsKey("result");
        } catch (Exception e) {
            log.error("Qdrant health check failed: {}", e.getMessage());
            return false;
        }
    }
}
