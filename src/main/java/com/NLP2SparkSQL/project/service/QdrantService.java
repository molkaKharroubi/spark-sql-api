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

    @Value("${qdrant.collection.name:sql_schema_collection}")
    private String collectionName;

    @Value("${qdrant.search.top:5}")
    private int topResults;

    private final WebClient webClient;

    public QdrantService() {
        this.webClient = WebClient.builder()
                .codecs(configurer -> configurer.defaultCodecs().maxInMemorySize(10 * 1024 * 1024))
                .build();
    }

    public String searchRelevantContext(float[] embedding) {
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

            return extractContextFromResponse(response);
        } catch (Exception e) {
            log.error("Error searching Qdrant: {}", e.getMessage(), e);
            return fallbackContext();
        }
    }

    private String extractContextFromResponse(Map<String, Object> response) {
        if (response == null || !response.containsKey("result")) return fallbackContext();

        List<Map<String, Object>> results = (List<Map<String, Object>>) response.get("result");
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < results.size(); i++) {
            Map<String, Object> payload = (Map<String, Object>) results.get(i).get("payload");
            if (payload != null) {
                for (Object val : payload.values()) {
                    if (val instanceof String str && str.length() > 10) {
                        sb.append("--- Document ").append(i + 1).append(" ---\n").append(str).append("\n\n");
                        break;
                    }
                }
            }
        }

        return sb.toString().trim().isEmpty() ? fallbackContext() : sb.toString().trim();
    }

    private String fallbackContext() {
        return """
                TABLE: customers
                COLUMNS: id (INT), name (VARCHAR), email (VARCHAR), city (VARCHAR)
                DESCRIPTION: Customer information table

                TABLE: orders
                COLUMNS: id (INT), customer_id (INT), total (DOUBLE), date (DATE), status (VARCHAR)
                DESCRIPTION: Order transactions table

                TABLE: products
                COLUMNS: id (INT), name (VARCHAR), category (VARCHAR), price (DOUBLE)
                DESCRIPTION: Product catalog table

                RELATIONSHIPS:
                - orders.customer_id -> customers.id
                """;
    }

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
