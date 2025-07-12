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

    // Base URL of the Qdrant server
    @Value("${qdrant.url:http://localhost:6333}")
    private String qdrantUrl;

    // Name of the Qdrant collection that stores schema embeddings
    @Value("${qdrant.collection.name:sql_schema_collection}")
    private String collectionName;

    // Number of top similar results to retrieve
    @Value("${qdrant.search.top:5}")
    private int topResults;

    // WebClient used for HTTP calls to Qdrant API
    private final WebClient webClient;

    // Constructor initializes WebClient with default max buffer size
    public QdrantService() {
        this.webClient = WebClient.builder()
                // Increase memory buffer to handle larger responses if needed
                .codecs(configurer -> configurer.defaultCodecs().maxInMemorySize(10 * 1024 * 1024))
                .build();
    }

    /**
     * Perform a similarity search on Qdrant using the given embedding vector.
     * Retrieves top N relevant schema documents with their payload.
     *
     * @param embedding Float array representing the query vector embedding
     * @return Concatenated string of relevant schema context or fallback context if failed
     */
    public String searchRelevantContext(float[] embedding) {
        try {
            // Build the JSON request body for Qdrant search
            Map<String, Object> request = new HashMap<>();
            request.put("vector", embedding);
            request.put("limit", topResults);
            request.put("with_payload", true);
            request.put("with_vector", false);

            // POST request to Qdrant search endpoint
            Map<String, Object> response = webClient.post()
                    .uri(qdrantUrl + "/collections/{collection}/points/search", collectionName)
                    .bodyValue(request)
                    .retrieve()
                    .bodyToMono(Map.class)
                    .timeout(Duration.ofSeconds(30))
                    .block();

            // Extract and format the relevant context from the response
            return extractContextFromResponse(response);
        } catch (Exception e) {
            log.error("Error searching Qdrant: {}", e.getMessage(), e);
            // Return fallback context if any error occurs
            return fallbackContext();
        }
    }

    /**
     * Extracts the textual context from the Qdrant search response payload.
     * Concatenates meaningful schema info from top search results.
     *
     * @param response JSON map from Qdrant API
     * @return Concatenated schema context string or fallback if none found
     */
    private String extractContextFromResponse(Map<String, Object> response) {
        if (response == null || !response.containsKey("result")) {
            return fallbackContext();
        }

        List<Map<String, Object>> results = (List<Map<String, Object>>) response.get("result");
        StringBuilder sb = new StringBuilder();

        // Loop over each result and append a human-readable schema segment
        for (int i = 0; i < results.size(); i++) {
            Map<String, Object> payload = (Map<String, Object>) results.get(i).get("payload");
            if (payload != null) {
                for (Object val : payload.values()) {
                    // Include only non-empty strings longer than 10 characters as relevant schema info
                    if (val instanceof String str && str.length() > 10) {
                        sb.append("--- Document ").append(i + 1).append(" ---\n")
                          .append(str).append("\n\n");
                        break; // Include only first suitable string per document
                    }
                }
            }
        }

        // Return the concatenated schema or fallback if empty
        String context = sb.toString().trim();
        return context.isEmpty() ? fallbackContext() : context;
    }

    /**
     * Provides a fallback schema context in case Qdrant search fails.
     * This ensures the SQL generation has at least a minimal schema description.
     *
     * @return Static fallback schema description string
     */
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

    /**
     * Health check method to verify Qdrant collection availability.
     *
     * @return true if collection exists and API is reachable, false otherwise
     */
    public boolean isHealthy() {
        try {
            Map<String, Object> response = webClient.get()
                    .uri(qdrantUrl + "/collections/{collection}", collectionName)
                    .retrieve()
                    .bodyToMono(Map.class)
                    .timeout(Duration.ofSeconds(5))
                    .block();
            // Valid response contains a 'result' key
            return response != null && response.containsKey("result");
        } catch (Exception e) {
            log.error("Qdrant health check failed: {}", e.getMessage());
            return false;
        }
    }
}
