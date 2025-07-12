package com.NLP2SparkSQL.project.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.netty.http.client.HttpClient;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Service
public class OllamaService {

    // Base URL of the Ollama server
    @Value("${ollama.url:http://localhost:11434}")
    private String ollamaUrl;

    // Ollama model name to use for generation
    @Value("${ollama.model:qwen3:1.7b}")
    private String modelName;

    // Timeout in seconds for HTTP calls
    @Value("${ollama.timeout:120}")
    private int timeoutSeconds;

    // WebClient instance configured with timeout and buffer size
    private final WebClient webClient;

    // Constructor initializes WebClient with custom timeout and buffer limit
    public OllamaService() {
        this.webClient = WebClient.builder()
                // Increase max memory buffer for large responses
                .codecs(configurer -> configurer.defaultCodecs().maxInMemorySize(100 * 1024 * 1024))
                // Set response timeout on the underlying HTTP client
                .clientConnector(new ReactorClientHttpConnector(
                        HttpClient.create().responseTimeout(Duration.ofSeconds(timeoutSeconds))
                ))
                .build();
    }

    /**
     * Call Ollama API to generate Spark SQL query from a natural language prompt.
     * Handles request building, API call, response parsing and error handling.
     *
     * @param prompt The prompt containing schema and question
     * @return Generated SQL query string or error SQL message
     */
    public String generateSparkSQL(String prompt) {
        try {
            log.info("Generating Spark SQL with Ollama model: {}", modelName);

            Map<String, Object> requestBody = createRequest(prompt);

            // POST request to Ollama /api/generate endpoint
            Map<String, Object> response = webClient.post()
                    .uri(ollamaUrl + "/api/generate")
                    .bodyValue(requestBody)
                    .retrieve()
                    .bodyToMono(Map.class)
                    .timeout(Duration.ofSeconds(timeoutSeconds))
                    .block();

            // Process and extract SQL from response
            return processResponse(response);

        } catch (WebClientResponseException e) {
            // Log detailed HTTP error info from Ollama API
            log.error("Ollama API error - Status: {}, Body: {}", e.getStatusCode(), e.getResponseBodyAsString());
            return generateErrorSQL("Ollama API error: " + e.getMessage());
        } catch (Exception e) {
            // General exception catch for connection or unexpected errors
            log.error("Error calling Ollama: {}", e.getMessage(), e);
            return generateErrorSQL("Ollama service unavailable: " + e.getMessage());
        }
    }

    /**
     * Build the request payload to send to Ollama generate endpoint.
     * Sets model, prompt, generation options like temperature, stop tokens, etc.
     *
     * @param prompt Text prompt for SQL generation
     * @return Map representing JSON request body
     */
    private Map<String, Object> createRequest(String prompt) {
        Map<String, Object> request = new HashMap<>();
        request.put("model", modelName);
        request.put("prompt", prompt);
        request.put("stream", false);

        Map<String, Object> options = new HashMap<>();
        options.put("temperature", 0.1);
        options.put("top_p", 0.9);
        options.put("top_k", 40);
        options.put("repeat_penalty", 1.1);
        options.put("num_predict", 200);
        // Stop tokens to signal end of generation early if any of these appear
        options.put("stop", new String[]{"\n\n", "Question:", "Answer:", "Explanation:"});

        request.put("options", options);
        return request;
    }

    /**
     * Extract the generated SQL query from the Ollama API response.
     * If response invalid or empty, returns an error SQL.
     *
     * @param response The JSON map returned by Ollama API
     * @return SQL query string or error SQL
     */
    private String processResponse(Map<String, Object> response) {
        if (response == null || !response.containsKey("response")) {
            return generateErrorSQL("Invalid response from Ollama");
        }

        String sql = (String) response.get("response");
        return (sql == null || sql.trim().isEmpty())
                ? generateErrorSQL("Empty SQL response")
                : sql.trim();
    }

    /**
     * Generates a SQL SELECT statement embedding an error message
     * so the caller can safely display or log the error.
     *
     * @param error Error message to embed
     * @return SQL query string representing the error
     */
    private String generateErrorSQL(String error) {
        return "/* Error: " + error + " */\nSELECT 'ERROR: Could not generate SQL query' AS error_message;";
    }

    /**
     * Simple health check to verify Ollama server is reachable
     * and models can be listed.
     *
     * @return true if Ollama is healthy, false otherwise
     */
    public boolean isHealthy() {
        try {
            Map<String, Object> response = webClient.get()
                    .uri(ollamaUrl + "/api/tags")
                    .retrieve()
                    .bodyToMono(Map.class)
                    .timeout(Duration.ofSeconds(timeoutSeconds))
                    .block();

            return response != null && response.containsKey("models");
        } catch (Exception e) {
            log.error("Ollama health check failed: {}", e.getMessage());
            return false;
        }
    }
}
