package com.NLP2SparkSQL.project.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.netty.http.client.HttpClient;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Service
public class OllamaService {

    @Value("${ollama.url:http://localhost:11434}")
    private String ollamaUrl;

    @Value("${ollama.model:qwen3:1.7b}")
    private String modelName;

    @Value("${ollama.timeout:120}")
    private int timeoutSeconds;

    private WebClient webClient;

    @PostConstruct
    private void init() {
        this.webClient = buildWebClient();
    }

    private WebClient buildWebClient() {
        return WebClient.builder()
                .codecs(configurer -> configurer.defaultCodecs().maxInMemorySize(100 * 1024 * 1024))
                .clientConnector(new ReactorClientHttpConnector(
                        HttpClient.create().responseTimeout(Duration.ofSeconds(timeoutSeconds))
                ))
                .build();
    }

    public String generateSparkSQL(String prompt) {
        try {
            log.info("Generating Spark SQL with Ollama model: {}", modelName);
            Map<String, Object> requestBody = createRequest(prompt);

            Map<String, Object> response = webClient.post()
                    .uri(ollamaUrl + "/api/generate")
                    .bodyValue(requestBody)
                    .retrieve()
                    .bodyToMono(Map.class)
                    .timeout(Duration.ofSeconds(timeoutSeconds))
                    .block();

            return processResponse(response);

        } catch (WebClientResponseException e) {
            log.error("Ollama API error - Status: {}, Body: {}", e.getStatusCode(), e.getResponseBodyAsString());
            return generateErrorSQL("Ollama API error: " + e.getMessage());
        } catch (Exception e) {
            log.error("Error calling Ollama: {}", e.getMessage(), e);
            return generateErrorSQL("Ollama service unavailable: " + e.getMessage());
        }
    }

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
        options.put("stop", new String[]{"\n\n", "Question:", "Answer:", "Explanation:"});

        request.put("options", options);
        return request;
    }

    private String processResponse(Map<String, Object> response) {
        if (response == null || !response.containsKey("response")) {
            return generateErrorSQL("Invalid response from Ollama");
        }

        Object raw = response.get("response");

        String sql;
        if (raw instanceof String) {
            sql = ((String) raw).trim();
        } else if (raw instanceof Map && ((Map<?, ?>) raw).containsKey("content")) {
            sql = ((Map<?, ?>) raw).get("content").toString().trim();
        } else {
            sql = raw.toString().trim();
        }

        if (sql.isEmpty()) {
            return generateErrorSQL("Empty SQL response");
        }

        return cleanSQL(sql);
    }

    private String cleanSQL(String sql) {
        return sql.replaceAll("(?s)```sql", "")
                .replaceAll("(?s)```", "")
                .replaceAll("(?s)/\\*.*?\\*/", "")
                .replaceAll("--.*", "")
                .replaceAll("<[^>]+>", "")
                .replaceAll("\\n{2,}", "\n")
                .trim();
    }

    private String generateErrorSQL(String error) {
        return "/* Error: " + error + " */\nSELECT 'ERROR: Could not generate SQL query' AS error_message;";
    }

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
