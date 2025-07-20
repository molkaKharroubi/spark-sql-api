package com.NLP2SparkSQL.project.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import okhttp3.OkHttpClient;
import okhttp3.ConnectionPool;
import okhttp3.Interceptor;
import okhttp3.Request;
import okhttp3.Response;

@Slf4j
@Configuration
public class OllamaConfiguration {

    @Value("${ollama.url:http://localhost:11434}")
    private String ollamaUrl;

    @Value("${ollama.timeout:300}")
    private long timeoutSeconds;

    @Bean
    @Primary
    public OkHttpClient customOkHttpClient() {
        log.info("Creating custom OkHttpClient for Ollama with timeout: {}s", timeoutSeconds);
        
        return new OkHttpClient.Builder()
                .connectTimeout(Duration.ofSeconds(30))
                .readTimeout(Duration.ofSeconds(timeoutSeconds)) // Use the configured timeout
                .writeTimeout(Duration.ofSeconds(60)) // Increased write timeout
                .callTimeout(Duration.ofSeconds(timeoutSeconds + 30)) // Add buffer to call timeout
                .connectionPool(new ConnectionPool(10, 5, TimeUnit.MINUTES))
                .retryOnConnectionFailure(true)
                .addInterceptor(new LoggingInterceptor()) // Add logging interceptor
                .build();
    }

    @Bean
    public OllamaHealthChecker ollamaHealthChecker() {
        return new OllamaHealthChecker(ollamaUrl);
    }

    // Custom interceptor for better logging
    private static class LoggingInterceptor implements Interceptor {
        @Override
        public Response intercept(Chain chain) throws IOException {
            Request request = chain.request();
            long startTime = System.currentTimeMillis();
            
            log.debug("Sending request to: {}", request.url());
            
            try {
                Response response = chain.proceed(request);
                long endTime = System.currentTimeMillis();
                
                log.debug("Received response from: {} in {}ms, status: {}", 
                         request.url(), endTime - startTime, response.code());
                
                return response;
            } catch (Exception e) {
                long endTime = System.currentTimeMillis();
                log.error("Request failed to: {} after {}ms, error: {}", 
                         request.url(), endTime - startTime, e.getMessage());
                throw e;
            }
        }
    }

    public static class OllamaHealthChecker {
        private final String ollamaUrl;

        public OllamaHealthChecker(String ollamaUrl) {
            this.ollamaUrl = ollamaUrl;
        }

        public boolean isOllamaHealthy() {
            try {
                log.debug("Checking Ollama health at: {}", ollamaUrl);
                URL url = new URL(ollamaUrl + "/api/tags");
                HttpURLConnection connection = (HttpURLConnection) url.openConnection();
                connection.setRequestMethod("GET");
                connection.setConnectTimeout(10000); // Increased connect timeout
                connection.setReadTimeout(15000); // Increased read timeout
                
                int responseCode = connection.getResponseCode();
                log.debug("Ollama health check response code: {}", responseCode);
                
                return responseCode == 200;
            } catch (IOException e) {
                log.error("Ollama health check failed: {}", e.getMessage());
                return false;
            }
        }

        public void waitForOllama(int maxWaitSeconds) {
            log.info("Waiting for Ollama to be available at: {}", ollamaUrl);
            
            int attempts = 0;
            int maxAttempts = maxWaitSeconds / 5; // Check every 5 seconds
            
            while (attempts < maxAttempts) {
                log.debug("Health check attempt {} of {}", attempts + 1, maxAttempts);
                
                if (isOllamaHealthy()) {
                    log.info("Ollama is now available after {} seconds", attempts * 5);
                    return;
                }
                
                attempts++;
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.warn("Health check interrupted");
                    break;
                }
            }
            
            throw new RuntimeException("Ollama is not available after " + maxWaitSeconds + " seconds");
        }
    }
}