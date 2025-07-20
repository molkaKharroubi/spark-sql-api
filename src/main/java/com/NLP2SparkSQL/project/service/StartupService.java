package com.NLP2SparkSQL.project.service;

import com.NLP2SparkSQL.project.config.OllamaConfiguration;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class StartupService {

    private final OllamaConfiguration.OllamaHealthChecker ollamaHealthChecker;

    @EventListener(ApplicationReadyEvent.class)
    public void onApplicationReady() {
        log.info("Application is ready, performing startup checks...");
        
        try {
            // Wait for Ollama to be available
            ollamaHealthChecker.waitForOllama(60); // Wait up to 60 seconds
            
            log.info("All startup checks passed successfully");
        } catch (Exception e) {
            log.error("Startup check failed: {}", e.getMessage());
            throw new RuntimeException("Application startup failed", e);
        }
    }
}