package com.NLP2SparkSQL.project.controller;

import com.NLP2SparkSQL.project.dto.QueryResponse;
import com.NLP2SparkSQL.project.service.RAGService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
@Tag(name = "SQL Generator", description = "Generate Spark SQL (SELECT, INSERT, UPDATE, DELETE) using RAG + Ollama")
public class QueryController {

    private final RAGService ragService;

    @Operation(summary = "Generate any type of Spark SQL from a natural language question")
    @PostMapping("/generate-sql")
    public QueryResponse generateSQL(@RequestBody SQLRequest request) {
        return ragService.processQuestion(request.getQuestion());
    }

    @Data
    static class SQLRequest {
        private String question;
    }
}
