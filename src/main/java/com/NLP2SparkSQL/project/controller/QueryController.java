package com.NLP2SparkSQL.project.controller;

import com.NLP2SparkSQL.project.dto.QueryResponse;
import com.NLP2SparkSQL.project.dto.SQLContextualRequest;
import com.NLP2SparkSQL.project.service.RAGService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
@Tag(name = "SQL Generator", description = "Generate Spark SQL using RAG + Ollama")
public class QueryController {

    private final RAGService ragService;

    @Operation(summary = "Generate Spark SQL from question and provided Spark context")
    @PostMapping("/generate-sql-with-context")
    public QueryResponse generateSQLWithContext(@Valid @RequestBody SQLContextualRequest request) {
        return ragService.processQuestionWithContext(request.getSparkContext(), request.getQuestion());
    }

    @Data
    public static class SQLRequest {
        @jakarta.validation.constraints.NotBlank(message = "Question must not be blank")
        private String question;
    }
}
