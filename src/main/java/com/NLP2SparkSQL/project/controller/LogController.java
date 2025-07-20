package com.NLP2SparkSQL.project.controller;

import com.NLP2SparkSQL.project.dto.LlmResponse;
import com.NLP2SparkSQL.project.dto.QueryResponse;
import com.NLP2SparkSQL.project.dto.RagRequest;
import com.NLP2SparkSQL.project.dto.RagResponse;
import com.NLP2SparkSQL.project.dto.SQLContextualRequest; // assuming this is your request DTO
import com.NLP2SparkSQL.project.service.QuestionAndSparkContextService;
import com.NLP2SparkSQL.project.service.RagAndQuestionService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import jakarta.validation.Valid;

@RestController
@RequestMapping("/log")  
@RequiredArgsConstructor
public class LogController {

    private final RagAndQuestionService ragService;
    private final QuestionAndSparkContextService questionAndSparkContextService;

    @PostMapping("/logRAG")
    public ResponseEntity<RagResponse> getExample(@RequestBody RagRequest request) {
        if (request.getQuestion() == null || request.getQuestion().trim().isEmpty()) {
            return ResponseEntity.badRequest().build();
        }
        RagResponse response = ragService.findExample(request.getQuestion());
        return ResponseEntity.ok(response);
    }

    @PostMapping("/logLLM")
    public ResponseEntity<LlmResponse> generate(@Valid @RequestBody SQLContextualRequest request) {
        long startTime = System.currentTimeMillis();

        QueryResponse qr = questionAndSparkContextService.generateSqlFromSchemaAndQuestion(
            request.getSparkContext(),
            request.getQuestion()
        );

        long endTime = System.currentTimeMillis();

        LlmResponse response = new LlmResponse(qr.getSparkSql(), endTime - startTime);
        return ResponseEntity.ok(response);
    }
}
