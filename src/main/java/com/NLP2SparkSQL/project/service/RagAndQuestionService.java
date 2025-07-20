package com.NLP2SparkSQL.project.service;

import com.NLP2SparkSQL.project.dto.RagResponse;
import com.NLP2SparkSQL.project.utils.EmbeddingUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
@RequiredArgsConstructor
@Slf4j
public class RagAndQuestionService {

    private final QdrantService qdrantService;

    // Méthode principale
    public RagResponse findExample(String question, String sql, String schema) {
        long start = System.currentTimeMillis();

        if ((question == null || question.trim().isEmpty()) &&
            (sql == null || sql.trim().isEmpty())) {
            return new RagResponse("Question and SQL are empty", "", 0);
        }

        String combinedText = preprocessText(question)
                            + (sql != null && !sql.trim().isEmpty() ? "\nSQL: " + sql.trim() : "")
                            + (schema != null && !schema.trim().isEmpty() ? "\nSchema: " + schema.trim() : "");

        float[] embedding = EmbeddingUtils.embed(combinedText);

        if (embedding == null || embedding.length == 0) {
            return new RagResponse("Failed to generate embedding", "", 0);
        }

        Map<String, String> result = qdrantService.searchRelevantContextStructured(embedding);

        long end = System.currentTimeMillis();
        long duration = end - start;

        log.info("RAG search took {} ms for combined input", duration);

        return new RagResponse(
            result.getOrDefault("question", ""),
            result.getOrDefault("sql", ""),
            duration
        );
    }

    // Surcharge pour ne prendre que la question
    public RagResponse findExample(String question) {
        return findExample(question, null, null);
    }

    private String preprocessText(String text) {
        return text == null ? "" : text.trim();
    }
}
