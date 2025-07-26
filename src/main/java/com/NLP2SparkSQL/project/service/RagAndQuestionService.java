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

        StringBuilder combinedText = new StringBuilder();
        combinedText.append(preprocessText(question));
        if (sql != null && !sql.trim().isEmpty()) {
            combinedText.append("\nSQL: ").append(sql.trim());
        }
        if (schema != null && !schema.trim().isEmpty()) {
            combinedText.append("\nSchema: ").append(schema.trim());
        }

        float[] embedding = EmbeddingUtils.embed(combinedText.toString());

        if (embedding == null || embedding.length == 0) {
            return new RagResponse("Failed to generate embedding", "", 0);
        }

        Map<String, String> result = qdrantService.searchRelevantContextStructured(embedding);

        long duration = System.currentTimeMillis() - start;

        log.info("RAG search took {} ms for combined input", duration);

        if (result == null) {
            return new RagResponse("", "", duration);
        }

        // Vérification simplifiée - juste s'assurer que les données sont présentes
        if (!qdrantService.hasValidResult(result)) {
            log.info("Aucune donnée valide trouvée dans le résultat");
            return new RagResponse("", "", duration);
        }

        String foundQuestion = result.getOrDefault("question", "");
        String foundSql = result.getOrDefault("sql", "");
        String confidenceStr = result.getOrDefault("confidence", "0");

        long confidenceValue = 0;
        try {
            // Garder le score pour information, mais ne plus l'utiliser pour filtrer
            confidenceValue = Math.round(Double.parseDouble(confidenceStr) * 100);
        } catch (NumberFormatException e) {
            log.warn("Unable to parse confidence value: {}", confidenceStr);
        }

        RagResponse response = new RagResponse(foundQuestion, foundSql, duration);
        response.setscore(confidenceValue);

        log.info("Plus proche voisin retourné avec score: {}", confidenceValue);

        return response;
    }

    // Surcharge pour ne prendre que la question
    public RagResponse findExample(String question) {
        return findExample(question, null, null);
    }

    private String preprocessText(String text) {
        return text == null ? "" : text.trim();
    }
}