package com.NLP2SparkSQL.project.utils;

import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.regex.Pattern;

@Slf4j
public class EmbeddingUtils {

    private static final int EMBEDDING_DIM = 384; // Common embedding dimension
    private static final Pattern WORD_PATTERN = Pattern.compile("[a-zA-Z]+");
    
    // SQL-specific keywords that should have higher weights
    private static final Set<String> SQL_KEYWORDS = Set.of(
        "select", "from", "where", "join", "group", "order", "having", "count", "sum", "avg",
        "max", "min", "distinct", "limit", "offset", "inner", "left", "right", "outer",
        "union", "intersect", "except", "case", "when", "then", "else", "end", "as",
        "and", "or", "not", "in", "exists", "between", "like", "is", "null"
    );
    
    // Business domain keywords
    private static final Set<String> BUSINESS_KEYWORDS = Set.of(
        "customer", "order", "product", "sale", "revenue", "profit", "quantity", "price",
        "total", "amount", "date", "time", "month", "year", "category", "status", "name",
        "email", "address", "phone", "city", "state", "country"
    );

    /**
     * Generate embedding for a given text using improved algorithm
     * 
     * @param text input text (question or context)
     * @return normalized embedding vector
     */
    public static float[] embed(String text) {
        if (text == null || text.trim().isEmpty()) {
            return new float[EMBEDDING_DIM];
        }

        try {
            // Preprocess text
            String processedText = preprocessText(text);
            
            // Tokenize
            List<String> tokens = tokenize(processedText);
            
            if (tokens.isEmpty()) {
                return new float[EMBEDDING_DIM];
            }
            
            // Generate embedding
            float[] embedding = generateEmbedding(tokens);
            
            // Normalize
            return normalizeVector(embedding);
            
        } catch (Exception e) {
            log.error("Error generating embedding for text: {}", text, e);
            return new float[EMBEDDING_DIM];
        }
    }

    /**
     * Preprocess text for better embedding generation
     */
    private static String preprocessText(String text) {
        return text.toLowerCase()
                  .replaceAll("[^a-zA-Z0-9\\s]", " ")
                  .replaceAll("\\s+", " ")
                  .trim();
    }

    /**
     * Tokenize text into words
     */
    private static List<String> tokenize(String text) {
        List<String> tokens = new ArrayList<>();
        String[] words = text.split("\\s+");
        
        for (String word : words) {
            if (word.length() > 2) { // Filter out very short words
                tokens.add(word);
            }
        }
        
        return tokens;
    }

    /**
     * Generate embedding vector from tokens
     */
    private static float[] generateEmbedding(List<String> tokens) {
        float[] embedding = new float[EMBEDDING_DIM];
        Map<String, Float> tokenWeights = calculateTokenWeights(tokens);
        
        for (Map.Entry<String, Float> entry : tokenWeights.entrySet()) {
            String token = entry.getKey();
            float weight = entry.getValue();
            
            float[] tokenVector = generateTokenVector(token);
            
            // Add weighted token vector to embedding
            for (int i = 0; i < EMBEDDING_DIM; i++) {
                embedding[i] += tokenVector[i] * weight;
            }
        }
        
        return embedding;
    }

    /**
     * Calculate weights for tokens based on their importance
     */
    private static Map<String, Float> calculateTokenWeights(List<String> tokens) {
        Map<String, Integer> tokenCounts = new HashMap<>();
        Map<String, Float> tokenWeights = new HashMap<>();
        
        // Count token frequencies
        for (String token : tokens) {
            tokenCounts.put(token, tokenCounts.getOrDefault(token, 0) + 1);
        }
        
        // Calculate TF-IDF-like weights
        for (Map.Entry<String, Integer> entry : tokenCounts.entrySet()) {
            String token = entry.getKey();
            int count = entry.getValue();
            
            float tf = (float) count / tokens.size();
            float weight = tf;
            
            // Boost SQL keywords
            if (SQL_KEYWORDS.contains(token)) {
                weight *= 2.0f;
            }
            
            // Boost business domain keywords
            if (BUSINESS_KEYWORDS.contains(token)) {
                weight *= 1.5f;
            }
            
            tokenWeights.put(token, weight);
        }
        
        return tokenWeights;
    }

    /**
     * Generate vector for a single token using improved hash-based method
     */
    private static float[] generateTokenVector(String token) {
        float[] vector = new float[EMBEDDING_DIM];
        
        // Use multiple hash functions for better distribution
        int hash1 = token.hashCode();
        int hash2 = token.hashCode() * 31;
        int hash3 = token.hashCode() * 37;
        
        // Generate pseudo-random values using different seeds
        Random random1 = new Random(hash1);
        Random random2 = new Random(hash2);
        Random random3 = new Random(hash3);
        
        for (int i = 0; i < EMBEDDING_DIM; i++) {
            float val1 = (float) (random1.nextGaussian() * 0.3f);
float val2 = (float) (random2.nextGaussian() * 0.3f);
float val3 = (float) (random3.nextGaussian() * 0.3f);

            vector[i] = (val1 + val2 + val3) / 3.0f;
        }
        
        return vector;
    }

    /**
     * Normalize vector to unit length
     */
    private static float[] normalizeVector(float[] vector) {
        float norm = 0.0f;
        
        // Calculate L2 norm
        for (float v : vector) {
            norm += v * v;
        }
        
        norm = (float) Math.sqrt(norm);
        
        if (norm > 0) {
            for (int i = 0; i < vector.length; i++) {
                vector[i] /= norm;
            }
        }
        
        return vector;
    }

    /**
     * Calculate cosine similarity between two vectors
     */
    public static float cosineSimilarity(float[] vec1, float[] vec2) {
        if (vec1.length != vec2.length) {
            throw new IllegalArgumentException("Vectors must have the same dimension");
        }
        
        float dotProduct = 0.0f;
        float norm1 = 0.0f;
        float norm2 = 0.0f;
        
        for (int i = 0; i < vec1.length; i++) {
            dotProduct += vec1[i] * vec2[i];
            norm1 += vec1[i] * vec1[i];
            norm2 += vec2[i] * vec2[i];
        }
        
        if (norm1 == 0.0f || norm2 == 0.0f) {
            return 0.0f;
        }
        
        return dotProduct / (float) (Math.sqrt(norm1) * Math.sqrt(norm2));
    }

    /**
     * Generate embedding specifically optimized for SQL schema matching
     */
    public static float[] embedForSchema(String schemaText) {
        if (schemaText == null || schemaText.trim().isEmpty()) {
            return new float[EMBEDDING_DIM];
        }

        try {
            // Extract key information from schema
            String processedSchema = extractSchemaKeywords(schemaText);
            
            // Generate embedding with schema-specific processing
            return embed(processedSchema);
            
        } catch (Exception e) {
            log.error("Error generating schema embedding: {}", e.getMessage());
            return new float[EMBEDDING_DIM];
        }
    }

    /**
     * Extract and emphasize important keywords from schema text
     */
    private static String extractSchemaKeywords(String schemaText) {
        StringBuilder enhanced = new StringBuilder();
        
        // Split by lines to process each line
        String[] lines = schemaText.split("\\n");
        
        for (String line : lines) {
            String lowerLine = line.toLowerCase();
            
            // Emphasize table names
            if (lowerLine.contains("table:") || lowerLine.contains("table ")) {
                enhanced.append("table ").append(line).append(" table ");
            }
            
            // Emphasize column names
            if (lowerLine.contains("column") || lowerLine.contains("field")) {
                enhanced.append("column ").append(line).append(" column ");
            }
            
            // Emphasize data types
            if (lowerLine.contains("int") || lowerLine.contains("varchar") || 
                lowerLine.contains("double") || lowerLine.contains("date")) {
                enhanced.append("datatype ").append(line).append(" datatype ");
            }
            
            // Add original line
            enhanced.append(line).append(" ");
        }
        
        return enhanced.toString();
    }

    /**
     * Batch embedding generation for multiple texts
     */
    public static List<float[]> batchEmbed(List<String> texts) {
        List<float[]> embeddings = new ArrayList<>();
        
        for (String text : texts) {
            embeddings.add(embed(text));
        }
        
        return embeddings;
    }

    /**
     * Find the most similar embedding from a collection
     */
    public static int findMostSimilar(float[] queryEmbedding, List<float[]> candidateEmbeddings) {
        if (candidateEmbeddings.isEmpty()) {
            return -1;
        }
        
        int bestIndex = 0;
        float bestSimilarity = cosineSimilarity(queryEmbedding, candidateEmbeddings.get(0));
        
        for (int i = 1; i < candidateEmbeddings.size(); i++) {
            float similarity = cosineSimilarity(queryEmbedding, candidateEmbeddings.get(i));
            if (similarity > bestSimilarity) {
                bestSimilarity = similarity;
                bestIndex = i;
            }
        }
        
        return bestIndex;
    }

    /**
     * Get embedding dimension
     */
    public static int getEmbeddingDimension() {
        return EMBEDDING_DIM;
    }

}