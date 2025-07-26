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
        "email", "address", "phone", "city", "state", "country", "employee", "job", "performance",
        "review", "department", "salary", "manager", "hire", "position", "work", "company"
    );

    /**
     * Generate embedding for a given text using improved algorithm
     * 
     * @param text input text (question or context)
     * @return normalized embedding vector
     */
    public static float[] embed(String text) {
        if (text == null || text.trim().isEmpty()) {
            log.warn("Empty or null text provided for embedding");
            return new float[EMBEDDING_DIM];
        }

        try {
            log.debug("Generating embedding for text: '{}'", text.substring(0, Math.min(text.length(), 100)));
            
            // Preprocess text
            String processedText = preprocessText(text);
            log.debug("Preprocessed text: '{}'", processedText);
            
            // Tokenize
            List<String> tokens = tokenize(processedText);
            log.debug("Extracted {} tokens: {}", tokens.size(), tokens.subList(0, Math.min(tokens.size(), 10)));
            
            if (tokens.isEmpty()) {
                log.warn("No tokens extracted from text: '{}'", text);
                return new float[EMBEDDING_DIM];
            }
            
            // Generate embedding
            float[] embedding = generateEmbedding(tokens);
            
            // Normalize
            float[] normalizedEmbedding = normalizeVector(embedding);
            
            // Log embedding statistics
            float norm = calculateNorm(normalizedEmbedding);
            float mean = calculateMean(normalizedEmbedding);
            log.debug("Generated embedding - Norm: {}, Mean: {}, Non-zero elements: {}", 
                     norm, mean, countNonZero(normalizedEmbedding));
            
            return normalizedEmbedding;
            
        } catch (Exception e) {
            log.error("Error generating embedding for text: {}", text, e);
            return new float[EMBEDDING_DIM];
        }
    }

    /**
     * Preprocess text for better embedding generation
     */
    private static String preprocessText(String text) {
        // Enhanced preprocessing for better SQL/business context understanding
        String processed = text.toLowerCase()
                              .replaceAll("\\bwhich\\b", "what") // Normalize question words
                              .replaceAll("\\bassociated with\\b", "related to")
                              .replaceAll("\\bhave had\\b", "have")
                              .replaceAll("\\bat least one\\b", "one or more")
                              .replaceAll("[^a-zA-Z0-9\\s]", " ")
                              .replaceAll("\\s+", " ")
                              .trim();
        
        return processed;
    }

    /**
     * Tokenize text into words with improved filtering
     */
    private static List<String> tokenize(String text) {
        List<String> tokens = new ArrayList<>();
        String[] words = text.split("\\s+");
        
        // Common stop words to filter out
        Set<String> stopWords = Set.of("the", "is", "are", "was", "were", "be", "been", "being", 
                                      "have", "has", "had", "do", "does", "did", "will", "would", 
                                      "could", "should", "may", "might", "must", "can", "a", "an", "to");
        
        for (String word : words) {
            if (word.length() > 1 && !stopWords.contains(word)) { // Lowered threshold and filter stop words
                tokens.add(word);
            }
        }
        
        return tokens;
    }

    /**
     * Generate embedding vector from tokens with improved weighting
     */
    private static float[] generateEmbedding(List<String> tokens) {
        float[] embedding = new float[EMBEDDING_DIM];
        Map<String, Float> tokenWeights = calculateTokenWeights(tokens);
        
        int processedTokens = 0;
        for (Map.Entry<String, Float> entry : tokenWeights.entrySet()) {
            String token = entry.getKey();
            float weight = entry.getValue();
            
            float[] tokenVector = generateTokenVector(token);
            
            // Add weighted token vector to embedding
            for (int i = 0; i < EMBEDDING_DIM; i++) {
                embedding[i] += tokenVector[i] * weight;
            }
            processedTokens++;
        }
        
        log.debug("Processed {} unique tokens for embedding generation", processedTokens);
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
        
        // Calculate improved weights
        for (Map.Entry<String, Integer> entry : tokenCounts.entrySet()) {
            String token = entry.getKey();
            int count = entry.getValue();
            
            float tf = (float) count / tokens.size();
            float weight = tf;
            
            // Boost SQL keywords more aggressively
            if (SQL_KEYWORDS.contains(token)) {
                weight *= 3.0f;
                log.debug("Boosted SQL keyword '{}' with weight: {}", token, weight);
            }
            
            // Boost business domain keywords
            if (BUSINESS_KEYWORDS.contains(token)) {
                weight *= 2.0f;
                log.debug("Boosted business keyword '{}' with weight: {}", token, weight);
            }
            
            // Boost question-specific terms
            if (token.equals("jobs") || token.equals("employees") || token.equals("performance") || token.equals("review")) {
                weight *= 2.5f;
                log.debug("Boosted domain-specific term '{}' with weight: {}", token, weight);
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
        
        // Use the token's bytes for more deterministic hashing
        byte[] tokenBytes = token.getBytes(StandardCharsets.UTF_8);
        
        // Use multiple hash functions for better distribution
        int hash1 = Arrays.hashCode(tokenBytes);
        int hash2 = token.hashCode() * 31 + token.length();
        int hash3 = token.hashCode() * 37 + tokenBytes.length;
        
        // Generate pseudo-random values using different seeds
        Random random1 = new Random(hash1);
        Random random2 = new Random(hash2);
        Random random3 = new Random(hash3);
        
        for (int i = 0; i < EMBEDDING_DIM; i++) {
            // Improved Gaussian distribution with better parameters
            float val1 = (float) (random1.nextGaussian() * 0.4f);
            float val2 = (float) (random2.nextGaussian() * 0.4f);
            float val3 = (float) (random3.nextGaussian() * 0.4f);
            
            // Mix the values for better distribution
            vector[i] = (val1 + val2 + val3) / 3.0f;
        }
        
        return vector;
    }

    /**
     * Normalize vector to unit length with better numerical stability
     */
    private static float[] normalizeVector(float[] vector) {
        float norm = 0.0f;
        
        // Calculate L2 norm
        for (float v : vector) {
            norm += v * v;
        }
        
        norm = (float) Math.sqrt(norm);
        
        if (norm > 1e-6f) { // Better numerical stability
            for (int i = 0; i < vector.length; i++) {
                vector[i] /= norm;
            }
        } else {
            log.warn("Vector norm too small ({}), returning zero vector", norm);
            Arrays.fill(vector, 0.0f);
        }
        
        return vector;
    }

    // Helper methods for debugging
    private static float calculateNorm(float[] vector) {
        float norm = 0.0f;
        for (float v : vector) {
            norm += v * v;
        }
        return (float) Math.sqrt(norm);
    }
    
    private static float calculateMean(float[] vector) {
        float sum = 0.0f;
        for (float v : vector) {
            sum += v;
        }
        return sum / vector.length;
    }
    
    private static int countNonZero(float[] vector) {
        int count = 0;
        for (float v : vector) {
            if (Math.abs(v) > 1e-6f) {
                count++;
            }
        }
        return count;
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
        
        float similarity = dotProduct / (float) (Math.sqrt(norm1) * Math.sqrt(norm2));
        log.debug("Calculated cosine similarity: {}", similarity);
        return similarity;
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

    /**
     * Debug method to print embedding info
     */
    public static void debugEmbedding(float[] embedding, String text) {
        log.info("=== EMBEDDING DEBUG for text: '{}' ===", text);
        log.info("Dimension: {}", embedding.length);
        log.info("Norm: {}", calculateNorm(embedding));
        log.info("Mean: {}", calculateMean(embedding));
        log.info("Non-zero elements: {}", countNonZero(embedding));
        log.info("First 10 values: {}", Arrays.toString(Arrays.copyOf(embedding, Math.min(10, embedding.length))));
    }
}