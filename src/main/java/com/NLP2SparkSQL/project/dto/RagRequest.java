package com.NLP2SparkSQL.project.dto;

public class RagRequest {
    private String question;
    public RagRequest() {}
    public RagRequest(String question) { this.question = question; }
    public String getQuestion() { return question; }
    public void setQuestion(String question) { this.question = question; }
}
