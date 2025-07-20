package com.NLP2SparkSQL.project.dto;

public class RagResponse {
    private String question;
    private String sql;
    private long timeTakenMs;

    public RagResponse() {}

    public RagResponse(String question, String sql, long timeTakenMs) {
        this.question = question;
        this.sql = sql;
        this.timeTakenMs = timeTakenMs;
    }

    public String getQuestion() {
        return question;
    }

    public void setQuestion(String question) {
        this.question = question;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public long getTimeTakenMs() {
        return timeTakenMs;
    }

    public void setTimeTakenMs(long timeTakenMs) {
        this.timeTakenMs = timeTakenMs;
    }
}
