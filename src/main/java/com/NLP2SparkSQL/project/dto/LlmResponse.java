package com.NLP2SparkSQL.project.dto;

public class LlmResponse {
    private String sparkSql;
    private long timeTakenMs;

    public LlmResponse() {}

    public LlmResponse(String sparkSql, long timeTakenMs) {
        this.sparkSql = sparkSql;
        this.timeTakenMs = timeTakenMs;
    }

    public String getSparkSql() {
        return sparkSql;
    }

    public void setSparkSql(String sparkSql) {
        this.sparkSql = sparkSql;
    }

    public long getTimeTakenMs() {
        return timeTakenMs;
    }

    public void setTimeTakenMs(long timeTakenMs) {
        this.timeTakenMs = timeTakenMs;
    }
}
