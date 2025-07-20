package com.NLP2SparkSQL.project.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class QueryResponse {
    private String sparkSql;
    private String result;
    private long time;
}
