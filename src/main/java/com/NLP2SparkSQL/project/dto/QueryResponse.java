package com.NLP2SparkSQL.project.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class QueryResponse {
    private String sparkSql;
    private String result;
}
