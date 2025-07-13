package com.NLP2SparkSQL.project.dto;

import jakarta.validation.constraints.NotBlank;
import lombok.Data;

@Data
public class SQLContextualRequest {

    @NotBlank(message = "Question must not be blank")
    private String question;

    @NotBlank(message = "Spark context must not be blank")
    private String sparkContext;
}
