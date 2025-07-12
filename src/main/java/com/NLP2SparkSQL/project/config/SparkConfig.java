package com.NLP2SparkSQL.project.config;

import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class SparkConfig {

    @Value("${spark.master:spark://spark-master:7077}")
    private String sparkMaster;

    @Value("${spark.app.name:NLP2SparkApp}")
    private String appName;

    @Bean
    public SparkSession sparkSession() {
        return SparkSession.builder()
                .appName(appName)
                .master(sparkMaster)
                .getOrCreate();
    }
}
