package com.NLP2SparkSQL.project.service;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalog.Table;
import org.springframework.stereotype.Service;
import lombok.extern.slf4j.Slf4j;

import java.util.stream.Collectors;

/**
 * Service responsible for extracting the schema of all registered Spark SQL tables.
 * This schema can be used for prompt engineering or to provide context to an LLM.
 */
@Service
@Slf4j
public class SparkSchemaService {

    private final SparkSession spark;

    // Constructor injection of the SparkSession instance
    public SparkSchemaService(SparkSession spark) {
        this.spark = spark;
    }

    /**
     * Extracts the schema of all available tables in the Spark catalog.
     * For each table, retrieves the column names and types, and formats them in a readable way.
     *
     * @return A textual description of all tables and their columns in the format:
     *         DATABASE db_name -> TABLE table_name (column_name DATA_TYPE, ...)
     */
    public String getDatabaseSchemaDescription() {
        var tables = spark.catalog().listTables().collectAsList();
        StringBuilder schemaDescription = new StringBuilder();

        // Case: no tables found in Spark catalog
        if (tables.isEmpty()) {
            schemaDescription.append("No tables found in Spark catalog.\n");
            return schemaDescription.toString();
        }

        // Loop through all tables available in Spark
        for (Table table : tables) {
            String tableName = table.name();       // Get the table name
            String dbName = table.database();      // Get the database name

            try {
                // Try to access the table as a DataFrame
                Dataset<Row> df = spark.table(tableName);

                // Build a string: column_name DATA_TYPE
                String schemaStr = java.util.Arrays.stream(df.schema().fields())
                        .map(field -> field.name() + " " + field.dataType().simpleString().toUpperCase())
                        .collect(Collectors.joining(", "));

                // Format: DATABASE db -> TABLE table (col1 TYPE, col2 TYPE, ...)
                schemaDescription.append("DATABASE ").append(dbName).append(" -> ")
                                 .append("TABLE ").append(tableName)
                                 .append(" (").append(schemaStr).append(")\n");

            } catch (Exception e) {
                // Log if there's an error accessing or reading the table
                log.warn("Failed to read schema for table {}: {}", tableName, e.getMessage());
            }
        }

        return schemaDescription.toString();
    }
}
