# NLP to Spark SQL API

A modular, intelligent Spring Boot application that transforms **natural language questions into Spark SQL queries**, powered by **LangChain4J**, **Ollama**, and **Qdrant**.

---

## Overview

This Java Spring Boot API enables users to write natural language questions and receive Spark SQL queries as output. It leverages:

- **LangChain4J**: for LLM orchestration (query understanding and prompt management).
- **Ollama (Qwen3: 1.7b)**: local LLM model to generate Spark SQL code.
- **Qdrant**: vector database for semantic document retrieval using embeddings.
- **Apache Spark**: executes generated Spark SQL queries and returns results.
- **Spring Boot (3.5.3)**: RESTful API, config management, and service layers.

---


## Features

- Natural language to Spark SQL translation
- Local LLM support (Qwen3 via Ollama)
- Semantic context search using Qdrant
- Retrieval-Augmented Generation (RAG)
- Spark SQL execution of the generated queries
- Swagger/OpenAPI documentation

---

## Dependencies

- Java 17
- Spring Boot 3.5.3
- Apache Spark 3.4.1
- LangChain4J (v0.24.0)
- Ollama LLM (`qwen3:1.7b`)
- Qdrant
- Spring WebFlux + Web MVC
- Swagger UI for API documentation

---


## Setup & Run

git clone https://github.com/your-username/NLP2SparkSQL.git](https://github.com/molkaKharroubi/spark-sql-api.git

mvn clean install

mvn spring-boot:run

## Access API

API Base URL: http://localhost:8080

Swagger UI: http://localhost:8080/swagger-ui/index.html
