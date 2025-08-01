# NLP to Spark SQL API

A **modular** and **intelligent** Spring Boot API that converts **natural language questions into Spark SQL queries**, leveraging **LangChain4J**, **Ollama**, and **Qdrant**.

---

## 🚀 Overview

This Java Spring Boot API empowers users to write natural language questions and instantly get **Spark SQL queries** with execution-ready results.

### 🔧 Core Components

* **LangChain4J** – Orchestrates the LLM, manages prompts, and contextual understanding. 
* **Ollama (Qwen3:1.7B)** – Local LLM for generating Spark SQL queries.
* **Qdrant** – Vector database for semantic search & context retrieval.
* **Apache Spark** – Executes the generated SQL queries on data.
* **Spring Boot 3.5.3** – Provides REST API endpoints, config, and service layers.

---

## ✨ Features

✅ Translate natural language into Spark SQL 

✅ Local LLM support with **Qwen3** via Ollama

✅ Semantic search using **Qdrant**

✅ Retrieval-Augmented Generation (RAG)

✅ Spark SQL execution of generated queries

✅ **Swagger UI** for interactive API documentation

---

## 📦 Dependencies

* ☕ **Java 17**
* 🌱 **Spring Boot 3.5.3**
* 🔗 **LangChain4J v0.24.0**
* 🤖 **Ollama LLM (`qwen3:1.7b`)**
* 📦 **Qdrant** (Vector Database)
* 🌐 **Spring WebFlux & Web MVC**
* 📄 **Swagger UI**

---

## ⚙️ Setup & Run

### 1️⃣ Clone the repository

```bash
git clone https://github.com/molkaKharroubi/spark-sql-api.git
cd spark-sql-api
```

### 2️⃣ Build the project

```bash
mvn clean install
```

### 3️⃣ Run the Spring Boot application

```bash
mvn spring-boot:run
```

### 4️⃣ Access API

👉 [**Swagger UI**](http://localhost:8080/swagger-ui/index.html)

---

## 🐳 Docker Deployment

Here is the **Docker Compose** configuration for running the stack:

```yaml
version: '3.8'

services:
  qdrant:
    image: molka001/qdrant-data:v3
    container_name: qdrant_data
    ports:
      - "6333:6333"
      - "6334:6334"
    networks:
      - app-network
    restart: unless-stopped

  ollama:
    image: ollama/ollama:latest
    container_name: ollama
    ports:
      - "11434:11434"
    volumes:
      - ollama_data:/root/.ollama    # ✅ on garde juste ce volume
    networks:
      - app-network
    restart: unless-stopped
    entrypoint: |
      /bin/sh -c "
      ollama serve &
      sleep 5 &&
      ollama pull qwen3:1.7b &&
      wait
      "

  spark-sql-api:
    image: molka001/spark-sql-api:v3
    container_name: spark-sql-api
    depends_on:
      - qdrant
      - ollama
    ports:
      - "8080:8080"
    environment:
      - QDRANT_URL=http://qdrant:6333
      - OLLAMA_URL=http://ollama:11434
      - OLLAMA_MODEL=qwen3:1.7b
    volumes:
      - ./sparkSQL_data:/sparkSQL/storage
    networks:
      - app-network
    restart: unless-stopped

volumes:
  ollama_data:

networks:
  app-network:
    driver: bridge
```

✅ This setup:

* Connects Qdrant, Ollama, and Spark SQL API via a shared Docker network.

* Exposes Qdrant on ports 6333 & 6334, Ollama on 11434, and the API on 8080.

* Uses depends_on to start Spark SQL API after Qdrant and Ollama are ready.

* Persists Ollama model data and Spark SQL storage with Docker volumes.
### ▶️ Run the services

```bash
docker-compose up -d
```
