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
services:
  qdrant:
    image: molka001/qdrant-data:v3   
    container_name: qdrant_data
    ports:
      - "6333:6333"
      - "6334:6334"
    restart: unless-stopped
    networks:
      - app-network

  spark-sql-api:
    image: molka001/spark-sql-api:v2
    container_name: spark-sql-api
    depends_on:
      - qdrant
    ports:
      - "8080:8080"
    environment:
      - QDRANT_URL=http://qdrant:6333
    restart: unless-stopped
    volumes:
      - ./sparkSQL_data:/sparkSQL/storage   
    networks:
      - app-network

networks:
  app-network:
    driver: bridge
```

✅ This setup:

* Runs **Qdrant** and **Spark SQL API** in the same network.
* Exposes Qdrant on ports **6333** & **6334** and the API on **8080**.
* Mounts a local volume for **Spark SQL data persistence**.
* Uses `depends_on` so **Spark SQL API** starts after Qdrant.

### ▶️ Run the services

```bash
docker-compose up -d
```

---

## 📊 Deployment Architecture

![Deployment Architecture](https://i.imgur.com/YOUR-DEPLOYMENT-IMAGE.png)

---

## 📜 License

This project is licensed under the **MIT License**.

---

## 🤝 Contributing

Contributions are welcome! Feel free to fork this repo, open issues, or submit pull requests.
