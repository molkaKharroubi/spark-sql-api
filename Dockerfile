# ----  Build Stage (Maven + Java 17) ----
FROM maven:3.9.5-eclipse-temurin-17 AS build
WORKDIR /app

# Copy pom.xml and download dependencies (cache Maven)
COPY pom.xml .
RUN mvn dependency:go-offline  

# Copy source code and build the application
COPY src ./src
RUN mvn clean package -DskipTests

# ----  Final Stage (Lightweight JDK) ----
FROM openjdk:17-jdk-slim
WORKDIR /app

# Copy the generated JAR from the build stage
COPY --from=build /app/target/project-0.0.1-SNAPSHOT.jar app.jar

# Expose port 8080 (default for Spring Boot)
EXPOSE 8080

# Start the Spring Boot application
ENTRYPOINT ["java","-jar","app.jar"]
