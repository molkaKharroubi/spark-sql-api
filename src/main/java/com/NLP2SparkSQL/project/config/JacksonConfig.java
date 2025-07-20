package com.NLP2SparkSQL.project.config;

import com.fasterxml.jackson.core.JsonParser;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder;

@Configuration
public class JacksonConfig {

    @Bean
    public Jackson2ObjectMapperBuilder jacksonBuilder() {
        return new Jackson2ObjectMapperBuilder()
            // Autorise les retours à la ligne non échappés (ASCII < 32)
            .featuresToEnable(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS);
    }
}
