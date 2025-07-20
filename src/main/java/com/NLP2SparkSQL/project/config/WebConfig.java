package com.NLP2SparkSQL.project.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.http.MediaType;
import org.springframework.http.converter.HttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import java.util.ArrayList;
import java.util.List;

@Configuration
public class WebConfig implements WebMvcConfigurer {

    @Override
    public void extendMessageConverters(List<HttpMessageConverter<?>> converters) {
        // Recherche le convertisseur Jackson existant
        for (HttpMessageConverter<?> converter : converters) {
            if (converter instanceof MappingJackson2HttpMessageConverter jacksonConverter) {
                // On ajoute text/plain en type supporté
                List<MediaType> types = new ArrayList<>(jacksonConverter.getSupportedMediaTypes());
                types.add(MediaType.TEXT_PLAIN);
                jacksonConverter.setSupportedMediaTypes(types);
            }
        }
    }
}
