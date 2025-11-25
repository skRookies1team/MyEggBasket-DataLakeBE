
package com.egg.collector.my_egg_basket.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class AppConfig {

    /**
     * Spring Boot가 기본적으로 제공하는 ObjectMapper를 명시적으로 Bean으로 등록합니다.
     */
    @Bean
    public ObjectMapper objectMapper() {
        return new ObjectMapper();
    }
}