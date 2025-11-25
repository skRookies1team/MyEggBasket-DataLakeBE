package com.egg.collector.my_egg_basket.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;

@Configuration
@EnableMongoRepositories(basePackages = "com.egg.collector.my_egg_basket.domain")
public class MongoConfig {
    // application.properties의 설정을 기반으로 Spring Boot가 자동으로 연결을 처리합니다.
    // 추가적인 복잡한 설정은 여기에 정의할 수 있습니다.
}