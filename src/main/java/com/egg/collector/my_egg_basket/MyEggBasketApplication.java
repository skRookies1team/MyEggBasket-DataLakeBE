package com.egg.collector.my_egg_basket;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling; // 추가

@EnableScheduling // 추가
@SpringBootApplication
public class MyEggBasketApplication {
    public static void main(String[] args) {
        SpringApplication.run(MyEggBasketApplication.class, args);
    }
}