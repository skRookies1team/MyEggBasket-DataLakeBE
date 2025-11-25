package com.egg.collector.my_egg_basket;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling; // 💡 추가

@SpringBootApplication
@EnableScheduling // 💡 스케줄링 활성화 (데이터 정리, 재접속 등에 사용)
public class MyEggBasketApplication {

	public static void main(String[] args) {
		SpringApplication.run(MyEggBasketApplication.class, args);
	}

}