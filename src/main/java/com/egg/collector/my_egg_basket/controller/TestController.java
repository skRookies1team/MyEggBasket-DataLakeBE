package com.egg.collector.my_egg_basket.controller;

import com.egg.collector.my_egg_basket.domain.RealtimeData;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
public class TestController {


    @GetMapping("/test/kafka")
    public String testKafka() {
        RealtimeData testData = new RealtimeData();
        testData.setStckShrnIscd("005930"); // 삼성전자
        testData.setTimestamp("2025-12-18 17:10:00");
        testData.setStckPrpr(75000L);
        testData.setPrdyVrss(1000L);
        testData.setPrdyCtrt(1.35);
        testData.setAcmlVol(1000000L);

        return "Kafka test data sent!";
    }
}