package com.egg.collector.my_egg_basket.controller;

import com.egg.collector.my_egg_basket.domain.RealtimeData;
import com.egg.collector.my_egg_basket.service.KafkaProducerService;
import com.egg.collector.my_egg_basket.service.RealtimeDataService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;

@RestController
@RequiredArgsConstructor
public class TestController {

    private final KafkaProducerService kafkaProducerService;
    private final RealtimeDataService realtimeDataService;

    @GetMapping("/test/kafka")
    public String testKafka() {
        RealtimeData testData = new RealtimeData();
        testData.setStckShrnIscd("005930"); // 삼성전자
        testData.setTimestamp("2025-12-18 17:10:00");
        testData.setStckPrpr(75000L);
        testData.setPrdyVrss(1000L);
        testData.setPrdyCtrt(1.35);
        testData.setAcmlVol(1000000L);

        kafkaProducerService.sendRealtimeData(testData);

        return "Kafka test data sent!";
    }

    @GetMapping("/test/mongo/save-three")
    public String saveThreedRecords() {
        List<RealtimeData> dataList = new ArrayList<>();

        // 첫 번째 레코드
        RealtimeData data1 = new RealtimeData();
        data1.setId("692e314eafe5b33213de1e20");
        data1.setTimestamp("2025-12-02 09:22:33");
        data1.setStckShrnIscd("000660");
        data1.setStckCntgHour("092233");
        data1.setStckPrpr(551500L);
        data1.setPrdyVrss(13500L);
        data1.setPrdyCtrt(2.51);
        data1.setAcmlVol(659049L);
        data1.setAcmlTrPbmn(362935082500L);
        data1.setAskp1(552000L);
        data1.setBidp1(551000L);
        data1.setWghtAvrgPrc(550695L);
        data1.setSelnCntgCsnu(7651L);
        data1.setShnuCntgCsnu(9171L);
        data1.setTotalAskpRsqn(149968L);
        data1.setTotalBidpRsqn(47977L);
        data1.setNegative(false);
        dataList.add(data1);

        // 두 번째 레코드
        RealtimeData data2 = new RealtimeData();
        data2.setId("692e314eafe5b33213de1e23");
        data2.setTimestamp("2025-12-02 09:22:33");
        data2.setStckShrnIscd("000660");
        data2.setStckCntgHour("092233");
        data2.setStckPrpr(551500L);
        data2.setPrdyVrss(13500L);
        data2.setPrdyCtrt(2.51);
        data2.setAcmlVol(659063L);
        data2.setAcmlTrPbmn(362942803500L);
        data2.setAskp1(552000L);
        data2.setBidp1(551000L);
        data2.setWghtAvrgPrc(550695L);
        data2.setSelnCntgCsnu(7651L);
        data2.setShnuCntgCsnu(9173L);
        data2.setTotalAskpRsqn(150024L);
        data2.setTotalBidpRsqn(47977L);
        data2.setNegative(false);
        dataList.add(data2);

        // 세 번째 레코드
        RealtimeData data3 = new RealtimeData();
        data3.setId("692e314eafe5b33213de1e1e");
        data3.setTimestamp("2025-12-02 09:22:33");
        data3.setStckShrnIscd("005930");
        data3.setStckCntgHour("092233");
        data3.setStckPrpr(101500L);
        data3.setPrdyVrss(700L);
        data3.setPrdyCtrt(0.69);
        data3.setAcmlVol(2069265L);
        data3.setAcmlTrPbmn(209994618600L);
        data3.setAskp1(101600L);
        data3.setBidp1(101500L);
        data3.setWghtAvrgPrc(101482L);
        data3.setSelnCntgCsnu(4817L);
        data3.setShnuCntgCsnu(5086L);
        data3.setTotalAskpRsqn(429485L);
        data3.setTotalBidpRsqn(139562L);
        data3.setNegative(false);
        dataList.add(data3);

        // MongoDB에 저장
        for (RealtimeData data : dataList) {
            realtimeDataService.save(data);
        }

        return "3 records saved to MongoDB successfully!";
    }
}