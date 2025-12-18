package com.egg.collector.my_egg_basket.service;

import com.egg.collector.my_egg_basket.domain.RealtimeData;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class KafkaProducerService {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${kafka.topic.realtime-stock}")
    private String topic;

    public void sendRealtimeData(RealtimeData data) {
        try {
            kafkaTemplate.send(topic, data.getStckShrnIscd(), data)
                    .whenComplete((result, ex) -> {
                        if (ex != null) {
                            log.error("Kafka 전송 실패: {}", ex.getMessage());
                        } else {
                            log.debug("Kafka 전송 성공: {} offset {}",
                                    data.getStckShrnIscd(),
                                    result.getRecordMetadata().offset());
                        }
                    });
        } catch (Exception e) {
            log.error("Kafka 전송 에러: {}", e.getMessage());
        }
    }
}