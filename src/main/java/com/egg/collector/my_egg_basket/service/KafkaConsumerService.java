package com.egg.collector.my_egg_basket.service;

import com.egg.collector.my_egg_basket.domain.RealtimeData;
import com.egg.collector.my_egg_basket.domain.RealtimeDataRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.time.Instant;

@Service
@RequiredArgsConstructor
@Slf4j
public class KafkaConsumerService {

    private final RealtimeDataRepository realtimeDataRepository;
    private volatile Instant lastSavedAt = Instant.now();

    @KafkaListener(
            topics = "${kafka.topic.realtime-stock}",
            groupId = "datalake-group",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeRealtimeData(RealtimeData data) {
        try {
            log.debug("Kafka 메시지 수신: {} at {}",
                    data.getStckShrnIscd(), data.getTimestamp());

            // MongoDB에 저장
            RealtimeData saved = realtimeDataRepository.save(data);
            lastSavedAt = Instant.now();

            log.debug("MongoDB 저장 완료: ID={}", saved.getId());

        } catch (Exception e) {
            log.error("MongoDB 저장 실패: {}", e.getMessage(), e);
        }
    }

    public Instant getLastSavedAt() {
        return lastSavedAt;
    }
}