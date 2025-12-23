package com.egg.collector.my_egg_basket.service;

import com.egg.collector.my_egg_basket.domain.RealtimeData;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class KafkaConsumerService {

    private final RealtimeDataService realtimeDataService;

    @RetryableTopic(
            attempts = "3",
            backoff = @Backoff(delay = 1000, multiplier = 2.0),
            exclude = {NullPointerException.class}
    )
    @KafkaListener(
            topics = "${kafka.topic.realtime-stock}",
            groupId = "mongo-consumer-group-v1",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeRealtimeData(RealtimeData data, Acknowledgment ack) {
        try {
            log.info("Kafka 메시지 수신: {} at {}",
                    data.getStckShrnIscd(), data.getTimestamp());

            // MongoDB 저장
            RealtimeData saved = realtimeDataService.save(data);

            if (saved == null) {
                log.error("MongoDB 저장 실패 (null 반환): {}", data.getStckShrnIscd());
                throw new RuntimeException("Failed to save to MongoDB");
            }

            log.info("MongoDB 저장 완료: {} (ID: {})",
                    saved.getStckShrnIscd(), saved.getId());

            // 성공 시에만 오프셋 커밋
            ack.acknowledge();

        } catch (Exception e) {
            log.error("처리 실패 (재시도 예정): {} - {}",
                    data.getStckShrnIscd(), e.getMessage(), e);
            throw e;
        }
    }

    @DltHandler
    public void handleDlt(RealtimeData data,
                          @Header(KafkaHeaders.EXCEPTION_MESSAGE) String exceptionMessage) {
        log.error("DLT 처리 (최종 실패): {} - 예외: {}",
                data.getStckShrnIscd(), exceptionMessage);
    }
}
