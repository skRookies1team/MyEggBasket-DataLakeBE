package com.egg.collector.my_egg_basket.service;

import com.egg.collector.my_egg_basket.domain.RealtimeData;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
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
            include = {DataAccessException.class}
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

            // RealtimeDataService를 통해 MongoDB에 저장
            realtimeDataService.save(data);

            log.info("MongoDB 저장 완료: {}", data.getStckShrnIscd());

            // 성공 시에만 오프셋 커밋
            ack.acknowledge();

        } catch (DataAccessException e) {
            log.error("MongoDB 저장 실패 (재시도 예정): {}", e.getMessage());
            throw e;
        } catch (Exception e) {
            log.error("처리 불가 예외 발생: {}", e.getMessage(), e);
            // 그 외 예외는 재시도하지 않고 즉시 DLT로 보내기 위해 ack 처리
            ack.acknowledge();
        }
    }

    @DltHandler
    public void handleDlt(RealtimeData data,
                          @Header(KafkaHeaders.EXCEPTION_MESSAGE) String exceptionMessage) {
        log.error("DLT 처리: {} - 예외: {}", data.getStckShrnIscd(), exceptionMessage);
        // TODO: 별도의 에러 로깅 DB 또는 알림 시스템에 기록
    }
}
