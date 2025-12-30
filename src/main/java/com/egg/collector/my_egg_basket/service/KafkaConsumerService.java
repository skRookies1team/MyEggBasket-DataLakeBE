package com.egg.collector.my_egg_basket.service;

import com.egg.collector.my_egg_basket.domain.RealtimeData;
import com.egg.collector.my_egg_basket.domain.RealtimeDataRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
@RequiredArgsConstructor
@Slf4j
public class KafkaConsumerService {

    private final RealtimeDataRepository realtimeDataRepository;

    // 종목별로 최신 데이터를 담을 버퍼 (실시간 수신되는 데이터를 여기 보관)
    private final Map<String, RealtimeData> dataBuffer = new ConcurrentHashMap<>();

    private volatile Instant lastSavedAt = Instant.now();

    /**
     * Kafka로부터 실시간 주식 데이터를 수신합니다.
     * 그룹 ID는 application.properties의 설정을 참조하도록 수정되었습니다.
     */
    @KafkaListener(
            topics = "${kafka.topic.realtime-stock}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeRealtimeData(RealtimeData data) {
        try {
            // 1. Python에서 보내지 않는 timestamp(yyyy-MM-dd HH:mm:ss) 생성 및 설정
            // stckCntgHour(HHmmss)를 사용하여 전체 타임스탬프를 구성합니다.
            if (data.getTimestamp() == null || data.getTimestamp().isEmpty()) {
                String today = LocalDate.now(ZoneId.of("Asia/Seoul")).toString();
                String rawTime = data.getStckCntgHour(); // 예: "153000"

                if (rawTime != null && rawTime.length() == 6) {
                    String formattedTime = String.format("%s:%s:%s",
                            rawTime.substring(0, 2),
                            rawTime.substring(2, 4),
                            rawTime.substring(4, 6));
                    data.setTimestamp(today + " " + formattedTime);
                }
            }

            // 2. 수신된 데이터를 즉시 저장하지 않고 메모리 버퍼에 업데이트 (종목코드를 키로 사용)
            // 이렇게 하면 해당 분의 가장 마지막(최신) 데이터만 유지되어 DB 부하를 줄입니다.
            dataBuffer.put(data.getStckShrnIscd(), data);
            log.debug("버퍼 업데이트 완료: 종목={}, 가격={}", data.getStckShrnIscd(), data.getStckPrpr());

        } catch (Exception e) {
            log.error("데이터 수신 및 가공 중 오류 발생: {}", e.getMessage());
        }
    }

    /**
     * 매 분 0초마다 실행되어 버퍼의 내용을 MongoDB에 일괄 저장합니다.
     */
    @Scheduled(cron = "0 * * * * *")
    public void saveBufferedData() {
        if (dataBuffer.isEmpty()) {
            log.debug("저장할 버퍼 데이터가 없습니다.");
            return;
        }

        try {
            // 버퍼에 쌓인 데이터들을 리스트로 변환 후 일괄 저장
            List<RealtimeData> toSave = new ArrayList<>(dataBuffer.values());
            int count = toSave.size();

            realtimeDataRepository.saveAll(toSave); // Bulk Insert 수행

            // 저장 후 버퍼 비우기 및 상태 업데이트
            dataBuffer.clear();
            lastSavedAt = Instant.now();

            log.info("분 단위 MongoDB 저장 완료: {} 건 저장됨", count);
        } catch (Exception e) {
            log.error("분 단위 MongoDB 저장 실패: {}", e.getMessage(), e);
        }
    }

    public Instant getLastSavedAt() {
        return lastSavedAt;
    }
}