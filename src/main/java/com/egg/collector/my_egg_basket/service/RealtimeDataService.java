package com.egg.collector.my_egg_basket.service;

import com.egg.collector.my_egg_basket.domain.RealtimeData;
import com.egg.collector.my_egg_basket.domain.RealtimeDataRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Slice;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

@Service
@RequiredArgsConstructor
@Slf4j
public class RealtimeDataService {

    private final RealtimeDataRepository realtimeDataRepository;
    private final ArchiveService archiveService;
    private final KafkaProducerService kafkaProducerService;

    /**
     * [변경됨] WebSocket에서 받은 데이터를 바로 Kafka로만 전송
     * MongoDB 저장은 KafkaConsumerService에서 처리
     */
    public void sendToKafka(RealtimeData data) {
        // 타임스탬프가 없으면 현재 시각 추가
        if (data.getTimestamp() == null) {
            String nowStr = LocalDateTime.now(ZoneId.of("Asia/Seoul"))
                    .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
            data.setTimestamp(nowStr);
        }

        try {
            // Kafka로 전송만 수행
            kafkaProducerService.sendRealtimeData(data);
            log.debug("📤 Kafka 전송 완료: {}", data.getStckShrnIscd());
        } catch (Exception e) {
            log.error("❌ Kafka 전송 실패: {}", e.getMessage(), e);
        }
    }

    /**
     * [변경] 최근 3일간의 데이터를 확인하여 아카이빙
     */
    public void archivePastDataIfNeeded() {
        LocalDate today = LocalDate.now(ZoneId.of("Asia/Seoul"));

        for (int i = 1; i <= 3; i++) {
            LocalDate targetDate = today.minusDays(i);
            String dateStr = targetDate.toString();

            if (archiveService.isArchived(dateStr)) {
                log.debug("Already archived for date: {}", dateStr);
                continue;
            }

            processArchiving(dateStr);
        }
    }

    private void processArchiving(String dateStr) {
        log.info("Checking & Archiving data for date: {}", dateStr);

        String startTimestamp = dateStr + " 00:00:00";
        String endTimestamp = dateStr + " 23:59:59";

        int page = 0;
        int batchSize = 1000;
        long totalProcessed = 0;

        while (true) {
            Slice<RealtimeData> slice = realtimeDataRepository.findAllByTimestampBetweenOrderByTimestampAsc(
                    startTimestamp, endTimestamp, PageRequest.of(page, batchSize)
            );

            if (!slice.hasContent()) {
                if (totalProcessed == 0) {
                    log.info("No data found for date: {} (Skipping creation)", dateStr);
                }
                break;
            }

            archiveService.archiveData(slice.getContent(), dateStr);
            totalProcessed += slice.getContent().size();

            if (!slice.hasNext()) {
                break;
            }
            page++;
        }

        if (totalProcessed > 0) {
            log.info("Completed archiving for {}: Total {} records.", dateStr, totalProcessed);
        }
    }
}