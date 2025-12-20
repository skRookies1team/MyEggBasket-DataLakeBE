package com.egg.collector.my_egg_basket.service;

import com.egg.collector.my_egg_basket.domain.RealtimeData;
import com.egg.collector.my_egg_basket.domain.RealtimeDataRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Slice;
import org.springframework.stereotype.Service;

import java.time.Instant;
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

    private volatile Instant lastSavedAt = Instant.now();

    public Instant getLastSavedAt() {
        return lastSavedAt;
    }

    /**
     * Kafka로 데이터 전송 (DB 저장 안 함)
     * 웹소켓 핸들러에서 호출
     */
    public void sendToKafka(RealtimeData data) {
        if (data.getTimestamp() == null) {
            String nowStr = LocalDateTime.now(ZoneId.of("Asia/Seoul"))
                    .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
            data.setTimestamp(nowStr);
        }
        kafkaProducerService.sendRealtimeData(data);
    }

    /**
     * MongoDB에 데이터 저장 (Kafka 전송 안 함)
     * Kafka Consumer에서 호출
     */
    public RealtimeData save(RealtimeData data) {
        try {
            log.debug("MongoDB 저장 시도: {}", data.getStckShrnIscd());
            
            // MongoDB 저장
            RealtimeData saved = realtimeDataRepository.save(data);
            lastSavedAt = Instant.now();
            
            log.debug("MongoDB 저장 성공: {} (ID: {})", 
                    saved.getStckShrnIscd(), saved.getId());
            
            return saved;
        } catch (Exception e) {
            log.error("MongoDB 저장 실패: {} - {}", 
                    data.getStckShrnIscd(), e.getMessage(), e);
            throw e;  // 예외를 다시 던져서 Consumer가 재시도할 수 있게
        }
    }

    /**
     * [변경] 최근 3일간의 데이터를 확인하여 아카이빙
     */
    public void archivePastDataIfNeeded() {
        LocalDate today = LocalDate.now(ZoneId.of("Asia/Seoul"));

        // 1일 전부터 3일 전까지 순회
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
        }4

        if (totalProcessed > 0) {
            log.info("Completed archiving for {}: Total {} records.", dateStr, totalProcessed);
        }
    }

    public void setBatchThreshold(long threshold) {}
    public void archiveBatchIfExceedsThreshold() {
        archivePastDataIfNeeded();
    }
}
