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

    public RealtimeData save(RealtimeData data) {
        if (data.getTimestamp() == null) {
            String nowStr = LocalDateTime.now(ZoneId.of("Asia/Seoul"))
                    .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
            data.setTimestamp(nowStr);
        }
        try {
            // MongoDB 저장
            RealtimeData saved = realtimeDataRepository.save(data);
            lastSavedAt = Instant.now();
            // Kafka 전송
            kafkaProducerService.sendRealtimeData(saved);
            return saved;
        } catch (Exception e) {
            log.error("Failed to save RealtimeData: {}", e.getMessage());
            return null;
        }
    }

    /**
     * [변경] 최근 3일간의 데이터를 확인하여 아카이빙
     * (예: 월요일 실행 시 -> 일, 토, 금 순서로 확인하여 금요일 데이터 누락 방지)
     */
    public void archivePastDataIfNeeded() {
        LocalDate today = LocalDate.now(ZoneId.of("Asia/Seoul"));

        // 1일 전부터 3일 전까지 순회
        for (int i = 1; i <= 3; i++) {
            LocalDate targetDate = today.minusDays(i);
            String dateStr = targetDate.toString(); // "2025-12-05"

            // 1. 이미 파일이 존재하는지 확인
            if (archiveService.isArchived(dateStr)) {
                log.debug("Already archived for date: {}", dateStr);
                continue; // 이미 있으면 다음 날짜 확인
            }

            // 2. 해당 날짜 아카이빙 수행
            processArchiving(dateStr);
        }
    }

    // 아카이빙 실제 로직 분리
    private void processArchiving(String dateStr) {
        log.info("Checking & Archiving data for date: {}", dateStr);

        String startTimestamp = dateStr + " 00:00:00";
        String endTimestamp = dateStr + " 23:59:59";

        int page = 0;
        int batchSize = 1000;
        long totalProcessed = 0;

        while (true) {
            // DB 조회
            Slice<RealtimeData> slice = realtimeDataRepository.findAllByTimestampBetweenOrderByTimestampAsc(
                    startTimestamp, endTimestamp, PageRequest.of(page, batchSize)
            );

            if (!slice.hasContent()) {
                if (totalProcessed == 0) {
                    log.info("No data found for date: {} (Skipping creation)", dateStr);
                }
                break;
            }

            // 파일 저장
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

    // 이전 호환성 유지 (필요 없다면 삭제 가능)
    public void setBatchThreshold(long threshold) {}
    public void archiveBatchIfExceedsThreshold() {
        archivePastDataIfNeeded();
    }
}