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
            RealtimeData saved = realtimeDataRepository.save(data);
            lastSavedAt = Instant.now();
            return saved;
        } catch (Exception e) {
            log.error("Failed to save RealtimeData: {}", e.getMessage());
            return null;
        }
    }

    // [변경] 어제 날짜의 CSV가 없으면 생성 (50만개 제한 로직 제거)
    public void archiveYesterdayDataIfNeeded() {
        // 한국 시간 기준 '어제' 날짜 계산
        LocalDate yesterday = LocalDate.now(ZoneId.of("Asia/Seoul")).minusDays(1);
        String dateStr = yesterday.toString(); // "2025-11-26" 형태

        // 1. 이미 파일이 존재하는지 확인
        if (archiveService.isArchived(dateStr)) {
            // 이미 처리되었으므로 스킵 (로그는 디버그 레벨로 줄여서 도배 방지)
            log.debug("Already archived for date: {}", dateStr);
            return;
        }

        log.info("Archiving data for date: {} (File not found)", dateStr);

        String startTimestamp = dateStr + " 00:00:00";
        String endTimestamp = dateStr + " 23:59:59";

        int page = 0;
        int batchSize = 1000; // 메모리 보호를 위해 읽을 때는 끊어서 읽음
        long totalProcessed = 0;

        while (true) {
            // 2. 어제 날짜 데이터 조회 (DB 삭제 안함)
            Slice<RealtimeData> slice = realtimeDataRepository.findAllByTimestampBetweenOrderByTimestampAsc(
                    startTimestamp, endTimestamp, PageRequest.of(page, batchSize)
            );

            if (!slice.hasContent()) {
                if (totalProcessed == 0) {
                    log.info("No data found for date: {}", dateStr);
                }
                break;
            }

            // 3. 파일에 이어 쓰기 (append)
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

    // 이전 메소드 호환성을 위해 남겨두거나 삭제 가능
    public void setBatchThreshold(long threshold) {}
    public void archiveBatchIfExceedsThreshold() {
        // 더 이상 사용하지 않음 -> archiveYesterdayDataIfNeeded 호출로 대체 권장
        archiveYesterdayDataIfNeeded();
    }
}