package com.egg.collector.my_egg_basket.service;

import com.egg.collector.my_egg_basket.domain.RealtimeData;
import com.egg.collector.my_egg_basket.domain.RealtimeDataRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Slice;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class RealtimeDataService {

    private final RealtimeDataRepository realtimeDataRepository;
    private final ArchiveService archiveService;

    // 초기값을 현재 시간으로 설정
    private volatile Instant lastSavedAt = Instant.now();

    // 임계값 50만 개
    private long batchThreshold = 500000;

    public void setBatchThreshold(long batchThreshold) {
        this.batchThreshold = batchThreshold;
    }

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

    // [최종 수정] 삭제 없이 조회만 하여 CSV로 저장 (중복 저장 주의)
    public void archiveBatchIfExceedsThreshold() {
        long currentCount = realtimeDataRepository.count();

        if (currentCount >= batchThreshold) {
            log.info("현재 데이터 {}개. 임계값({}) 도달! CSV 변환 시작 (DB 삭제 안함)", currentCount, batchThreshold);

            long targetToProcess = batchThreshold;
            long processedCount = 0;
            int batchSize = 1000;
            int page = 0; // 삭제하지 않으므로 페이지를 넘겨가며 조회해야 함

            while (processedCount < targetToProcess) {
                // page 변수를 사용해 다음 데이터를 계속 가져옴
                Slice<RealtimeData> slice = realtimeDataRepository.findAllByOrderByTimestampAsc(
                        PageRequest.of(page, batchSize)
                );

                if (!slice.hasContent()) {
                    break;
                }

                List<RealtimeData> contents = slice.getContent();

                // 날짜별로 CSV 저장
                Map<String, List<RealtimeData>> groupedByDate = contents.stream()
                        .collect(Collectors.groupingBy(d -> {
                            if (d.getTimestamp() != null && d.getTimestamp().length() >= 10) {
                                return d.getTimestamp().substring(0, 10);
                            }
                            return "unknown_date";
                        }));

                groupedByDate.forEach((dateStr, list) -> {
                    try {
                        archiveService.archiveData(list, dateStr);
                    } catch (Exception e) {
                        log.error("Failed to archive data for date: {}", dateStr, e);
                    }
                });

                // ★ 중요: deleteAll() 코드가 없으므로 DB 데이터는 안전합니다.

                processedCount += contents.size();
                page++; // 다음 페이지로 이동

                log.debug("Archived {} records (Page {})...", processedCount, page);
            }
            log.info("CSV 변환 완료. 총 {}개 처리됨. (데이터는 DB에 그대로 남음)", processedCount);
        } else {
            log.info("현재 데이터: {}개. 아직 임계값({})에 도달하지 않음.", currentCount, batchThreshold);
        }
    }
}