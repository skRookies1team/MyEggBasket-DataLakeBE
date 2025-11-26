package com.egg.collector.my_egg_basket.service;

import com.egg.collector.my_egg_basket.domain.RealtimeData;
import com.egg.collector.my_egg_basket.domain.RealtimeDataRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Slice;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;

@Service
@RequiredArgsConstructor
@Slf4j
public class RealtimeDataService {

    private final RealtimeDataRepository realtimeDataRepository;
    private final ArchiveService archiveService;

    public RealtimeData save(RealtimeData data) {
        // [수정됨] 타임스탬프가 비어있으면 현재 KST 시간으로 문자열 포맷팅해서 저장
        if (data.getTimestamp() == null) {
            String nowStr = LocalDateTime.now(ZoneId.of("Asia/Seoul"))
                    .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
            data.setTimestamp(nowStr);
        }

        try {
            return realtimeDataRepository.save(data);
        } catch (Exception e) {
            log.error("Failed to save RealtimeData for {}: {}", data.getStckShrnIscd(), e.getMessage());
            return null;
        }
    }

    // 3일치 데이터 정리 로직
    public void cleanupOldData() {
        // [수정됨] 비교 기준도 String으로 변환
        LocalDateTime cutoffDate = LocalDateTime.now(ZoneId.of("Asia/Seoul")).minusDays(3);
        String cutoffStr = cutoffDate.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        String archiveDateStr = cutoffDate.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));

        int pageSize = 1000;
        boolean hasNext = true;

        log.info("Starting archival and cleanup for data before {}", cutoffStr);

        while (hasNext) {
            // String 타입이어도 날짜 포맷이 일정하면 대소비교(Before) 가능
            Slice<RealtimeData> slice = realtimeDataRepository.findAllByTimestampBefore(
                    cutoffStr,
                    PageRequest.of(0, pageSize)
            );

            List<RealtimeData> contents = slice.getContent();

            if (contents.isEmpty()) {
                break;
            }

            try {
                archiveService.archiveData(contents, archiveDateStr);
                realtimeDataRepository.deleteAll(contents);
            } catch (Exception e) {
                log.error("Archiving failed. Stopping cleanup.", e);
                break;
            }

            hasNext = slice.hasNext();
        }
        log.info("Cleanup process finished.");
    }
}