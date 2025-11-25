package com.egg.collector.my_egg_basket.service;

import com.egg.collector.my_egg_basket.domain.RealtimeData;
import com.egg.collector.my_egg_basket.domain.RealtimeDataRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.List;

@Service
@RequiredArgsConstructor
@Slf4j
public class RealtimeDataService {

    private final RealtimeDataRepository realtimeDataRepository;

    // 데이터 저장
    public RealtimeData save(RealtimeData data) {
        data.setTimestamp(LocalDateTime.now());
        try {
            return realtimeDataRepository.save(data);
        } catch (Exception e) {
            // 중복 키 에러 등 DB 저장 실패 시 로그만 남기고 무시
            log.error("Failed to save RealtimeData for {}: {}", data.getStckShrnIscd(), e.getMessage());
            return null;
        }
    }

    // 3일치 데이터 보존 로직 (스케줄링을 통해 주기적으로 실행)
    public void cleanupOldData() {
        LocalDateTime cutoffDate = LocalDateTime.now().minusDays(3);
        List<RealtimeData> deletedItems = realtimeDataRepository.deleteByTimestampBefore(cutoffDate);
        log.info("Cleanup completed. Deleted {} old records before {}", deletedItems.size(), cutoffDate);
    }
}