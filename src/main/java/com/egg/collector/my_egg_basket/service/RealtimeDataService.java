package com.egg.collector.my_egg_basket.service;

import com.egg.collector.my_egg_basket.domain.RealtimeData;
import com.egg.collector.my_egg_basket.domain.RealtimeDataRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Slice;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

@Service
@RequiredArgsConstructor
@Slf4j
public class RealtimeDataService {

    private final RealtimeDataRepository realtimeDataRepository;
    private final ArchiveService archiveService;

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
        String dateStr = cutoffDate.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));

        int pageSize = 1000; // 한 번에 1000개씩 처리
        boolean hasNext = true;

        log.info("Starting archival and cleanup for data before {}", cutoffDate);

        while (hasNext) {
            // 1. 오래된 데이터 조회 (삭제하지 않고 조회만)
            // PageRequest.of(0, pageSize)를 계속 호출하는 이유:
            // 앞에서부터 삭제하면 뒤에 있던 데이터가 앞으로 밀려오기 때문입니다.
            // 하지만 여기서는 '조회 후 삭제' 트랜잭션이 아니므로,
            // 안전하게 Slice로 가져와서 처리 후 ID 기반 삭제를 하거나,
            // 데이터를 '백업 후 일괄 삭제' 하는 것이 좋습니다.
            //
            // 가장 간단한 전략: 1000개를 가져와서 -> 파일에 쓰고 -> 그 1000개를 지운다.

            Slice<RealtimeData> slice = realtimeDataRepository.findAllByTimestampBefore(
                    cutoffDate,
                    PageRequest.of(0, pageSize)
            );

            List<RealtimeData> contents = slice.getContent();

            if (contents.isEmpty()) {
                hasNext = false;
                break;
            }

            // 2. 다른 저장소(파일/DB)로 백업
            try {
                archiveService.archiveData(contents, dateStr);

                // 3. 백업 성공 시에만 MongoDB에서 삭제
                realtimeDataRepository.deleteAll(contents);

            } catch (Exception e) {
                log.error("Archiving failed. Stopping cleanup to prevent data loss.", e);
                break; // 백업 실패하면 삭제 중단
            }

            hasNext = slice.hasNext();
        }

        log.info("Cleanup and Archival process finished.");
    }
}