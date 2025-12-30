package com.egg.collector.my_egg_basket.service;

import com.egg.collector.my_egg_basket.domain.RealtimeDataRepository;
import com.egg.collector.my_egg_basket.domain.RealtimeData;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Slice;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.ZoneId;

@Service
@RequiredArgsConstructor
@Slf4j
public class RealtimeDataService {

    private final RealtimeDataRepository realtimeDataRepository;
    private final ArchiveService archiveService;

    // [수정] KafkaProducerService 관련 의존성 및 sendToKafka 메서드 완전 제거

    /**
     * 최근 3일간의 데이터를 확인하여 아카이빙 수행
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

            if (!slice.hasContent()) break;

            archiveService.archiveData(slice.getContent(), dateStr);
            totalProcessed += slice.getContent().size();

            if (!slice.hasNext()) break;
            page++;
        }
        if (totalProcessed > 0) {
            log.info("Completed archiving for {}: Total {} records.", dateStr, totalProcessed);
        }
    }
}