package com.egg.collector.my_egg_basket.service;

import com.egg.collector.my_egg_basket.domain.RealtimeData;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.format.DateTimeFormatter;
import java.util.List;

@Service
@Slf4j
public class ArchiveService {

    private static final String ARCHIVE_DIR = "data-lake/archive/"; // 파일 저장 경로

    public void archiveData(List<RealtimeData> dataList, String dateStr) {
        if (dataList.isEmpty()) return;

        File directory = new File(ARCHIVE_DIR);
        if (!directory.exists()) {
            directory.mkdirs();
        }

        // 파일명 예시: stock_data_2025-11-22.csv
        String fileName = ARCHIVE_DIR + "stock_data_" + dateStr + ".csv";

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(fileName, true))) {
            // 헤더가 없는 경우에만 헤더 작성 (파일이 처음 생성될 때)
            File file = new File(fileName);
            if (file.length() == 0) {
                writer.write("timestamp,code,price,volume,change_rate\n"); // 필요한 ML 피쳐들
            }

            for (RealtimeData data : dataList) {
                // CSV 포맷으로 변환 (콤마로 구분)
                String line = String.format("%s,%s,%d,%d,%.2f\n",
                        data.getTimestamp(),
                        data.getStckShrnIscd(),
                        data.getStckPrpr(),
                        data.getAcmlVol(),
                        data.getPrdyCtrt()
                );
                writer.write(line);
            }
            log.info("Archived {} records to {}", dataList.size(), fileName);

        } catch (IOException e) {
            log.error("Failed to archive data", e);
            throw new RuntimeException("Archiving failed"); // 백업 실패 시 삭제 막기 위해 예외 발생
        }
    }
}