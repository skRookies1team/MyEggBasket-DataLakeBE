package com.egg.collector.my_egg_basket.service;

import com.egg.collector.my_egg_basket.domain.RealtimeData;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Service
@Slf4j
public class ArchiveService {

    private static final String ARCHIVE_DIR = "data-lake/archive/";
    private static final ZoneId TIMEZONE = ZoneId.of("Asia/Seoul");
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ISO_OFFSET_DATE_TIME;

    // CSV에 저장할 컬럼 순서 고정 (누락 방지)
    private static final String[] HEADERS = {
            "id", "timestamp", "stckShrnIscd", "stckCntgHour",
            "stckPrpr", "prdyVrss", "prdyCtrt",
            "acmlVol", "acmlTrPbmn",
            "askp1", "bidp1",
            "wghtAvrgPrc", "selnCntgCsnu", "shnuCntgCsnu",
            "totalAskpRsqn", "totalBidpRsqn", "isNegative"
    };

    private final ObjectMapper objectMapper;

    public ArchiveService(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    // [추가] 해당 날짜의 파일이 이미 존재하는지 확인
    public boolean isArchived(String dateStr) {
        File file = new File(ARCHIVE_DIR + "stock_data_" + dateStr + ".csv");
        // 파일이 존재하고 내용이 있으면 true
        return file.exists() && file.length() > 0;
    }

    public void archiveData(List<RealtimeData> dataList, String dateStr) {
        if (dataList == null || dataList.isEmpty()) return;

        File directory = new File(ARCHIVE_DIR);
        if (!directory.exists()) {
            directory.mkdirs();
        }

        String fileName = ARCHIVE_DIR + "stock_data_" + dateStr + ".csv";
        File file = new File(fileName);
        boolean isNewFile = !file.exists() || file.length() == 0;

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(file, true))) {
            // 새 파일이면 헤더 작성
            if (isNewFile) {
                writer.write(String.join(",", HEADERS));
                writer.write("\n");
            }

            for (RealtimeData data : dataList) {
                Map<String, Object> map = convertToMap(data);
                StringBuilder line = new StringBuilder();

                for (int i = 0; i < HEADERS.length; i++) {
                    if (i > 0) line.append(",");
                    Object val = map.get(HEADERS[i]);
                    line.append(escapeCsv(formatValue(val)));
                }
                line.append("\n");
                writer.write(line.toString());
            }
            log.debug("Archived {} records to {}", dataList.size(), fileName);

        } catch (IOException e) {
            log.error("Failed to archive data", e);
        }
    }

    private Map<String, Object> convertToMap(RealtimeData data) {
        return objectMapper.convertValue(data, new TypeReference<LinkedHashMap<String, Object>>() {});
    }

    private String formatValue(Object val) {
        if (val == null) return "";
        if (val instanceof Instant) {
            return TIME_FORMATTER.format(((Instant) val).atZone(TIMEZONE));
        }
        return val.toString();
    }

    private String escapeCsv(String s) {
        if (s == null || s.isEmpty()) return "";
        boolean needQuote = s.contains(",") || s.contains("\"") || s.contains("\n");
        String escaped = s.replace("\"", "\"\"");
        return needQuote ? "\"" + escaped + "\"" : escaped;
    }
}