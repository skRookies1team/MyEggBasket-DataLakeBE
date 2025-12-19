package com.egg.collector.my_egg_basket.service;

import com.egg.collector.my_egg_basket.domain.RealtimeData;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.function.Consumer;

@Slf4j
public class StockWebSocketHandler extends TextWebSocketHandler {

    private final String approvalKey;
    private final String[] stockCodes;
    private final String trId;
    private final RealtimeDataService dataService;
    private final ObjectMapper objectMapper;
    private final Consumer<Void> onCloseCallback;

    public StockWebSocketHandler(String approvalKey, String[] stockCodes, String trId, RealtimeDataService dataService, ObjectMapper objectMapper, Consumer<Void> onCloseCallback) {
        this.approvalKey = approvalKey;
        this.stockCodes = stockCodes;
        this.trId = trId;
        this.dataService = dataService;
        this.objectMapper = objectMapper;
        this.onCloseCallback = onCloseCallback;
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession session) throws Exception {
        log.info("WebSocket connection established. Subscribing to {} stocks...", stockCodes.length);
        for (String code : stockCodes) {
            if (!session.isOpen()) {
                log.warn("Session closed, stopping subscription.");
                break;
            }

            try {
                session.sendMessage(new TextMessage(createSubscribeMessage(code)));
                Thread.sleep(300);
            } catch (IOException e) {
                log.error("Error sending subscription for {}: {}", code, e.getMessage());
                break;
            }
        }
    }

    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) throws Exception {
        String payload = message.getPayload();
        String[] parts = payload.split("\\|");

        if (parts.length > 3) {
            String dataPart = parts[3];
            String[] values = dataPart.split("\\^");

            try {
                String stockCode = values[0];
                String timeStr = values[1];

                LocalDate kstDate = LocalDate.now(ZoneId.of("Asia/Seoul"));
                LocalTime kstTime = LocalTime.parse(timeStr, DateTimeFormatter.ofPattern("HHmmss"));
                LocalDateTime finalTimestamp = LocalDateTime.of(kstDate, kstTime);
                String timestampStr = finalTimestamp.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

                RealtimeData data = new RealtimeData();
                data.setTimestamp(timestampStr);
                data.setStckShrnIscd(stockCode);
                data.setStckCntgHour(timeStr);

                data.setStckPrpr(parseDoubleAsLong(values, 2));
                data.setPrdyVrss(parseDoubleAsLong(values, 4));
                data.setPrdyCtrt(parseDouble(values, 5));
                data.setWghtAvrgPrc(parseDoubleAsLong(values, 6));

                data.setAskp1(parseDoubleAsLong(values, 10));
                data.setBidp1(parseDoubleAsLong(values, 11));

                data.setAcmlVol(parseDoubleAsLong(values, 13));
                data.setAcmlTrPbmn(parseDoubleAsLong(values, 14));

                data.setSelnCntgCsnu(parseDoubleAsLong(values, 15));
                data.setShnuCntgCsnu(parseDoubleAsLong(values, 16));

                data.setTotalAskpRsqn(parseDoubleAsLong(values, 38));
                data.setTotalBidpRsqn(parseDoubleAsLong(values, 39));

                data.setNegative(data.getPrdyCtrt() < 0);

                // [수정] 데이터를 Kafka로만 전송
                dataService.sendToKafka(data);

            } catch (Exception e) {
                log.error("Parsing error: {}", e.getMessage());
            }
        }
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        log.warn("WebSocket connection closed: {}", status);
        if (onCloseCallback != null) onCloseCallback.accept(null);
    }

    private Long parseDoubleAsLong(String[] values, int index) {
        if (index >= values.length) return 0L;
        String str = values[index].replace(",", "").trim();
        if (str.isEmpty()) return 0L;
        try { return (long) Double.parseDouble(str); } catch (Exception e) { return 0L; }
    }

    private Double parseDouble(String[] values, int index) {
        if (index >= values.length) return 0.0;
        String str = values[index].replace(",", "").trim();
        if (str.isEmpty()) return 0.0;
        try { return Double.parseDouble(str); } catch (Exception e) { return 0.0; }
    }

    private String createSubscribeMessage(String stockCode) throws JsonProcessingException {
        String cleanCode = stockCode.trim();
        Map<String, Object> body = Map.of("input", Map.of("tr_id", trId, "tr_key", stockCode));
        Map<String, Object> header = Map.of("approval_key", approvalKey, "custtype", "P", "tr_type", "1", "content-type", "utf-8");
        return objectMapper.writeValueAsString(Map.of("header", header, "body", body));
    }
}
