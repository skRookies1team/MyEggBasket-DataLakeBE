package com.egg.collector.my_egg_basket.service;

import com.egg.collector.my_egg_basket.domain.RealtimeData;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.io.IOException;
import java.util.Map;
import java.util.function.Consumer;

import static com.egg.collector.my_egg_basket.service.WebSocketClient.FIELD_MAP;

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
        
        // 연결 성공 후 모든 종목 구독 요청 전송
        for (String code : stockCodes) {
            String subscribeMessage = createSubscribeMessage(code);
            session.sendMessage(new TextMessage(subscribeMessage));
            log.debug("Subscribed to: {}", code);
        }
    }

    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) throws Exception {
        String payload = message.getPayload();
        
        // 0|H0ST0000|005930|... -> 데이터 메시지
        if (payload.startsWith("0|") || payload.startsWith("1|")) {
            RealtimeData data = parseRealtimeData(payload);
            if (data != null) {
                // 데이터 파싱 성공 시 MongoDB에 저장
                dataService.save(data);
                // 💡 추가 구현: 이 데이터를 프론트엔드로 브로드캐스트하는 로직 (예: STOMP/Redis)이 여기에 추가됩니다.
            }
        } 
        // System message (JSON format, e.g., connection confirmation)
        else if (payload.startsWith("{")) {
            try {
                JsonNode jsonNode = objectMapper.readTree(payload);
                log.info("System Message: {}", jsonNode.toPrettyString());
            } catch (JsonProcessingException e) {
                log.warn("Failed to parse system message as JSON: {}", payload);
            }
        }
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        log.warn("WebSocket connection closed. Status: {}", status);
        onCloseCallback.accept(null); // 재접속 로직 호출
    }

    // useRealtimeStock.ts의 parseRealtimeData 로직을 Java로 구현
    private RealtimeData parseRealtimeData(String message) {
        // 메시지 구조: 접두(0/1)|TR_ID|TR_KEY|데이터
        String[] parts = message.split("\\|");
        if (parts.length < 4) return null;

        String trKey = parts[2]; // 종목 코드가 포함됨
        String dataString = parts[3];

        String[] dataFields = dataString.split("\\^");
        if (dataFields.length == 0) return null;

        RealtimeData data = new RealtimeData();
        data.setStck_shrn_iscd(trKey); // TR_KEY를 종목 코드로 사용

        // 필드 추출 및 설정 (H0STCNT0_FIELD_MAP 참고)
        try {
            data.setStck_cntg_hour(dataFields[FIELD_MAP.get("stck_cntg_hour")]);

            // 시그널 처리: dataFields[2]에 코드가 들어옴 (useRealtimeStock.ts 기준)
            String signField = safeGet(dataFields, 2);
            boolean isNegative = "5".equals(signField) || "4".equals(signField);

            data.setStck_prpr(toNumber(dataFields, FIELD_MAP.get("stck_prpr")));
            data.setPrdy_vrss(toNumber(dataFields, FIELD_MAP.get("prdy_vrss")));
            data.setPrdy_ctrt(toDouble(dataFields, FIELD_MAP.get("prdy_ctrt")));
            data.setAcml_vol(toNumber(dataFields, FIELD_MAP.get("acml_vol")));
            data.setAskp1(toNumber(dataFields, FIELD_MAP.get("askp1")));
            data.setBidp1(toNumber(dataFields, FIELD_MAP.get("bidp1")));
            data.setWght_avrg_prc(toNumber(dataFields, FIELD_MAP.get("wght_avrg_prc")));
            data.setAcml_tr_pbmn(toNumber(dataFields, FIELD_MAP.get("acml_tr_pbmn")));
            data.setSeln_cntg_csnu(toNumber(dataFields, FIELD_MAP.get("seln_cntg_csnu")));
            data.setShnu_cntg_csnu(toNumber(dataFields, FIELD_MAP.get("shnu_cntg_csnu")));
            data.setTotal_askp_rsqn(toNumber(dataFields, FIELD_MAP.get("total_askp_rsqn")));
            data.setTotal_bidp_rsqn(toNumber(dataFields, FIELD_MAP.get("total_bidp_rsqn")));
            data.setNegative(isNegative);
        } catch (Exception e) {
            log.error("Failed to parse data for {}: {}", trKey, e.getMessage());
            return null;
        }

        return data;
    }
    
    // 안전하게 Long으로 변환
    private Long toNumber(String[] fields, int index) {
        String val = safeGet(fields, index);
        if (val.isEmpty()) return 0L;
        try {
            return Long.parseLong(val.replace(",", ""));
        } catch (NumberFormatException e) {
            return 0L;
        }
    }
    
    // 안전하게 Double로 변환
    private Double toDouble(String[] fields, int index) {
        String val = safeGet(fields, index);
        if (val.isEmpty()) return 0.0;
        try {
            return Double.parseDouble(val.replace(",", ""));
        } catch (NumberFormatException e) {
            return 0.0;
        }
    }
    
    private String safeGet(String[] fields, int index) {
        return (index >= 0 && index < fields.length) ? fields[index] : "";
    }

    private String createSubscribeMessage(String stockCode) throws JsonProcessingException {
        // 실시간 체결가 구독 요청 (tr_type: '1'은 구독)
        Map<String, Object> header = Map.of(
                "approval_key", approvalKey,
                "custtype", "P", // 개인
                "tr_type", "1", // 구독
                "content-type", "utf-8"
        );
        Map<String, Object> input = Map.of(
                "tr_id", trId, // H0STCNT0
                "tr_key", stockCode
        );
        Map<String, Object> body = Map.of("input", input);
        Map<String, Object> subscribeData = Map.of("header", header, "body", body);

        return objectMapper.writeValueAsString(subscribeData);
    }
}