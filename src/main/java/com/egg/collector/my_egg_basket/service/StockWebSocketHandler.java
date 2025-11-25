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
            String subscribeMessage = createSubscribeMessage(code);
            session.sendMessage(new TextMessage(subscribeMessage));
            log.debug("Subscribed to: {}", code);
        }
    }

    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) throws Exception {
        String payload = message.getPayload();

        if (payload.startsWith("0|") || payload.startsWith("1|")) {
            RealtimeData data = parseRealtimeData(payload);
            if (data != null) {
                dataService.save(data);
            }
        }
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
        onCloseCallback.accept(null);
    }

    private RealtimeData parseRealtimeData(String message) {
        String[] parts = message.split("\\|");
        if (parts.length < 4) return null;

        String trKey = parts[2];
        String dataString = parts[3];

        String[] dataFields = dataString.split("\\^");
        if (dataFields.length == 0) return null;

        RealtimeData data = new RealtimeData();
        data.setStckShrnIscd(trKey); // 🚨 setter 이름 변경

        try {
            data.setStckCntgHour(dataFields[KisWebSocketConnector.FIELD_MAP.get("stck_cntg_hour")]); // 🚨 setter 이름 변경
            String signField = safeGet(dataFields, 2);
            boolean isNegative = "5".equals(signField) || "4".equals(signField);

            // 🚨 나머지 setter 이름도 모두 변경
            data.setStckPrpr(toNumber(dataFields, KisWebSocketConnector.FIELD_MAP.get("stck_prpr")));
            data.setPrdyVrss(toNumber(dataFields, KisWebSocketConnector.FIELD_MAP.get("prdy_vrss")));
            data.setPrdyCtrt(toDouble(dataFields, KisWebSocketConnector.FIELD_MAP.get("prdy_ctrt")));
            data.setAcmlVol(toNumber(dataFields, KisWebSocketConnector.FIELD_MAP.get("acml_vol")));
            data.setAskp1(toNumber(dataFields, KisWebSocketConnector.FIELD_MAP.get("askp1")));
            data.setBidp1(toNumber(dataFields, KisWebSocketConnector.FIELD_MAP.get("bidp1")));
            data.setWghtAvrgPrc(toNumber(dataFields, KisWebSocketConnector.FIELD_MAP.get("wght_avrg_prc")));
            data.setAcmlTrPbmn(toNumber(dataFields, KisWebSocketConnector.FIELD_MAP.get("acml_tr_pbmn")));
            data.setSelnCntgCsnu(toNumber(dataFields, KisWebSocketConnector.FIELD_MAP.get("seln_cntg_csnu")));
            data.setShnuCntgCsnu(toNumber(dataFields, KisWebSocketConnector.FIELD_MAP.get("shnu_cntg_csnu")));
            data.setTotalAskpRsqn(toNumber(dataFields, KisWebSocketConnector.FIELD_MAP.get("total_askp_rsqn")));
            data.setTotalBidpRsqn(toNumber(dataFields, KisWebSocketConnector.FIELD_MAP.get("total_bidp_rsqn")));
            data.setNegative(isNegative);
        } catch (Exception e) {
            log.error("Failed to parse data for {}. Message: {}. Error: {}", trKey, dataString, e.getMessage());
            return null;
        }

        return data;
    }

    private Long toNumber(String[] fields, int index) {
        String val = safeGet(fields, index);
        if (val.isEmpty()) return 0L;
        try {
            return Long.parseLong(val.replace(",", ""));
        } catch (NumberFormatException e) {
            return 0L;
        }
    }

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
        Map<String, Object> header = Map.of(
                "approval_key", approvalKey,
                "custtype", "P",
                "tr_type", "1",
                "content-type", "utf-8"
        );
        Map<String, Object> input = Map.of(
                "tr_id", trId,
                "tr_key", stockCode
        );
        Map<String, Object> body = Map.of("input", input);
        Map<String, Object> subscribeData = Map.of("header", header, "body", body);

        return objectMapper.writeValueAsString(subscribeData);
    }
}