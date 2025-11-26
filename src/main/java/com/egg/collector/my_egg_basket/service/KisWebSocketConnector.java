package com.egg.collector.my_egg_basket.service;

import com.egg.collector.my_egg_basket.domain.RealtimeDataRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import org.springframework.web.socket.client.WebSocketClient;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;

import java.net.URI;
import java.net.URISyntaxException; // 🚨 URISyntaxException 임포트 추가
import java.util.HashMap;
import java.util.Map;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@Service
@RequiredArgsConstructor
@Slf4j
public class KisWebSocketConnector {

    private final RealtimeDataService dataService;
    private final ObjectMapper objectMapper;

    @Value("${kis.api.url}")
    private String apiUrl;
    @Value("${kis.ws.url}")
    private String wsUrl;
    @Value("${kis.app.key}")
    private String appKey;
    @Value("${kis.app.secret}")
    private String appSecret;
    @Value("${kis.tr.id}")
    private String trId;
    @Value("${kis.subscription.codes}")
    private String[] stockCodes;

    private final AtomicReference<String> approvalKey = new AtomicReference<>(null);
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(); // 🚨 오타 수정: newSingleThreadScheduledService() -> newSingleThreadScheduledExecutor()
    // H0STCNT0 필드 인덱스 매핑 (useRealtimeStock.ts 참고)
    public static final Map<String, Integer> FIELD_MAP = new HashMap<>();
    static {
        FIELD_MAP.put("stck_shrn_iscd", 0);
        FIELD_MAP.put("stck_cntg_hour", 1);
        FIELD_MAP.put("stck_prpr", 2);
        FIELD_MAP.put("prdy_vrss", 4);
        FIELD_MAP.put("prdy_ctrt", 5);
        FIELD_MAP.put("acml_vol", 6);
        FIELD_MAP.put("askp1", 7);
        FIELD_MAP.put("bidp1", 8);
        FIELD_MAP.put("wght_avrg_prc", 10);
        FIELD_MAP.put("acml_tr_pbmn", 14);
        FIELD_MAP.put("seln_cntg_csnu", 15);
        FIELD_MAP.put("shnu_cntg_csnu", 16);
        FIELD_MAP.put("total_askp_rsqn", 38);
        FIELD_MAP.put("total_bidp_rsqn", 39);
    }
    public static final int IDX_TOTAL_BIDP_RSQN = 39;

    @PostConstruct
    public void startClient() {
        scheduler.scheduleAtFixedRate(this::getAndConnect, 0, 24, TimeUnit.HOURS);
        dataService.cleanupOldData();
        scheduler.scheduleAtFixedRate(dataService::cleanupOldData, 1, 1, TimeUnit.HOURS);
    }

    private void getAndConnect() {
        try {
            String key = getApprovalKey();
            if (key != null && !key.isEmpty()) {
                approvalKey.set(key);
                connectAndSubscribe();
            } else {
                log.error("Failed to get Approval Key. Retrying in 1 hour.");
                scheduler.schedule(this::getAndConnect, 1, TimeUnit.HOURS);
            }
        } catch (Exception e) {
            log.error("Error during initial connection setup: {}", e.getMessage(), e);
        }
    }

    /**
     * 1. 한국투자증권 REST API를 호출하여 Approval Key를 발급받습니다.
     * @return 발급된 Approval Key
     */
    private String getApprovalKey() {
        log.info("Requesting Approval Key from KIS API at {}/oauth2/Approval...", apiUrl);
        try {
            WebClient webClient = WebClient.builder()
                    .baseUrl(apiUrl)
                    .defaultHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                    .build();

            Map<String, String> requestBody = Map.of(
                    "grant_type", "client_credentials",
                    "appkey", appKey,
                    "secretkey", appSecret
            );

            JsonNode response = webClient.post()
                    .uri("/oauth2/Approval")
                    .bodyValue(requestBody)
                    .retrieve()
                    .bodyToMono(JsonNode.class)
                    .block();

            String key = response != null && response.has("approval_key")
                    ? response.get("approval_key").asText()
                    : null;

            if (key != null && !key.isEmpty()) {
                log.info("Approval Key received successfully: {}", key.substring(0, 10) + "...");
                return key;
            } else {
                log.error("Failed to parse approval_key from response or key is empty. Response: {}", response != null ? response.toString() : "null");
                return null;
            }
        } catch (WebClientResponseException e) {
            log.error("Failed to get Approval Key (HTTP Status: {}): Check your AppKey/AppSecret. Response Body: {}", e.getStatusCode(), e.getResponseBodyAsString());
            return null;
        } catch (Exception e) {
            log.error("Failed to get Approval Key: {}", e.getMessage(), e);
            return null;
        }
    }

    /**
     * 2. 웹소켓에 연결하고 모든 종목을 구독합니다.
     */
    private void connectAndSubscribe() {
        WebSocketClient client = new StandardWebSocketClient();
        URI uri = null;
        try {uri = new URI(wsUrl); // 유효성 검증만 수행
        } catch (URISyntaxException e) {
            log.error("Invalid WebSocket URL syntax: {}. Error: {}", wsUrl, e.getMessage());
            handleClose(null);
            return;
        }

        try {
            client.execute(
                    new StockWebSocketHandler(approvalKey.get(), stockCodes, trId, dataService, objectMapper, this::handleClose),
                    wsUrl // execute는 String 시그니처 사용
            ).get();

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("WebSocket connection interrupted.");
        } catch (Exception e) {
            log.error("WebSocket connection failed: {}", e.getMessage());
            handleClose(null);
        }
    }

    /**
     * 웹소켓 연결 종료 시 재접속을 시도합니다. (Consumer<Void> 시그니처 맞춤)
     * @param ignored StockWebSocketHandler에서 전달되는 Void 인자 (사용하지 않음)
     */
    private void handleClose(Void ignored) {
        log.warn("WebSocket connection closed. Retrying connection in 5 seconds...");
        scheduler.schedule(this::connectAndSubscribe, 5, TimeUnit.SECONDS);
    }
}