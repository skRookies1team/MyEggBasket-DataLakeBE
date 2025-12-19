package com.egg.collector.my_egg_basket.service;

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
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.client.WebSocketClient;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;

import java.time.Duration;
import java.time.Instant;
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
    private final KafkaConsumerService kafkaConsumerService;
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

    private final AtomicReference<WebSocketSession> currentSession = new AtomicReference<>(null);
    private final AtomicReference<String> approvalKey = new AtomicReference<>(null);
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    @PostConstruct
    public void startClient() {
        // 1. 초기 실행 및 매일 Approval Key 갱신
        scheduler.scheduleAtFixedRate(this::refreshKeyAndConnect, 0, 24, TimeUnit.HOURS);

        // 2. 데이터 아카이빙 체크 (1시간 주기)
        scheduler.scheduleAtFixedRate(dataService::archivePastDataIfNeeded, 1, 60, TimeUnit.MINUTES);

        // 3. 헬스체크 (1분 주기)
        scheduler.scheduleAtFixedRate(this::healthCheck, 1, 1, TimeUnit.MINUTES);
    }

    private void refreshKeyAndConnect() {
        try {
            String key = getApprovalKey();
            if (key != null) {
                approvalKey.set(key);
                connectAndSubscribe();
            }
        } catch (Exception e) {
            log.error("Failed to refresh key: {}", e.getMessage());
        }
    }

    private void healthCheck() {
        WebSocketSession session = currentSession.get();
        if (session != null && session.isOpen()) {
            // KafkaConsumerService에서 마지막 저장 시각 확인
            Instant last = kafkaConsumerService.getLastSavedAt();
            // 마지막 저장 후 2분이 지났으면 '좀비 연결'로 판단하고 재접속
            if (Duration.between(last, Instant.now()).toMinutes() >= 2) {
                log.warn("No data received for 2 mins (Last saved: {}). Force reconnecting...", last);
                closeSession(session);
            }
        } else {
            log.info("Session is closed or null. Connecting...");
            connectAndSubscribe();
        }
    }

    private synchronized void connectAndSubscribe() {
        WebSocketSession existing = currentSession.get();
        if (existing != null && existing.isOpen()) {
            return;
        }

        if (approvalKey.get() == null) {
            log.warn("Approval Key is missing. Skipping connection.");
            return;
        }

        WebSocketClient client = new StandardWebSocketClient();
        try {
            log.info("Connecting to WebSocket URL: {}", wsUrl);
            client.execute(
                    new StockWebSocketHandler(approvalKey.get(), stockCodes, trId, dataService, objectMapper, this::handleClose),
                    wsUrl
            ).thenAccept(session -> {
                currentSession.set(session);
                log.info("Session connected: {}", session.getId());
            });

        } catch (Exception e) {
            log.error("WebSocket connection failed: {}", e.getMessage());
            scheduler.schedule(this::connectAndSubscribe, 10, TimeUnit.SECONDS);
        }
    }

    private void handleClose(Void ignored) {
        log.warn("WebSocket connection closed/reset detected.");
        currentSession.set(null);
        // 너무 빠른 재접속 방지 (5초 대기)
        scheduler.schedule(this::connectAndSubscribe, 5, TimeUnit.SECONDS);
    }

    private void closeSession(WebSocketSession session) {
        try {
            if (session != null && session.isOpen()) {
                session.close();
            }
        } catch (Exception e) {
            log.error("Error closing session: {}", e.getMessage());
        }
    }

    private String getApprovalKey() {
        log.info("Requesting Approval Key...");
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
            JsonNode response = webClient.post().uri("/oauth2/Approval").bodyValue(requestBody).retrieve().bodyToMono(JsonNode.class).block();
            return response != null ? response.get("approval_key").asText() : null;
        } catch (Exception e) {
            log.error("Key issuance failed: {}", e.getMessage());
            return null;
        }
    }
}