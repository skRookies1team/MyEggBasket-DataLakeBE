package com.egg.collector.my_egg_basket.domain;

import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.index.CompoundIndexes;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.LocalDateTime;

// MongoDB에 저장할 실시간 체결 데이터 모델
@Data
@Document(collection = "realtime_price")
@CompoundIndexes({
        // 종목코드와 시간으로 인덱스 설정 (빠른 검색 및 유니크 키 역할)
        @CompoundIndex(name = "stock_time_idx", def = "{'stckShrnIscd': 1, 'timestamp': 1}", unique = true) // 💡 필드명 변경
})
public class RealtimeData {

    @Id
    private String id;

    // 데이터 수집 시간 (DB 저장 시간)
    private LocalDateTime timestamp;

    private String stckShrnIscd; // 종목 코드
    private String stckCntgHour; // 체결 시각 (HHmmss)
    private Long stckPrpr;        // 현재가
    private Long prdyVrss;        // 전일 대비 (절대값)
    private Double prdyCtrt;      // 전일 대비율 (%)
    private Long acmlTrPbmn;     // 누적 거래 대금
    private Long acmlVol;         // 누적 거래량
    private Long selnCntgCsnu;   // 매도 체결 건수
    private Long shnuCntgCsnu;   // 매수 체결 건수
    private Long wghtAvrgPrc;    // 가중 평균 가격
    private Long askp1;            // 매도 호가 1
    private Long bidp1;            // 매수 호가 1
    private Long totalAskpRsqn;  // 총 매도 잔량
    private Long totalBidpRsqn;  // 총 매수 잔량

    // 편의를 위한 필드
    private boolean isNegative; // 전일 대비 마이너스 여부
}