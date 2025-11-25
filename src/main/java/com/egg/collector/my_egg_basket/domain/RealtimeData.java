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
        @CompoundIndex(name = "stock_time_idx", def = "{'stck_shrn_iscd': 1, 'timestamp': 1}", unique = true)
})
public class RealtimeData {

    @Id
    private String id;

    // 데이터 수집 시간 (DB 저장 시간)
    private LocalDateTime timestamp;

    // H0STCNT0 주요 필드 (useRealtimeStock.ts 참고)
    private String stck_shrn_iscd; // 종목 코드
    private String stck_cntg_hour; // 체결 시각 (HHmmss)
    private Long stck_prpr;        // 현재가
    private Long prdy_vrss;        // 전일 대비 (절대값)
    private Double prdy_ctrt;      // 전일 대비율 (%)
    private Long acml_tr_pbmn;     // 누적 거래 대금
    private Long acml_vol;         // 누적 거래량
    private Long seln_cntg_csnu;   // 매도 체결 건수
    private Long shnu_cntg_csnu;   // 매수 체결 건수
    private Long wght_avrg_prc;    // 가중 평균 가격
    private Long askp1;            // 매도 호가 1
    private Long bidp1;            // 매수 호가 1
    private Long total_askp_rsqn;  // 총 매도 잔량
    private Long total_bidp_rsqn;  // 총 매수 잔량

    // 편의를 위한 필드
    private boolean isNegative; // 전일 대비 마이너스 여부
}