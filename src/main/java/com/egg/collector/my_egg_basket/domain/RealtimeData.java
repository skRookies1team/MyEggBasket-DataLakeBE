package com.egg.collector.my_egg_basket.domain;

import lombok.Data;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.index.CompoundIndexes;
import org.springframework.data.mongodb.core.mapping.Document;

@Data
@Document(collection = "realtime_price")
@CompoundIndexes({
        // 문자열이어도 ISO 포맷(yyyy-MM-dd HH:mm:ss)이면 정렬/범위 검색이 정상 작동합니다.
        @CompoundIndex(name = "stock_time_idx", def = "{'stckShrnIscd': 1, 'timestamp': -1}")
})
public class RealtimeData {

    @Id
    private String id;

    private String timestamp;

    private String stckShrnIscd;
    private String stckCntgHour; // 주식 체결 시간

    private Long stckPrpr; // 주식 현재가
    private Long prdyVrss; // 전일 대비
    private Double prdyCtrt; // 전일 대비율

    private Long acmlVol; // 누적 거래량
    private Long acmlTrPbmn; // 누적 거래 대금

    private Long askp1; // 매도호가1
    private Long bidp1; // 매수호가1

    private Long wghtAvrgPrc; //
    private Long selnCntgCsnu; // 매도 체결 건수
    private Long shnuCntgCsnu; // 매수 체결 건수

    private Long totalAskpRsqn; // 총 매도호가 잔량
    private Long totalBidpRsqn; // 총 매수호가 잔량

    private boolean isNegative;
}