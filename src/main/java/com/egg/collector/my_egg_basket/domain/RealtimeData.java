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

    // [수정됨] LocalDateTime -> String
    // DB에 "2025-11-26 09:45:09" 문자열 그대로 저장됨
    private String timestamp;

    private String stckShrnIscd;
    private String stckCntgHour;

    private Long stckPrpr;
    private Long prdyVrss;
    private Double prdyCtrt;

    private Long acmlVol;
    private Long acmlTrPbmn;

    private Long askp1;
    private Long bidp1;

    private Long wghtAvrgPrc;
    private Long selnCntgCsnu;
    private Long shnuCntgCsnu;

    private Long totalAskpRsqn;
    private Long totalBidpRsqn;

    private boolean isNegative;
}