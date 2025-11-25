package com.egg.collector.my_egg_basket.domain;

import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;

@Repository
public interface RealtimeDataRepository extends MongoRepository<RealtimeData, String> {

    // 특정 종목 코드의 가장 최신 데이터를 조회
    RealtimeData findTopByStckShrnIscdOrderByTimestampDesc(String stckShrnIscd);

    // 3일치 데이터만 저장한다는 요구사항에 맞게, 오래된 데이터를 삭제할 수 있는 메서드
    List<RealtimeData> deleteByTimestampBefore(LocalDateTime cutoffDate);
    Slice<RealtimeData> findAllByTimestampBefore(LocalDateTime date, Pageable pageable);
}