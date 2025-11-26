package com.egg.collector.my_egg_basket.domain;

import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface RealtimeDataRepository extends MongoRepository<RealtimeData, String> {

    // [수정됨] 파라미터 타입 LocalDateTime -> String
    // MongoDB는 문자열 비교 시 사전순(Lexicographical) 비교를 하므로,
    // "yyyy-MM-dd HH:mm:ss" 포맷은 시간 순서대로 정확히 검색됩니다.
    Slice<RealtimeData> findAllByTimestampBefore(String timestamp, Pageable pageable);
}