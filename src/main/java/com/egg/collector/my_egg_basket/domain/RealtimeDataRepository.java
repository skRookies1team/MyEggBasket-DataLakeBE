package com.egg.collector.my_egg_basket.domain;

import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface RealtimeDataRepository extends MongoRepository<RealtimeData, String> {

    // 오래된 순서대로 데이터를 조회하기 위한 메소드 추가
    Slice<RealtimeData> findAllByOrderByTimestampAsc(Pageable pageable);
}