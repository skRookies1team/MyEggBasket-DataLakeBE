package com.egg.collector.my_egg_basket.domain;

import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface RealtimeDataRepository extends MongoRepository<RealtimeData, String> {

    Slice<RealtimeData> findAllByTimestampBetweenOrderByTimestampAsc(String start, String end, Pageable pageable);
}