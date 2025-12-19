package com.egg.collector.my_egg_basket.config;

import com.egg.collector.my_egg_basket.domain.RealtimeData;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    public ConsumerFactory<String, RealtimeData> consumerFactory() {
        Map<String, Object> config = new HashMap<>();

        // Kafka 서버 주소
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        // Consumer Group ID
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "mongo-consumer-group-v1");

        // Key Deserializer (String)
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        // Value Deserializer (JSON → RealtimeData)
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);

        // 자동 오프셋 커밋 비활성화 (수동 커밋으로 변경)
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        // 오프셋이 없을 때 처음부터 읽기 (기존 메시지 모두 처리)
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // JSON Deserializer 설정
        JsonDeserializer<RealtimeData> deserializer = new JsonDeserializer<>(RealtimeData.class);
        deserializer.addTrustedPackages("*");
        deserializer.setRemoveTypeHeaders(false);
        deserializer.setUseTypeMapperForKey(false);

        return new DefaultKafkaConsumerFactory<>(
                config,
                new StringDeserializer(),
                deserializer
        );
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, RealtimeData> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, RealtimeData> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());

        // 동시 처리 스레드 개수
        // 주의: 종목별(key별) 순서 보장을 위해 1로 설정
        // (같은 key의 메시지는 같은 파티션으로 가므로 순서 보장됨)
        factory.setConcurrency(1);

        // [수정] 컨테이너 속성에 수동 커밋 모드 설정
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);

        // [제거] 데이터 중복을 유발할 수 있는 RebalanceListener 제거

        return factory;
    }
}
