package com.example.producer_service.publisher;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.producer_service.sources.ProducerSource;

@Service
public class SourcePublisher {

    private static final Logger log = LoggerFactory.getLogger(SourcePublisher.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public SourcePublisher(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public <T> void publish(ProducerSource<T> source) {
        log.info("🔄 Running source={}", source.sourceName());

        for (T item : source.fetch()) {
            if (item == null) continue;

            String key = source.keyOf(item);
            if (key == null || key.isBlank()) continue;

            kafkaTemplate.send(source.topic(), key, item);
        }

        log.info("✅ Finished source={} topic={}", source.sourceName(), source.topic());
    }
}
