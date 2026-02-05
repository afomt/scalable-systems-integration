package com.example.producer_service.kafka;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
class KafkaPublishTest {

    @Test
    void kafkaContextLoads() {
        System.out.println("Kafka context started ✔");
    }
}
