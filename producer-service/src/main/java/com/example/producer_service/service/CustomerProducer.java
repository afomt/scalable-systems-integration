package com.example.producer_service.service;

import com.example.producer_service.client.CrmClient;
import com.example.producer_service.model.Customer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;

@Service
public class CustomerProducer {

    private static final Logger log = LoggerFactory.getLogger(CustomerProducer.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final CrmClient crmClient;

    public CustomerProducer(KafkaTemplate<String, Object> kafkaTemplate, CrmClient crmClient) {
        this.kafkaTemplate = kafkaTemplate;
        this.crmClient = crmClient;
    }

    @PostConstruct
    public void init() {
        log.info("CustomerProducer bean loaded successfully");
    }

    @Scheduled(fixedRate = 300000, initialDelay = 5000)
    public void publishCustomers() {
        log.info("⏳ Scheduled job started: fetching customers from CRM REST mock...");

        Customer[] customers = crmClient.fetchCustomers();

        if (customers == null || customers.length == 0) {
            log.warn("No customers returned from CRM");
            return;
        }

        for (Customer c : customers) {
            if (c == null || c.getId() == null || c.getId().isBlank()) {
                log.warn("⚠️ Skipping invalid customer record");
                continue;
            }

            log.info("📤 Publishing customer id={}", c.getId());
            kafkaTemplate.send("customer_data", c.getId(), c);
        }

        log.info("✅ Published {} customers", customers.length);
    }
}
