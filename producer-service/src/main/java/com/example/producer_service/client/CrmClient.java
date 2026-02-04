package com.example.producer_service.client;

import com.example.producer_service.model.Customer;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

@Service
public class CrmClient {

    private final RestTemplate restTemplate = new RestTemplate();

    @Retryable(
            retryFor = { Exception.class },
            maxAttempts = 3,
            backoff = @Backoff(delay = 2000, multiplier = 2)
    )
    public Customer[] fetchCustomers() {
        // IMPORTANT: inside docker network use service-name and container-port
        String url = "http://crm-rest-mock:4010/customers";

        return restTemplate.getForObject(url, Customer[].class);
    }
}
