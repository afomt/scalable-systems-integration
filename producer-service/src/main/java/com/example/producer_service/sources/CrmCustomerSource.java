package com.example.producer_service.sources;

import com.example.producer_service.client.CrmClient;
import com.example.producer_service.model.Customer;
import com.example.producer_service.sources.ProducerSource;

import java.util.Arrays;
import java.util.List;

import org.springframework.stereotype.Service;

@Service
public class CrmCustomerSource implements ProducerSource<Customer> {

    private final CrmClient crmClient;

    public CrmCustomerSource(CrmClient crmClient) {
        this.crmClient = crmClient;
    }

    @Override public String sourceName() { return "crm"; }

    @Override public String topic() { return "customer_data"; }

    @Override public Iterable<Customer> fetch() {
        Customer[] customers = crmClient.fetchCustomers();
        return customers == null ? List.of() : Arrays.asList(customers);
    }

    @Override public String keyOf(Customer c) {
        return c.getId();
    }
}
