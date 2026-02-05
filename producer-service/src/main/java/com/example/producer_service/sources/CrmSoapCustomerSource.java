package com.example.producer_service.sources;

import com.example.producer_service.client.CrmSoapClient;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class CrmSoapCustomerSource implements ProducerSource <Object>{

    private final CrmSoapClient soapClient;

    public CrmSoapCustomerSource(CrmSoapClient soapClient) {
        this.soapClient = soapClient;
    }

    @Override
    public String sourceName() {
        return "crm-soap";
    }

    @Override
    public String topic() {
        return "customer_data";
    }

    @Override
    public List<Object> fetch() {
        return soapClient.getCustomers().stream()
                .map(s -> (Object) s)
                .toList();
    }

    @Override
    public String keyOf(Object item) {
        // item format: C003|Charlie Brown|charlie@example.com
        String record = (String) item;
        return record.split("\\|")[0];
    }
}
