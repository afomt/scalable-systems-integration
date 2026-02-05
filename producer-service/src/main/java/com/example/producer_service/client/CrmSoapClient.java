package com.example.producer_service.client;

import com.example.producer_service.soap.Application;
import jakarta.xml.ws.BindingProvider;
import jakarta.xml.ws.Service;
import org.springframework.stereotype.Component;

import javax.xml.namespace.QName;
import java.net.URL;
import java.util.List;

@Component
public class CrmSoapClient {

    private Application port;

    private Application getPort() throws Exception {
        if (port == null) {
            URL wsdl = new URL("http://crm-soap-mock:8000/?wsdl");

            QName SERVICE_NAME = new QName("crm.soap.service", "CustomerService");
            QName PORT_NAME    = new QName("crm.soap.service", "Application");

            Service service = Service.create(wsdl, SERVICE_NAME);
            port = service.getPort(PORT_NAME, Application.class);

            ((BindingProvider) port).getRequestContext().put(
                    BindingProvider.ENDPOINT_ADDRESS_PROPERTY,
                    "http://crm-soap-mock:8000/"
            );
        }
        return port;
    }

    public List<String> getCustomers() {
        try {
            return getPort().getCustomers().getString();
        } catch (Exception e) {
            throw new RuntimeException("SOAP CRM failed", e);
        }
    }
}
