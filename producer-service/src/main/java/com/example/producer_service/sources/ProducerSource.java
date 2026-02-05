package com.example.producer_service.sources;

public interface ProducerSource<T> {
    String sourceName();        // crm, inventory, orders...
    String topic();             // customer_data, inventory_data
    Iterable<T> fetch();        // fetch from REST/SOAP
    String keyOf(T item);       // kafka key => partitioning
}
