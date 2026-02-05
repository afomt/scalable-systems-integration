package com.example.producer_service.sources;

import com.example.producer_service.client.InventoryClient;
import com.example.producer_service.model.Product;
import com.example.producer_service.sources.ProducerSource;

import java.util.Arrays;
import java.util.List;

import org.springframework.stereotype.Service;
@Service
public class InventoryProductSource implements ProducerSource<Product> {

    private final InventoryClient inventoryClient;

    public InventoryProductSource(InventoryClient inventoryClient) {
        this.inventoryClient = inventoryClient;
    }

    @Override public String sourceName() { return "inventory"; }

    @Override public String topic() { return "inventory_data"; }

    @Override public Iterable<Product> fetch() {
        Product[] products = inventoryClient.fetchProducts();
        return products == null ? List.of() : Arrays.asList(products);
    }

    @Override public String keyOf(Product p) {
        return p.getId();
    }
}
