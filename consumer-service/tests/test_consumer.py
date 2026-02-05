import asyncio
import json
import pytest

from consumer import process_event


@pytest.mark.asyncio
async def test_customer_inventory_join():

    customers = {}
    products = {}
    sent = set()

    customer = {"id": "C1", "name": "Alice"}
    product = {"id": "P1", "name": "Mouse", "stock": 10}

    result = await process_event(customer, "customer_data", customers, products, sent)
    assert result is None

    result = await process_event(product, "inventory_data", customers, products, sent)
    assert result["customerId"] == "C1"
    assert result["products"][0]["id"] == "P1"
