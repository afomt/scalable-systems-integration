def merge_data(customer):
    return {
        "customer_id": customer["id"],
        "email": customer["email"],
        "event_type": "CUSTOMER_SYNC"
    }
