import os
import json
import asyncio
from datetime import datetime
import aiohttp
from aiokafka import AIOKafkaConsumer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
ANALYTICS_URL = os.getenv("ANALYTICS_URL", "http://analytics-mock:9000/analytics/data")

CUSTOMER_TOPIC = "customer_data"
INVENTORY_TOPIC = "inventory_data"

customers = {}
products = {}
sent_pairs = set()


def decode(msg):
    return json.loads(msg.value.decode())


async def send_to_analytics(session, customer_id, product_id):
    key = (customer_id, product_id)
    if key in sent_pairs:
        return

    customer = customers.get(customer_id)
    product = products.get(product_id)

    if not customer or not product:
        return

    payload = {
        "customerId": customer["id"],
        "customerName": customer.get("name"),
        "products": [product],
        "timestamp": datetime.utcnow().isoformat()
    }

    try:
        async with session.post(ANALYTICS_URL, json=payload) as resp:
            if resp.status == 200:
                print("Sent analytics:", payload, flush=True)
                sent_pairs.add(key)
    except Exception as e:
        print("Analytics error:", e, flush=True)


async def process_messages():
    while True:
        consumer = AIOKafkaConsumer(
            CUSTOMER_TOPIC,
            INVENTORY_TOPIC,
            bootstrap_servers=BOOTSTRAP,
            group_id="analytics-consumer",
            auto_offset_reset="earliest",
            enable_auto_commit=True
        )

        try:
            print("Connecting to Kafka...", flush=True)
            await consumer.start()
            print("Connected to Kafka", flush=True)

            async with aiohttp.ClientSession() as session:
                async for msg in consumer:
                    try:
                        event = decode(msg)

                        if msg.topic == CUSTOMER_TOPIC:
                            customers[event["id"]] = event
                            for pid in products:
                                await send_to_analytics(session, event["id"], pid)

                        elif msg.topic == INVENTORY_TOPIC:
                            products[event["id"]] = event
                            for cid in customers:
                                await send_to_analytics(session, cid, event["id"])

                    except Exception as e:
                        print("Message processing error:", e, flush=True)

        except Exception as e:
            print("Kafka connection error:", e, "retrying in 5s", flush=True)
            await asyncio.sleep(5)

        finally:
            print("Closing consumer...", flush=True)
            await consumer.stop()
            print("Consumer closed", flush=True)


if __name__ == "__main__":
    asyncio.run(process_messages())
