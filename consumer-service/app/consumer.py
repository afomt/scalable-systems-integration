import os
import json
import asyncio
from datetime import datetime
import aiohttp
from aiokafka import AIOKafkaConsumer
import redis.asyncio as redis

# ------------------------------------------------
# Config
# ------------------------------------------------

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
ANALYTICS_URL = os.getenv("ANALYTICS_URL", "http://analytics-mock:9000/analytics/data")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379")

CUSTOMER_TOPIC = "customer_data"
INVENTORY_TOPIC = "inventory_data"

WORKERS = int(os.getenv("WORKERS", "20"))
QUEUE_SIZE = int(os.getenv("QUEUE_SIZE", "5000"))

customers = {}
products = {}

event_queue = asyncio.Queue(maxsize=QUEUE_SIZE)

redis_client = None


# ------------------------------------------------
# Redis connection with retry
# ------------------------------------------------
async def get_redis():
    global redis_client

    if redis_client:
        return redis_client

    while True:
        try:
            print("Connecting to Redis...", flush=True)
            redis_client = redis.from_url(REDIS_URL, decode_responses=True)
            await redis_client.ping()
            print("Connected to Redis", flush=True)
            return redis_client
        except Exception:
            print("Redis not ready, retrying in 3s...", flush=True)
            await asyncio.sleep(3)


async def is_duplicate(customer_id, product_id):
    r = await get_redis()
    return await r.exists(f"processed:{customer_id}:{product_id}")


async def mark_processed(customer_id, product_id):
    r = await get_redis()
    await r.set(f"processed:{customer_id}:{product_id}", "1")


# ------------------------------------------------
# Message decoder (SOAP + REST)
# ------------------------------------------------
def decode(msg):
    raw = msg.value.decode()

    try:
        data = json.loads(raw)

        # SOAP wrapped as JSON string
        if isinstance(data, str) and "|" in data:
            parts = data.split("|")
            return {
                "type": "customer",
                "id": parts[0].replace('"', ''),
                "name": parts[1]
            }

        return data

    except:
        if "|" in raw:
            parts = raw.split("|")
            return {
                "type": "customer",
                "id": parts[0].replace('"', ''),
                "name": parts[1]
            }
        raise


# ------------------------------------------------
# Analytics sender (idempotent)
# ------------------------------------------------
async def send_to_analytics(session, customer_id, product_id):

    if await is_duplicate(customer_id, product_id):
        print(f"Duplicate ignored -> {customer_id}:{product_id}", flush=True)
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
                print(f"Sent analytics -> {customer_id}:{product_id}", flush=True)
                await mark_processed(customer_id, product_id)
            else:
                print("Analytics rejected:", resp.status, flush=True)

    except Exception as e:
        print("Analytics error:", e, flush=True)


# ------------------------------------------------
# Worker pool (parallel processing proof)
# ------------------------------------------------
async def analytics_worker(session, worker_id):

    while True:
        cid, pid = await event_queue.get()

        print(f"[Worker {worker_id}] Processing {cid}:{pid}", flush=True)

        try:
            await send_to_analytics(session, cid, pid)
        finally:
            event_queue.task_done()


# ------------------------------------------------
# Main consumer loop
# ------------------------------------------------
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

                # start worker pool
                workers = [
                    asyncio.create_task(analytics_worker(session, i))
                    for i in range(WORKERS)
                ]

                async for msg in consumer:

                    try:
                        event = decode(msg)

                        # ---------- CUSTOMER ----------
                        if msg.topic == CUSTOMER_TOPIC:

                            cid = event["id"]
                            customers[cid] = event

                            print(f"Customer received: {cid} ({event.get('name')})", flush=True)

                            for pid in products:
                                await event_queue.put((cid, pid))

                        # ---------- INVENTORY ----------
                        elif msg.topic == INVENTORY_TOPIC:

                            pid = event["id"]
                            products[pid] = event

                            print(f"Inventory received: {pid} stock={event.get('stock')}", flush=True)

                            for cid in customers:
                                await event_queue.put((cid, pid))

                    except Exception as e:
                        print("Message processing error:", e, flush=True)

        except Exception as e:
            print("Kafka connection error:", e, "retrying in 5s", flush=True)
            await asyncio.sleep(5)

        finally:
            print("Closing consumer...", flush=True)
            await consumer.stop()
            print("Consumer closed", flush=True)


# ------------------------------------------------

if __name__ == "__main__":
    asyncio.run(process_messages())
